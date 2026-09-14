#!/usr/bin/env python3
"""
DSA Reader - Deterministic Mechanical Archiver & Collation Engine

Ingests articles using direct structured XML/RSS/Atom feeds, 
applies layout-specific semantic extraction rules, and outputs a 
collated responsive dashboard with live search, publication filtering, 
and sorting options.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import html
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional

from bs4 import BeautifulSoup, Tag

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
# Public site root. Override with CAUCUS_COMMONS_SITE_ROOT if this folder moves.
OUTPUT_DIR = Path(os.environ.get(
    "CAUCUS_COMMONS_SITE_ROOT",
    "/Users/marcbandaronek/Documents/fun/my website/onquarryrd mov",
)).expanduser().resolve()
SOURCE_DIR = OUTPUT_DIR / "caucuscommons-source"
ARTICLE_TEXT_DIR = OUTPUT_DIR / "caucuscommons-article-text"
DB_FILE = DATA_DIR / "dsa_intel.db"
DASHBOARD_FILE = OUTPUT_DIR / "caucuscommons.html"
DISCOVERED_HTML = OUTPUT_DIR / "caucuscommons-discovered-links.html"
DISCOVERED_JSON = OUTPUT_DIR / "caucuscommons-discovered-links.json"
PROCESSED_LINKS_JSON = OUTPUT_DIR / "caucuscommons-processed-links.json"
EXTRACTION_DIAGNOSTICS_JSON = OUTPUT_DIR / "caucuscommons-extraction-diagnostics.json"
RSS_FILE = OUTPUT_DIR / "caucuscommons.xml"
RSS_DATA_FILE = OUTPUT_DIR / "caucuscommons-feed-data.json"
PAGES_WORKER_FILE = OUTPUT_DIR / "_worker.js"
# Keep logs and deployment credentials beside the script, not in the public tree.
LOG_FILE = SCRIPT_DIR / "dsa_scraper.log"
CLOUDFLARE_CREDENTIALS_FILE = SCRIPT_DIR / "cloudflare_api_token.txt"
CLOUDFLARE_PROJECT = "onquarryrd"
CLOUDFLARE_PRODUCTION_BRANCH = "main"
RUN_STARTED_AT = datetime.now().astimezone()


@dataclass
class SourceSpec:
    key: str
    name: str
    url: str
    source_type: str  # "rss" or "linktree"
    same_domain_only: bool = True
    include_podcast: bool = False


SOURCES: list[SourceSpec] = [
    SourceSpec("north_star", "North Star Caucus Blog", "https://www.dsanorthstar.org/1/feed", "rss"),
    SourceSpec("reform_revolution", "Reform & Revolution", "https://reformandrevolution.org/feed/", "rss"),
    SourceSpec("socialist_call", "Socialist Call (Bread & Roses)", "https://socialistcall.com/feed/", "rss"),
    SourceSpec("caracol", "Caracol", "https://caracoldsa.org/rss/", "rss"),
    SourceSpec("communist_caucus", "Communist Caucus Bulletin", "https://communistcaucus.com/bulletin/", "html"),
    SourceSpec("spadework", "Spadework", "https://spade.work/rss/", "rss"),
    SourceSpec("power_map", "Power Map Mag (Groundwork)", "https://powermapmag.substack.com/feed", "rss"),
    SourceSpec("building_up", "Building Up (Groundwork)", "https://www.groundworkdsa.com/building-up?format=rss", "rss"),
    SourceSpec("zenith", "Zenith (Red Star)", "https://redstarcaucus.org/tag/zenith/rss/", "rss"),
    SourceSpec("red_star_news", "Red Star Newsletter", "https://redstarcaucus.org/tag/newsletter/rss/", "rss"),
    SourceSpec("twenty_first_century_socialism", "21st Century Socialism", "https://www.21csocialism.org/rss/", "rss"),
    SourceSpec("liberation", "Liberation", "https://www.liberationcaucus.org/feed/", "rss"),
    SourceSpec("lsc_pamphlets", "LSC Pamphlets", "https://dsa-lsc.org/category/pamphlets/feed/", "rss"),
    SourceSpec("lsc_statements", "LSC Statements", "https://dsa-lsc.org/category/statements/feed/", "rss"),
    SourceSpec("emerge", "Emerge", "https://dsaemerge.org/feed/", "rss"),
    SourceSpec("partisan", "Partisan Magazine", "https://partisanmag.com/feed/", "rss"),
    SourceSpec("mug_statements", "Marxist Unity Group", "https://www.marxistunity.com/tag/statements/rss/", "rss"),
    SourceSpec("mug_latest", "Light & Air (MUG)", "https://www.marxistunity.com/latest/rss/", "rss"),
    SourceSpec("smc", "The Agitator (SMC)", "https://www.socialistmajority.com/theagitator?format=rss", "rss"),
]


PODCAST_URL_RE = re.compile(r"(podcast|buzzsprout|\.mp3(?:$|[?&]))", re.I)
NON_ARTICLE_URL_RE = re.compile(
    r"(linktr\.ee|docs\.google\.com|drive\.google\.com|forms\.gle|google\.com/forms|"
    r"twitter\.com|x\.com/intent|instagram\.com|facebook\.com/sharer|linkedin\.com/sharing|"
    r"linksynergy\.com|pxf\.io|sjv\.io|kqzyfj\.com|mailto:|javascript:)",
    re.I,
)
STATIC_PATH_RE = re.compile(
    r"/(about|about-us|join|join-mug|rules-code-of-conduct|curriculum|reading-list|how-do-i-join|"
    r"strategic-approaches|our-statement|latest|light-and-air|tag|category|author|feed|comments/feed|search)(?:/|$)",
    re.I,
)
MUG_STATIC_PATH_RE = re.compile(
    r"/(about|about-us|join-mug|rules-code-of-conduct|curriculum|latest|light-and-air|tag|"
    r"sobre-nosotros|ektaakaa-saat-bundaahruu|hindii-anuvaad-yhaan|2026-tasks-perspectives)(?:/|$)",
    re.I,
)
NAV_TITLE_RE = re.compile(
    r"^(home|about|contact|subscribe|sign in|join dsa|join mug|points of unity|rules & code of conduct|"
    r"latest articles|light & air magazine|skip to content|menu|close menu|powered by wordpress|to the top)",
    re.I,
)


# -----------------------------------------------------------------------------
# Utility & Formatting
# -----------------------------------------------------------------------------
def ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    SOURCE_DIR.mkdir(parents=True, exist_ok=True)
    ARTICLE_TEXT_DIR.mkdir(parents=True, exist_ok=True)


def ensure_cloudflare_credentials_file() -> None:
    """Create a private credential template beside dsa.py when absent."""
    if CLOUDFLARE_CREDENTIALS_FILE.exists():
        return
    CLOUDFLARE_CREDENTIALS_FILE.write_text(
        "# Keep this file private. Do not publish or commit it.\n"
        "CLOUDFLARE_API_TOKEN=PASTE_YOUR_CLOUDFLARE_API_TOKEN_HERE\n"
        "CLOUDFLARE_ACCOUNT_ID=PASTE_YOUR_CLOUDFLARE_ACCOUNT_ID_HERE\n",
        encoding="utf-8",
    )
    try:
        CLOUDFLARE_CREDENTIALS_FILE.chmod(0o600)
    except OSError:
        pass


def load_cloudflare_credentials() -> dict[str, str]:
    """Load the two required Cloudflare values without logging secrets."""
    ensure_cloudflare_credentials_file()
    values: dict[str, str] = {}
    for raw_line in CLOUDFLARE_CREDENTIALS_FILE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    required = ("CLOUDFLARE_API_TOKEN", "CLOUDFLARE_ACCOUNT_ID")
    missing = [
        key for key in required
        if not values.get(key) or values[key].startswith("PASTE_YOUR_")
    ]
    if missing:
        raise RuntimeError(
            f"Edit {CLOUDFLARE_CREDENTIALS_FILE} and provide: " + ", ".join(missing)
        )
    return values


def validate_public_site_for_deploy() -> None:
    """Fail closed if expected output is absent or a secret could be published."""
    if not OUTPUT_DIR.is_dir():
        raise RuntimeError(f"Public site folder does not exist: {OUTPUT_DIR}")
    if not DASHBOARD_FILE.is_file() or DASHBOARD_FILE.stat().st_size == 0:
        raise RuntimeError(f"Generated dashboard is missing or empty: {DASHBOARD_FILE}")
    try:
        CLOUDFLARE_CREDENTIALS_FILE.resolve().relative_to(OUTPUT_DIR.resolve())
    except ValueError:
        pass
    else:
        raise RuntimeError(
            "Refusing deployment because cloudflare_api_token.txt is inside the public "
            "site folder. Move dsa.py and its credential file outside OUTPUT_DIR."
        )


def _deploy_to_cloudflare_pages_now() -> None:
    """Deploy the complete public site folder to Pages production via Wrangler."""
    validate_public_site_for_deploy()
    credentials = load_cloudflare_credentials()
    local_wrangler = SCRIPT_DIR / "node_modules" / ".bin" / "wrangler"
    if local_wrangler.is_file():
        command = [str(local_wrangler)]
    elif shutil.which("npx"):
        command = ["npx", "--yes", "wrangler"]
    elif shutil.which("wrangler"):
        command = ["wrangler"]
    else:
        raise RuntimeError(
            "Wrangler was not found. Install Node.js, then run: "
            "npm install --save-dev wrangler"
        )
    command.extend([
        "pages", "deploy", str(OUTPUT_DIR),
        "--project-name", CLOUDFLARE_PROJECT,
        "--branch", CLOUDFLARE_PRODUCTION_BRANCH,
        "--commit-dirty=true",
    ])
    env = os.environ.copy()
    env.update(credentials)
    log(
        f"Deploying {OUTPUT_DIR} to Cloudflare Pages project "
        f"{CLOUDFLARE_PROJECT!r}, production branch {CLOUDFLARE_PRODUCTION_BRANCH!r}..."
    )
    completed = subprocess.run(
        command,
        cwd=str(SCRIPT_DIR),
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    for line in (completed.stdout or "").splitlines():
        log(f"  [wrangler] {line}")
    if completed.returncode != 0:
        raise RuntimeError(
            f"Cloudflare Pages deployment failed with exit code {completed.returncode}. "
            "The previous production deployment remains in place."
        )
    log("Cloudflare Pages production deployment completed successfully.")
def deploy_to_cloudflare_pages() -> None:
    """Publish every run, then retain active production plus one snapshot per local day."""
    _deploy_to_cloudflare_pages_now()
    try:
        from cleanup_cloudflare_deployments import prune_to_daily_snapshots
        summary = prune_to_daily_snapshots(force=True, delay=0.20)
        log(
            "Cloudflare deployment retention completed: "
            f"kept {summary['kept']}, deleted {summary['deleted']}, failed {summary['failed']}."
        )
    except Exception as exc:
        # Publishing succeeded. Cleanup failure is retried after the next deployment.
        log(
            "[!] Cloudflare deployment succeeded, but retention cleanup failed: "
            f"{type(exc).__name__}: {exc}"
        )
def log(msg: str) -> None:
    print(msg, flush=True)
    ensure_dirs()
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n")


def normalize_space(text: str) -> str:
    text = html.unescape(text or "").replace("\xa0", " ")
    text = re.sub(r"\r\n?|\n", "\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n[ \t]+", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def html_to_text(value: str) -> str:
    if not value:
        return ""
    value = html.unescape(str(value))
    soup = BeautifulSoup(value, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(" ")
    return normalize_space(text)


# -----------------------------------------------------------------------------
# Source-Specific DOM Extraction Engine
# -----------------------------------------------------------------------------
SOURCE_DOM_CONFIGS = {
    "www.groundworkdsa.com": {
        "body": ".blog-item-content, .entry-content, article.h-entry",
        "exclude": [".item-pagination", ".related-posts", ".sqs-share-buttons"]
    },
    "caracoldsa.org": {
        "body": ".entry-content",
        "exclude": ["#jp-post-flair", ".sharedaddy", ".wp-block-template-part"]
    },
    "communistcaucus.com": {
        "body": ".entry-content",
        "exclude": [".wpzoom-social-sharing-buttons-bottom"]
    },
    "spade.work": {
        "body": "article.gh-article > section.gh-content, article.gh-article .gh-content, .gh-content",
        "exclude": [".gh-post-upgrade-cta"]
    },
    "houstondsa.org": {
        "body": ".entry-content",
        "exclude": [".wp-block-post-navigation-link", ".yoast-breadcrumbs", ".wp-block-post-date", ".taxonomy-category"]
    },
    "dsaemerge.org": {
        "body": ".entry-content",
        "exclude": [".wp-block-buttons"]
    },
    "dsa-lsc.org": {
        "body": ".entry-content",
        "exclude": [".pmb-print-this-page", ".sharedaddy", ".wp-block-comments"]
    },
    "www.dsanorthstar.org": {
        "body": "#wsite-content .blog-post > .blog-content, #wsite-content .blog-post .blog-content, #656377232525154383-blog #wsite-content .blog-post > .blog-content, #main-wrap .blog-post > .blog-content, #main-wrap .blog-content, #main-wrap .blog-post, #main-wrap",
        "exclude": [".blog-page-nav-previous", ".blog-page-nav-next", "#commentArea", "#header-wrap", "#footer-wrap", "#navmobile", "#banner-wrap", ".wsite-header-elements", ".blog-sidebar", ".column-blog", ".blog-social", ".blog-comments", ".blog-comments-bottom", ".blog-post-separator"]
    },
    "partisanmag.com": {
        "body": ".pf-content, .entry-content",
        "exclude": [".printfriendly", ".et_bloom_below_post", ".et_social_bottom_trigger", ".wp-block-uagb-buttons"]
    },
    "powermapmag.substack.com": {
        "body": ".body.markup, .available-content",
        "exclude": [".subscribe-widget", ".post-footer", ".post-ufi", ".button-wrapper"]
    },
    "redstarcaucus.org": {
        "body": ".gh-content",
        "exclude": [".gh-post-upgrade-cta", ".kg-bookmark-card"]
    },
    "www.21csocialism.org": {
        "body": "article.article.post > section.gh-content, article.article.post .gh-content, section.gh-content.gh-canvas, .gh-content",
        "exclude": [".gh-post-upgrade-cta", ".kg-cta-card", ".kg-signup-card", ".kg-product-card", ".footer-cta", ".read-more-wrap", ".article-byline", ".article-header", ".post-card"]
    },
    "reformandrevolution.org": {
        "body": ".entry-content",
        "exclude": [".bm-social-sharing", "[class*='mailmunch-forms']", ".m-a-box", ".bam-related-posts", ".category-list", ".post-thumbnail", ".entry-footer"]
    },
    "socialistcall.com": {
        "body": ".entry-content",
        "exclude": [".related-articles", ".author", ".featured-caption", ".post-thumbnail"]
    },
    "www.socialistmajority.com": {
        "body": ".blog-item-content, article.h-entry",
        "exclude": [".item-pagination", ".sqs-share-buttons"]
    },
    "www.marxistunity.com": {
        "body": ".c-post-content",
        "exclude": [".c-post-actions", ".c-post-authors", ".c-related-section", ".kg-cta-card", ".c-post-header"]
    },
    "democraticleft.dsausa.org": {
        "body": ".entry-content",
        "exclude": [".sharedaddy", ".wp-block-post-featured-image", ".is-meta-field", ".wp-block-post-date", ".wp-block-post-excerpt"]
    },
    "www.liberationcaucus.org": {
        "body": ".gh-content",
        "exclude": [".kg-card"]
    },
    "mdcdsa.org": {
        "body": ".entry-content",
        "exclude": [".comments-wrapper"]
    },
    "buffalodsa.org": {
        "body": ".entry-content",
        "exclude": [".wp-block-group.has-link-color"]
    },
    "redmadison.com": {
        "body": ".entry-content",
        "exclude": [".wp-block-comments"]
    }
}


COMMON_DOM_EXCLUDES = [
    "script", "style", "noscript", "svg", "form", "nav", "footer", "header", "aside", "iframe", "button",
    "[role='navigation']", "[aria-label='breadcrumb']", ".breadcrumb", ".breadcrumbs", ".yoast-breadcrumbs",
    ".comments", ".comment", ".comments-area", ".comments-wrapper", "#comments", "#commentArea",
    ".related", ".related-post", ".related-posts", ".related-articles", ".c-related-section",
    ".share", ".sharedaddy", ".sqs-share-buttons", ".bm-social-sharing", "[class*='social-sharing']",
    ".subscribe", ".subscribe-widget", ".newsletter", ".post-ufi", ".button-wrapper",
    ".item-pagination", ".pagination-single", ".wp-block-post-navigation-link",
    ".entry-footer", ".post-footer", ".c-post-actions", ".gh-post-upgrade-cta", ".kg-cta-card",
]

BOILERPLATE_TEXT_RE = re.compile(
    r"(?:subscribe|sign in|share this|related articles|you might also like|leave a reply|"
    r"previous|next|cookie|privacy policy|terms of use|powered by)",
    re.I,
)


def apply_excludes(soup: BeautifulSoup | Tag, selectors: list[str] | tuple[str, ...]) -> None:
    """Remove shared and source-specific boilerplate blocks from a BeautifulSoup tree."""
    for ex in [*COMMON_DOM_EXCLUDES, *list(selectors or [])]:
        try:
            for bad_tag in soup.select(ex):
                bad_tag.decompose()
        except Exception:
            # Keep extraction resilient if a site-specific selector is not accepted by SoupSieve.
            continue


def article_node_score(node: Tag) -> int:
    """
    Score a candidate article node without changing it.
    Higher scores favor long, text-dense article bodies with headings/paragraphs/lists
    and penalize link-heavy or boilerplate-heavy containers.
    """
    text = normalize_space(node.get_text(" "))
    if not text:
        return 0

    text_len = len(text)
    paragraphs = len(node.find_all("p"))
    headings = len(node.find_all(["h1", "h2", "h3", "h4", "h5", "h6"]))
    list_items = len(node.find_all("li"))
    blockquotes = len(node.find_all("blockquote"))
    links = len(node.find_all("a"))
    link_text_len = sum(len(normalize_space(a.get_text(" "))) for a in node.find_all("a"))
    link_density = link_text_len / max(text_len, 1)
    boilerplate_hits = len(BOILERPLATE_TEXT_RE.findall(text))

    score = text_len
    score += paragraphs * 80
    score += headings * 60
    score += list_items * 30
    score += blockquotes * 50
    score -= int(link_density * 600)
    score -= links * 4
    score -= boilerplate_hits * 180

    # Very small containers are usually labels, nav fragments, or empty wrappers.
    if text_len < 250:
        score -= 500

    return max(score, 0)


def select_best_article_node(soup: BeautifulSoup, selectors_string: str) -> Optional[tuple[Tag, str, int]]:
    """
    Evaluate configured selector matches and return the best article node.

    The selector list is ordered from most-specific to broadest. Earlier selectors
    receive a bounded priority bonus so a precise article-body selector can beat a
    broad wrapper that contains the article plus sidebar/footer material. Tiny or
    mostly-empty nodes still lose because article_node_score() returns a low score.
    """
    best: Optional[tuple[Tag, str, int]] = None
    seen: set[int] = set()
    selectors = [s.strip() for s in (selectors_string or "").split(",") if s.strip()]
    total = len(selectors)

    for index, selector in enumerate(selectors):
        try:
            nodes = soup.select(selector)
        except Exception:
            continue
        priority_bonus = max(total - index, 0) * 10000
        for node in nodes:
            node_id = id(node)
            if node_id in seen:
                continue
            seen.add(node_id)
            raw_score = article_node_score(node)
            if raw_score <= 0:
                continue
            score = raw_score + priority_bonus
            if best is None or score > best[2]:
                best = (node, selector, score)

    return best

def extract_article_text(raw_html: str, url: str = "") -> tuple[str, str]:
    """
    Extract the main readable article body from an archived HTML page using
    domain-specific DOM selectors, with fallback heuristics. Ensures paragraph
    formatting is preserved and compiles elements into Markdown for AI readability.
    """
    soup = BeautifulSoup(raw_html or "", "html.parser")
    host = ""
    if url:
        host = urllib.parse.urlparse(url).netloc.lower().replace("www.", "")
        if f"www.{host}" in SOURCE_DOM_CONFIGS:
            host = f"www.{host}"

    # Global cleanup of obvious non-text elements
    for tag in soup(["script", "style", "noscript", "svg", "form", "nav", "footer", "header", "aside", "iframe", "button"]):
        tag.decompose()

    best_node = None
    best_method = ""
    config = SOURCE_DOM_CONFIGS.get(host)

    if config:
        # 1. Apply source-specific and shared boilerplate exclusions
        apply_excludes(soup, config.get("exclude", []))

        # 2. Find the best body container among all configured matches
        preferred = select_best_article_node(soup, config.get("body", ""))
        if preferred:
            best_node, body_sel, score = preferred
            best_method = f"source_config_scored:{body_sel}:score={score}"

    # 3. Fallback to generic heuristic cascade
    if not best_node:
        selectors = [
            "article", "main article", "main", ".entry-content", ".post-content",
            ".post-body", ".article-content", ".article-body", ".content",
            ".gh-content", ".kg-card-markdown", ".sqs-html-content",
            ".blog-item-content", ".wp-block-post-content"
        ]
        best_len = 0
        for selector in selectors:
            for el in soup.select(selector):
                text_len = len(el.get_text(strip=True))
                if text_len > best_len:
                    best_len = text_len
                    best_node = el
                    best_method = selector

    # 4. Final fallback: whole body
    if not best_node:
        best_node = soup.body or soup
        best_method = "whole_page"

    import copy
    work_node = copy.deepcopy(best_node)
    
    # 5. DOM to Markdown Compiler
    for tag in work_node.find_all(['strong', 'b']):
        tag.insert_before('**')
        tag.insert_after('**')
        tag.unwrap()
    for tag in work_node.find_all(['em', 'i']):
        tag.insert_before('*')
        tag.insert_after('*')
        tag.unwrap()
    for tag in work_node.find_all('a'):
        href = tag.get('href')
        if href and tag.text.strip():
            abs_href = urllib.parse.urljoin(url, href)
            tag.insert_before('[')
            tag.insert_after(f']({abs_href})')
        tag.unwrap()
    for tag in work_node.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
        level = int(tag.name[1])
        tag.insert_before('\n\n' + ('#' * level) + ' ')
        tag.insert_after('\n\n')
        tag.unwrap()
    for tag in work_node.find_all('li'):
        tag.insert_before('\n• ')
        tag.unwrap()
    for tag in work_node.find_all('blockquote'):
        tag.insert_before('\n\n> ')
        tag.insert_after('\n\n')
        tag.unwrap()
    for tag in work_node.find_all(['p', 'div', 'section', 'article', 'ul', 'ol']):
        tag.insert_before('\n\n')
        tag.insert_after('\n\n')
        tag.unwrap()
    for tag in work_node.find_all('br'):
        tag.replace_with('\n')

    raw_text = work_node.get_text()

    # 6. Sweeper Logic for Extracted Text
    exact_skips = {
        "listen", "copy link", "email", "x", "bluesky", "mastodon", "linkedin",
        "facebook", "whatsapp", "telegram", "reddit", "previous", "next",
        "written by", "welcome", "principles", "join us", "our strategy", "blog",
        "about", "about north star", "about dsa", "contact", "home", "sunrise",
        "people's action", "dream defenders", "dsa north star",
        "building a global progressive network to combat fascism - dsa north star",
        "national network for immigrant and refugee rights",
        "add marxist unity group on google", "·", "share this:", "like", "uncategorized",
        "loading…"
    }
    
    skip_patterns = [
        r"^subscribe$",
        r"^sign in$",
        r"^share$",
        r"^skip to content$",
        r"^cookie",
        r"^privacy policy$",
        r"^terms of use$",
        r"^powered by",
        r"^\d+\s*min read$",
        r"^© copyright",
        r"^liked what you read\?$",
        r"^more in [a-z\.\s]+$",
        r"^\d+$",
        r"^share on [a-z\s]+ \(opens in new window\)$"
    ]

    cleaned_lines = []
    lines = [line.strip() for line in raw_text.split('\n')]
    
    i = 0
    while i < len(lines):
        line = lines[i]
        i += 1
        if not line:
            continue
            
        # Collapse internal spacing for processing
        line = re.sub(r' {2,}', ' ', line)
        
        # Strip out Markdown link syntax to see if the core text is an exact skip match
        pure_text = re.sub(r'\[(.*?)\]\(.*?\)', r'\1', line).strip().lower()

        if pure_text in exact_skips:
            continue
            
        # For "Previous / Next" pagination pairs, skip current and next
        if pure_text in {"previous", "next"}:
            if i < len(lines):
                next_pure = re.sub(r'\[(.*?)\]\(.*?\)', r'\1', lines[i]).strip().lower()
                if next_pure == pure_text:
                    i += 1 # skip duplicate label
                    if i < len(lines):
                        i += 1 # skip title
            continue
            
        if any(re.search(pat, pure_text, flags=re.I) for pat in skip_patterns):
            continue
            
        # Clean up empty markdown shells (e.g. "** **")
        line = line.replace('** **', ' ').replace('* *', ' ')
        if line.strip() in ['**', '*']:
            continue

        cleaned_lines.append(line)

    final_text = '\n\n'.join(cleaned_lines)
    final_text = trim_extracted_text(final_text, url)
    # Collapse 3+ newlines into 2
    final_text = re.sub(r'\n{3,}', '\n\n', final_text).strip()

    return final_text, best_method



def trim_extracted_text(text: str, url: str = "") -> str:
    """
    Trim publication-specific tail boilerplate that survives DOM selection.
    North Star/Weebly pages can flatten the blog sidebar into the selected wrapper
    if a broad container wins; this cuts only after substantial article text exists.
    """
    host = normalized_url_host(url)
    if host != "dsanorthstar.org":
        return text

    stop_exact = {
        "rss feed",
        "north star caucus members",
        "socialist education",
        "left periodicals",
        "comrades",
    }
    stop_prefixes = (
        "**[principles]",
        "[**our strategy**]",
        "**principles",
        "the opinions expressed here are those of members and allies of dsa north star caucus",
    )

    lines = text.split("\n\n")
    kept: list[str] = []
    chars_seen = 0
    for line in lines:
        pure = re.sub(r"\[(.*?)\]\(.*?\)", r"\1", line).strip().lower()
        if chars_seen > 1000:
            if pure == "0 comments" or pure in stop_exact or any(pure.startswith(p) for p in stop_prefixes):
                break
        kept.append(line)
        chars_seen += len(line) + 2

    return "\n\n".join(kept).strip()


def save_article_text_file(article_id: str, title: str, article_text: str) -> str:
    if not article_text:
        return ""
    name = f"{article_id}_{slugify(title)}.txt"
    path = ARTICLE_TEXT_DIR / name
    # Prepend a UTF-8 Byte Order Mark (BOM) so browsers like Safari render special characters correctly
    path.write_bytes(('\ufeff' + article_text).encode("utf-8"))
    return str(path.relative_to(OUTPUT_DIR))


def clean_title(title: str) -> str:
    title = normalize_space(title)
    patterns = [
        r"\s*-\s*LIBERTARIAN\s*SOCIALISTAUCUS$",
        r"\s*–\s*LIBERTARIAN\s*SOCIALISTAUCUS$",
        r"\s*\|\s*Reform\s*&\s*Revolution$",
        r"\s*-\s*The\s*Call$",
        r"\s*-\s*Democratic\s*Socialists\s*of\s*America$",
        r"\s*-\s*Democratic\s*Left$",
        r"\s*-\s*Google\s*Docs$",
        r"\s*-\s*Google\s*Forms$",
        r"\s*-\s*Substack$",
    ]
    for pattern in patterns:
        title = re.sub(pattern, "", title, flags=re.I)
    return title.strip()


def short(text: str, n: int = 220) -> str:
    text = normalize_space(text).replace("\n", " ")
    return text if len(text) <= n else text[:n].rsplit(" ", 1)[0] + "..."


def stable_id(source: str, link: str, title: str = "") -> str:
    return hashlib.sha256(f"{source}|{link}|{title}".encode("utf-8", errors="ignore")).hexdigest()[:24]


def slugify(text: str, fallback: str = "source") -> str:
    text = normalize_space(text).lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text[:80] or fallback


def canonicalize_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    return urllib.parse.urlunparse(parsed._replace(fragment=""))



def normalized_url_host(url: str) -> str:
    """Normalize host values for source filtering and diagnostics."""
    value = normalize_space(url).lower()
    if "://" not in value:
        value = "//" + value
    host = urllib.parse.urlparse(value).netloc.lower()
    return host[4:] if host.startswith("www.") else host


def detect_platform_schema(raw_html: str, url: str = "") -> str:
    """
    Detect broad publishing platform/schema from visible DOM evidence.
    This is intentionally heuristic and used for filtering/diagnostics only.
    """
    soup = BeautifulSoup(raw_html or "", "html.parser")
    host = normalized_url_host(url)
    body_classes = " ".join(soup.body.get("class", [])) if soup.body else ""

    if host == "medium.com" or "medium.com" in host:
        return "medium"
    if soup.select_one("#wsite-content .blog-post, .wsite-menu-default, [id^='wsite-'], .wsite-elements") or "wsite-" in body_classes:
        return "weebly"
    if soup.select_one("article.h-entry, article#sections, .blog-item-wrapper, .sqs-layout, .sqs-html-content"):
        return "squarespace"
    if soup.select_one(".body.markup, .available-content, .pencraft, .post-ufi"):
        return "substack"
    if soup.select_one(".gh-content, .kg-card, .kg-bookmark-card, .kg-cta-card, .c-post-content"):
        return "ghost"
    if soup.select_one(".entry-content, .wp-block-post-content, body[class*='wordpress'], body[class*='wp-']"):
        return "wordpress"
    if soup.select_one("article, main article"):
        return "semantic_article"
    return "unknown"


def candidate_matches_resolved_host(c: dict[str, Any], resolved_host: str) -> bool:
    """Return True when a candidate URL resolves to the requested normalized host."""
    wanted = normalized_url_host(resolved_host)
    return not wanted or normalized_url_host(c.get("url", "")) == wanted


def decode_bytes(data: bytes) -> str:
    for enc in ("utf-8", "utf-8-sig", "latin-1"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="replace")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def db_connect() -> sqlite3.Connection:
    """
    Open the scraper database after ensuring its parent directories exist.

    This prevents sqlite3.OperationalError: unable to open database file
    when a DB helper is called after the data directory is missing, moved,
    or not yet created in the current runtime context.
    """
    ensure_dirs()
    return sqlite3.connect(str(DB_FILE))


@contextmanager
def db_session() -> sqlite3.Connection:
    """
    Open, commit/rollback, and always close the scraper SQLite database.

    sqlite3.Connection used directly as a context manager commits or rolls
    back transactions, but does not close the connection. This wrapper prevents
    leaked file handles during large discovery runs.
    """
    ensure_dirs()
    conn = sqlite3.connect(str(DB_FILE))
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def parse_date_robust(date_str: str) -> Optional[datetime]:
    """Robustly interprets dateless formatting and corrects future overflows."""
    date_str = normalize_space(date_str)
    if not date_str:
        return None
        
    now_dt = datetime.now(timezone.utc)
    
    # 1. Standard ISO Format
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except Exception:
        pass
        
    # 2. Native Email/RFC format
    try:
        from email.utils import parsedate_to_datetime
        dt = parsedate_to_datetime(date_str)
        if dt: return dt
    except Exception:
        pass
        
    # Standardize ordinal suffixes to raw day values to prevent strptime errors (e.g. 1st, 2nd, 3rd)
    clean_str = re.sub(r'(?<=\d)(st|nd|rd|th)\b', '', date_str, flags=re.IGNORECASE)
    
    # 3. Explicit Year Formats
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(clean_str, fmt)
        except Exception:
            pass
        try:
            return datetime.strptime(date_str, fmt)
        except Exception:
            pass

    # 4. Handle month-day only (e.g., "June 27" or "Oct 5")
    for fmt in ("%B %d", "%b %d"):
        try:
            dt = datetime.strptime(clean_str, fmt)
            # Baseline assumptions: article was published in the current year
            dt = dt.replace(year=now_dt.year)
            # If standardizing it places the date in the future (e.g. evaluating "October 27" while 
            # the system is in July), send it back by a year to accurately align the sort. 
            if dt > now_dt.replace(tzinfo=None):
                dt = dt.replace(year=now_dt.year - 1)
            return dt
        except Exception:
            pass
            
    return None


def parse_to_iso(date_str: str) -> str:
    """Normalizes multiple date formats to standard ISO 8601 strings for deterministic client-side sorting."""
    dt = parse_date_robust(date_str)
    return dt.isoformat() if dt else now_iso()


def format_display_date(value: str) -> str:
    dt = parse_date_robust(value)
    if dt:
        # Prevents zero padding (05) without needing system-dependent formatters (-d vs #d)
        return dt.strftime(f"%B {dt.day}, %Y")
    return normalize_space(value) if value else "Date unknown"


def fetch_url(url: str, timeout: int = 35) -> Optional[bytes]:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.8",
        "Accept-Encoding": "identity, gzip",
    }
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read()
            encoding = (resp.headers.get("Content-Encoding") or "").lower()
            if "gzip" in encoding or data.startswith(bytes([0x1f, 0x8b])):
                try:
                    data = gzip.decompress(data)
                except Exception as gz_e:
                    log(f"  [!] gzip decompress failed for {url}: {type(gz_e).__name__}: {gz_e}")
            return data
    except Exception as e:
        log(f"  [X] fetch failed {url}: {type(e).__name__}: {e}")
        return None


def make_candidate(source: SourceSpec, title: str, url: str, pub_date_hint: str = "", snippet: str = "", method: str = "") -> dict[str, Any]:
    url = canonicalize_url(urllib.parse.urljoin(source.url, url))
    title = normalize_space(title)
    return {
        "id": stable_id(source.name, url, title),
        "source_key": source.key,
        "source": source.name,
        "title": title,
        "url": url,
        "pub_date_hint": normalize_space(pub_date_hint),
        "snippet": normalize_space(snippet),
        "method": method
    }


def write_discovered(catalog: list[dict[str, Any]]) -> None:
    DISCOVERED_JSON.write_text(json.dumps(catalog, indent=2, ensure_ascii=False), encoding="utf-8")
    groups: dict[str, list[dict[str, Any]]] = {}
    for c in catalog:
        groups.setdefault(c["source"], []).append(c)
    body = [f"<p>{len(catalog)} broad candidates before mechanical selection filtering.</p>"]
    for source, items in groups.items():
        body.append(f"<h2>{html.escape(source)} <small>({len(items)})</small></h2><ol>")
        for c in items:
            body.append(f"<li><a href='{html.escape(c['url'])}'>{html.escape(c['title'])}</a> <small>{html.escape(c.get('pub_date_hint',''))} · {html.escape(c.get('method',''))}</small></li>")
        body.append("</ol>")
    DISCOVERED_HTML.write_text("<!doctype html><meta charset='utf-8'><title>Discovered Links</title><body><h1>Discovered Links</h1>" + "".join(body) + "</body>", encoding="utf-8")
    log(f"\nFull candidate list written: {DISCOVERED_HTML}")


# -----------------------------------------------------------------------------
# SQLite Storage
# -----------------------------------------------------------------------------
def init_db(reset: bool = False) -> None:
    ensure_dirs()
    with db_session() as conn:
        if reset:
            conn.execute("DROP TABLE IF EXISTS candidates")
            conn.execute("DROP TABLE IF EXISTS intel_feed")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS candidates (
                id TEXT PRIMARY KEY,
                source_key TEXT,
                source TEXT,
                title TEXT,
                url TEXT,
                pub_date_hint TEXT,
                snippet TEXT,
                method TEXT,
                selected INTEGER DEFAULT 0,
                ai_candidate_status TEXT DEFAULT '',
                ai_reason TEXT DEFAULT ''
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS intel_feed (
                id TEXT PRIMARY KEY,
                candidate_id TEXT,
                source_key TEXT,
                source TEXT,
                title TEXT,
                link TEXT,
                pub_date TEXT,
                author TEXT,
                description TEXT,
                content_type TEXT,
                image_url TEXT,
                raw_source TEXT,
                local_source_path TEXT,
                ai_metadata_json TEXT,
                date_fetched TEXT,
                metadata_status TEXT,
                article_text TEXT,
                article_text_method TEXT,
                article_classification TEXT,
                local_text_path TEXT
            )
        """)
        for table, cols in {
            "candidates": {"source_key":"TEXT", "source":"TEXT", "title":"TEXT", "url":"TEXT", "pub_date_hint":"TEXT", "snippet":"TEXT", "method":"TEXT", "selected":"INTEGER DEFAULT 0", "ai_candidate_status":"TEXT DEFAULT ''", "ai_reason":"TEXT DEFAULT ''"},
            "intel_feed": {"candidate_id":"TEXT", "source_key":"TEXT", "source":"TEXT", "title":"TEXT", "link":"TEXT", "pub_date":"TEXT", "author":"TEXT", "description":"TEXT", "content_type":"TEXT", "image_url":"TEXT", "raw_source":"TEXT", "local_source_path":"TEXT", "ai_metadata_json":"TEXT", "date_fetched":"TEXT", "metadata_status":"TEXT", "article_text":"TEXT", "article_text_method":"TEXT", "article_classification":"TEXT", "local_text_path":"TEXT"},
        }.items():
            existing = {r[1] for r in conn.execute(f"PRAGMA table_info({table})")}
            for col, decl in cols.items():
                if col not in existing:
                    conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {decl}")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_candidates_source ON candidates(source_key)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_candidates_selected ON candidates(selected)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_feed_source ON intel_feed(source_key)")


def upsert_candidate(c: dict[str, Any]) -> None:
    with db_session() as conn:
        conn.execute("""
            INSERT INTO candidates (id, source_key, source, title, url, pub_date_hint, snippet, method)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
              source_key=excluded.source_key, source=excluded.source, title=excluded.title,
              url=excluded.url, pub_date_hint=excluded.pub_date_hint, snippet=excluded.snippet,
              method=excluded.method
        """, (c["id"], c["source_key"], c["source"], c["title"], c["url"], c.get("pub_date_hint", ""), c.get("snippet", ""), c.get("method", "")))


def mark_candidate(cid: str, selected: bool, status: str, reason: str = "") -> None:
    with db_session() as conn:
        conn.execute("UPDATE candidates SET selected=?, ai_candidate_status=?, ai_reason=? WHERE id=?", (1 if selected else 0, status, reason, cid))


def upsert_article(row: dict[str, Any]) -> None:
    cols = list(row.keys())
    placeholders = ",".join("?" for _ in cols)
    assignments = ",".join(f"{c}=excluded.{c}" for c in cols if c != "id")
    with db_session() as conn:
        conn.execute(f"INSERT INTO intel_feed ({','.join(cols)}) VALUES ({placeholders}) ON CONFLICT(id) DO UPDATE SET {assignments}", [row[c] for c in cols])


# -----------------------------------------------------------------------------
# XML & Linktree Parsing
# -----------------------------------------------------------------------------
def sanitize_xml_payload(data: bytes) -> bytes:
    text = decode_bytes(data)
    text = text.replace("&nbsp;", " ")
    text = text.replace("&nbsp", " ")
    text = text.replace("&mdash;", "—")
    text = text.replace("&ndash;", "–")
    text = text.replace("&ldquo;", "“")
    text = text.replace("&rdquo;", "”")
    text = text.replace("&lsquo;", "‘")
    text = text.replace("&rsquo;", "’")
    text = text.replace("&hellip;", "…")
    text = text.replace("&middot;", "·")
    text = re.sub(r"&(?!amp;|lt;|gt;|quot;|apos;|#\d+;|#x[0-9a-fA-F]+;)", "&amp;", text)
    return text.encode("utf-8", errors="ignore")


def parse_any_feed(data: bytes, source: SourceSpec) -> list[dict[str, Any]]:
    try:
        root = ET.fromstring(sanitize_xml_payload(data))
    except Exception as e:
        log(f"  [!] XML parse warning on {source.name}: {e}; trying BeautifulSoup XML fallback")
        soup = BeautifulSoup(decode_bytes(data), "xml")
        items = soup.find_all("item")
        if not items:
            log(f"  [X] XML parse error on {source.name}: {e}")
            return []
        candidates: list[dict[str, Any]] = []
        for item in items:
            def txt(name: str) -> str:
                el = item.find(name)
                return normalize_space(el.get_text(" ")) if el else ""
            title = clean_title(txt("title") or "Untitled")
            link = txt("link")
            pub_date = txt("pubDate") or txt("published") or txt("updated")
            summary = txt("description") or txt("summary") or txt("encoded")
            author = txt("creator") or txt("author")
            if link:
                c = make_candidate(source, title, link, pub_date, html_to_text(summary), "rss_feed_bs4_fallback")
                c["author_hint"] = author
                c["image_url_hint"] = ""
                candidates.append(c)
        return candidates

    tag_lower = root.tag.lower()
    out = []

    ns = {
        'dc': 'http://purl.org/dc/elements/1.1/',
        'content': 'http://purl.org/rss/1.0/modules/content/',
        'media': 'http://search.yahoo.com/mrss/',
        'atom': 'http://www.w3.org/2005/Atom'
    }

    def find_val(elem: ET.Element, tag_name: str) -> str:
        val = elem.find(tag_name)
        if val is not None and val.text:
            return val.text.strip()
        for prefix, uri in ns.items():
            val = elem.find(f"{{{uri}}}{tag_name}")
            if val is not None and val.text:
                return val.text.strip()
        for child in list(elem):
            if child.tag.endswith("}" + tag_name) or child.tag == tag_name:
                if child.text:
                    return child.text.strip()
        return ""

    if "feed" in tag_lower:
        entries = root.findall(".//{http://www.w3.org/2005/Atom}entry") or root.findall(".//entry")
        for entry in entries:
            title = clean_title(find_val(entry, "title") or "Untitled")
            link = ""
            link_el = entry.find("{http://www.w3.org/2005/Atom}link") or entry.find("link")
            if link_el is not None:
                link = link_el.get("href") or ""
            if not link:
                for l in entry.findall(".//link") + entry.findall(".//{http://www.w3.org/2005/Atom}link"):
                    href = l.get("href")
                    rel = l.get("rel")
                    if href and (not rel or rel == "alternate"):
                        link = href
                        break

            pub_date = find_val(entry, "published") or find_val(entry, "updated")
            author = ""
            author_el = entry.find(".//{http://www.w3.org/2005/Atom}author") or entry.find(".//author")
            if author_el is not None:
                author = find_val(author_el, "name")

            summary = find_val(entry, "summary") or find_val(entry, "content")

            if link:
                out.append({
                    "title": title,
                    "url": link,
                    "pub_date_hint": pub_date,
                    "author": author,
                    "snippet": summary,
                    "image_url": "",
                    "method": "atom_feed"
                })
    else:
        items = root.findall(".//item")
        for item in items:
            title = clean_title(find_val(item, "title") or "Untitled")
            link = find_val(item, "link")
            pub_date = find_val(item, "pubDate") or find_val(item, "date")
            author = find_val(item, "creator") or find_val(item, "author")
            summary = find_val(item, "description") or find_val(item, "summary")

            image_url = ""
            enclosure = item.find("enclosure")
            if enclosure is not None and enclosure.get("type", "").startswith("image/"):
                image_url = enclosure.get("url", "")
            if not image_url:
                media_content = item.find(".//{http://search.yahoo.com/mrss/}content")
                if media_content is None:
                    media_content = item.find(".//media:content", ns)
                if media_content is not None and media_content.get("url"):
                    image_url = media_content.get("url")

            if link:
                out.append({
                    "title": title,
                    "url": link,
                    "pub_date_hint": pub_date,
                    "author": author,
                    "snippet": summary,
                    "image_url": image_url,
                    "method": "rss_feed"
                })

    candidates = []
    for item in out:
        c = make_candidate(
            source,
            item["title"],
            item["url"],
            item["pub_date_hint"],
            html_to_text(item["snippet"]),
            item["method"]
        )
        c["author_hint"] = item.get("author", "")
        c["image_url_hint"] = item.get("image_url", "")
        candidates.append(c)
    return candidates


def parse_linktree_json(source: SourceSpec, html_text: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html_text, "html.parser")
    script = soup.find("script", id="__NEXT_DATA__")
    if not script or not script.string:
        return []
    try:
        data = json.loads(script.string)
    except Exception:
        return []
    raw = []

    def walk(obj: Any) -> None:
        if isinstance(obj, dict):
            if isinstance(obj.get("url"), str) and (obj.get("title") or obj.get("linkTitle")):
                raw.append(obj)
            for v in obj.values():
                walk(v)
        elif isinstance(obj, list):
            for x in obj:
                walk(x)

    walk(data)
    ad_domains = ["hellofresh.com", "hulu.com", "thanks.is", "factor75.com", "samsclub.com", "acorns.com", "headspace.com", "upside.com", "curology.com", "aaa.com", "clearstem.com", "omniluxled.com", "armra.com", "fabletics.com", "purplecarrot.com", "dailyharvest.com", "gobble.com", "maev.com", "zeroproof.com", "jlab.com"]
    out, seen = [], set()
    for obj in raw:
        url = canonicalize_url(str(obj.get("url") or ""))
        title = clean_title(str(obj.get("title") or obj.get("linkTitle") or url))
        low = url.lower()
        if not url.startswith("http") or url in seen:
            continue
        if "linktr.ee" in low:
            continue
        if any(d in low for d in ad_domains):
            continue
        if re.search(r"\.(png|jpg|jpeg|gif|webp)(\?|$)", low):
            continue
        seen.add(url)
        out.append(make_candidate(source, title, url, "", "", "linktree_next_data"))
    return out


# -----------------------------------------------------------------------------
# Targeted Mechanical Metadata Extractor
# -----------------------------------------------------------------------------
def mechanical_metadata(raw_html: str, url: str, fallback_title: str, fallback_date: str = "") -> dict[str, str]:
    soup = BeautifulSoup(raw_html or "", "html.parser")

    def meta_content(*names: str) -> str:
        for name in names:
            tag = soup.find("meta", attrs={"property": name}) or soup.find("meta", attrs={"name": name})
            if tag and tag.get("content"):
                return normalize_space(tag["content"])
        return ""

    title = meta_content("og:title", "twitter:title", "dc:title")
    if not title and soup.title:
        title = normalize_space(soup.title.get_text(" "))
    title = clean_title(title or fallback_title)

    author = meta_content("author", "article:author", "twitter:creator", "twitter:data1")
    if not author or author.lower() in {"admin", "editor", "staff", "author unknown"}:
        author_selectors = [
            "span.author", "a.author", ".post-meta-author",
            ".gh-author-name", ".gh-article-meta .gh-author", "a.author-name",
            ".author-name", ".author-name a", "a.author-name-link",
            ".entry-author-name", ".entry-author a", "[rel='author']", ".byline a",
            ".byline", ".author", ".post-author", "span.meta-author", "h5.is-acf-field"
        ]
        for sel in author_selectors:
            el = soup.select_one(sel)
            if el:
                val = normalize_space(el.get_text(" "))
                val = re.sub(r"(?i)^by\s+", "", val)
                val = re.sub(r"(?i)^written\s+by\s+", "", val)
                if val and len(val) < 100:
                    author = val
                    break

    pub_date = meta_content("article:published_time", "date", "datePublished", "pubdate", "og:pubdate")
    if not pub_date:
        date_selectors = [
            ".gh-article-date", "time[datetime]", "time.entry-date", "time.published", "time.updated",
            ".post-date", ".meta-date", "span.date", ".date-header"
        ]
        for sel in date_selectors:
            el = soup.select_one(sel)
            if el:
                pub_date = el.get("datetime") or normalize_space(el.get_text(" "))
                if pub_date:
                    break
    pub_date = pub_date or fallback_date

    description = meta_content("og:description", "twitter:description", "description")
    if not description or len(description) < 12:
        paragraphs = []
        for p in soup.find_all("p"):
            txt = normalize_space(p.get_text(" "))
            if txt and len(txt) > 30 and not any(skip in txt.lower() for skip in ["skip to content", "javascript", "cookies", "subscribe", "sign up"]):
                paragraphs.append(txt)
                if len(paragraphs) >= 3:
                    break
        if paragraphs:
            description = " ".join(paragraphs)

    image = meta_content("og:image", "twitter:image", "twitter:image:src")
    if not image:
        image_selectors = [
            ".gh-article-image img", ".gh-feature-image img",
            ".wp-post-image", ".entry-content img", "article img",
            ".post-header img", ".featured-media img"
        ]
        for sel in image_selectors:
            el = soup.select_one(sel)
            if el and el.get("src"):
                image = urllib.parse.urljoin(url, el.get("src"))
                break

    content_type = "article"
    low_url = url.lower()
    if "podcast" in low_url or "buzzsprout" in low_url:
        content_type = "podcast"
    elif low_url.endswith(".pdf") or "drive.google.com" in low_url:
        content_type = "pdf/pamphlet"
    elif "docs.google.com" in low_url:
        content_type = "google-doc"

    return {
        "title": title,
        "author": normalize_space(author or "Author unknown"),
        "pub_date": normalize_space(pub_date),
        "description": normalize_space(description),
        "image_url": image,
        "content_type": content_type,
    }


def get_robust_metadata(raw_html: str, url: str, c: dict[str, Any]) -> dict[str, str]:
    html_meta = mechanical_metadata(raw_html, url, c["title"], c.get("pub_date_hint", ""))

    title = html_meta.get("title") or c["title"]
    author = html_meta.get("author") or c.get("author_hint") or "Author unknown"
    if author.lower() in {"admin", "editor", "staff", "author unknown", ""}:
        if c.get("author_hint"):
            author = c["author_hint"]

    pub_date = html_meta.get("pub_date") or c.get("pub_date_hint") or ""
    description = html_meta.get("description") or c.get("snippet") or ""
    image_url = html_meta.get("image_url") or c.get("image_url_hint") or ""
    content_type = html_meta.get("content_type") or "article"

    return {
        "title": normalize_space(title),
        "author": normalize_space(author),
        "pub_date": normalize_space(pub_date),
        "description": normalize_space(short(description, 350)),
        "image_url": image_url,
        "content_type": content_type
    }



def classify_article_text(article_text: str) -> str:
    """
    Classify extracted text for diagnostics and optional dashboard exclusion.
    This does not change extraction itself; it labels likely stubs/link posts.
    """
    text = normalize_space(article_text)
    if not text:
        return "empty_stub"

    stripped_links = re.sub(r"\[(.*?)\]\(.*?\)", r"\1", text)
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    link_count = len(re.findall(r"\]\(https?://", text))
    word_count = len(re.findall(r"\b\w+\b", stripped_links))
    sentence_count = len(re.findall(r"[.!?](?:\s|$)", stripped_links))
    text_len = len(text)

    if text_len < 250:
        return "empty_stub"
    if text_len < 1200 and link_count >= 1 and (sentence_count < 4 or word_count < 140):
        return "link_stub"
    if text_len < 1000:
        return "article_short"
    if len(lines) <= 6 and link_count >= 1 and word_count < 200:
        return "link_stub"
    return "article_full"


def save_source_file(article_id: str, title: str, data: bytes, url: str) -> tuple[str, str]:
    low = url.lower().split("?")[0]
    if data[:5].startswith(b"%PDF-") or low.endswith(".pdf"):
        ext = ".pdf"
        raw_text = "[PDF binary saved locally; raw_source omitted from SQLite text field]"
    else:
        ext = ".html"
        raw_text = decode_bytes(data)
    name = f"{article_id}_{slugify(title)}{ext}"
    path = SOURCE_DIR / name
    path.write_bytes(data)
    # Return the path relative directly to the HTML output dir rather than to SCRIPT_DIR
    # This prevents the 'output/output/source...' local file bug on the frontend.
    return str(path.relative_to(OUTPUT_DIR)), raw_text


def process_candidate(c: dict[str, Any], args: argparse.Namespace) -> None:
    log(f"  -> fetching source for {c['source']}: {short(c['title'], 100)}")
    article_id = stable_id(c["source"], c["url"], c["title"])
    article_text = ""
    article_text_method = ""
    article_classification = ""
    local_text_path = ""
    data = fetch_url(c["url"], timeout=45)

    if not data:
        raw_source = "[Failed to fetch source.]"
        local_path = ""
        fallback = {
            "title": c["title"],
            "author": c.get("author_hint") or "Author unknown",
            "pub_date": c.get("pub_date_hint") or "",
            "description": c.get("snippet") or "",
            "image_url": c.get("image_url_hint") or "",
            "content_type": "unknown"
        }
        meta, status = fallback, "fetch_failed_metadata_fallback"
    else:
        detected_schema = "pdf" if data[:5].startswith(b"%PDF-") else detect_platform_schema(decode_bytes(data), c["url"])
        requested_schema = normalize_space(getattr(args, "detected_schema", "") or "").lower()
        if requested_schema and detected_schema.lower() != requested_schema:
            log(f"     skipped after fetch: detected schema {detected_schema} != --detected-schema {requested_schema}")
            return

        local_path, raw_source = save_source_file(article_id, c["title"], data, c["url"])
        if data[:5].startswith(b"%PDF-"):
            fallback = {
                "title": c["title"],
                "author": c.get("author_hint") or "Author unknown",
                "pub_date": c.get("pub_date_hint") or "",
                "description": c.get("snippet") or "",
                "image_url": "",
                "content_type": "pdf"
            }
            meta, status = fallback, "pdf_source_saved_no_metadata_parsing"
        else:
            meta = get_robust_metadata(raw_source, c["url"], c)
            article_text, article_text_method = extract_article_text(raw_source, c["url"])
            article_classification = classify_article_text(article_text)
            article_text_method = f"{article_text_method};schema={detected_schema};class={article_classification}"
            local_text_path = save_article_text_file(article_id, meta.get("title") or c["title"], article_text)
            status = "deterministic_metadata_parsed"
            min_chars = max(0, int(getattr(args, "min_article_chars", 0) or 0))
            if getattr(args, "skip_short_articles", False) and len(article_text) < min_chars:
                status = "skipped_short_article"

    row = {
        "id": article_id,
        "candidate_id": c["id"],
        "source_key": c["source_key"],
        "source": c["source"],
        "title": meta.get("title") or c["title"],
        "link": c["url"],
        "pub_date": meta.get("pub_date") or c.get("pub_date_hint", ""),
        "author": meta.get("author") or "Author unknown",
        "description": meta.get("description") or c.get("snippet", ""),
        "content_type": meta.get("content_type") or "article",
        "image_url": meta.get("image_url") or "",
        "raw_source": raw_source,
        "local_source_path": local_path,
        "article_text": article_text,
        "article_text_method": article_text_method,
        "article_classification": article_classification,
        "local_text_path": local_text_path,
        "ai_metadata_json": json.dumps({"engine": "deterministic_mechanical_parser"}, ensure_ascii=False),
        "date_fetched": now_iso(),
        "metadata_status": status,
    }
    upsert_article(row)
    log(f"     archived source: {local_path or 'not saved'} ({status}); text: {len(article_text)} chars as {article_classification or 'unclassified'} via {article_text_method or 'none'}")


# Candidate/article gatekeeping -------------------------------------------------
def is_probably_non_article_candidate(c: dict[str, Any]) -> tuple[bool, str]:
    title = normalize_space(c.get("title", ""))
    url = canonicalize_url(c.get("url", ""))
    low = url.lower()
    path = urllib.parse.urlparse(url).path.lower()
    source_key = c.get("source_key", "")

    if not url.startswith(("http://", "https://")):
        return True, "not an http(s) URL"
    if PODCAST_URL_RE.search(title) or PODCAST_URL_RE.search(low):
        return True, "podcast/audio link excluded"
    if NON_ARTICLE_URL_RE.search(low):
        return True, "external doc/social/affiliate/link hub excluded"
    if re.search(r"\.(png|jpg|jpeg|gif|webp|css|js|mp3|m4a|wav|zip)(?:$|[?&])", low):
        return True, "non-article asset link excluded"
    if NAV_TITLE_RE.search(title.strip(" -—–")):
        return True, "navigation/static title excluded"
    if source_key in {"mug_latest", "mug_statements"} and MUG_STATIC_PATH_RE.search(path):
        return True, "MUG static/nav/archive page excluded"
    if source_key == "communist_caucus" and STATIC_PATH_RE.search(path):
        return True, "Communist Caucus static/nav/archive page excluded"
    if source_key not in {"mug_latest", "mug_statements", "communist_caucus"} and STATIC_PATH_RE.search(path):
        return True, "static/archive page excluded"
    return False, ""


def filter_candidates(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    kept = []
    for c in candidates:
        bad, reason = is_probably_non_article_candidate(c)
        if bad:
            mark_candidate(c["id"], False, "rejected_non_article", reason)
            continue
        kept.append(c)
    return kept


def discover_communist_bulletin(source: SourceSpec, html_text: str) -> list[dict[str, Any]]:
    soup = BeautifulSoup(html_text, "html.parser")
    main = soup.select_one("main, article .entry-content, .entry-content, #site-content") or soup
    out, seen = [], set()
    for a in main.find_all("a", href=True):
        title = normalize_space(a.get_text(" ") or a.get("title", "") or "")
        href = canonicalize_url(urllib.parse.urljoin(source.url, a.get("href", "")))
        parsed = urllib.parse.urlparse(href)
        if parsed.netloc.lower().replace("www.", "") != "communistcaucus.com":
            continue
        if href in seen:
            continue
        if not title or title.lower() in {"click to read more…", "click here to read more…", "click to read more", "click here to read more"}:
            parent_text = normalize_space((a.parent.get_text(" ") if a.parent else ""))
            prev = a.find_previous(["h1", "h2", "h3"])
            title = normalize_space(prev.get_text(" ")) if prev else parent_text
        c = make_candidate(source, title[:450], href, nearby_date_hint(a), normalize_space((a.parent.get_text(" ") if a.parent else "")[:900]), "bulletin_html")
        bad, _reason = is_probably_non_article_candidate(c)
        if bad:
            continue
        seen.add(href)
        out.append(c)
    return out


def nearby_date_hint(a: Tag) -> str:
    chunks = []
    for node in [a, a.parent, a.parent.parent if a.parent else None]:
        if node is not None:
            chunks.append(normalize_space(node.get_text(" ")))
    joined = " | ".join(chunks)
    for pat in [r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\s+\d{1,2},\s+20\d{2}\b", r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", r"\b20\d{2}-\d{2}-\d{2}\b"]:
        m = re.search(pat, joined, flags=re.I)
        if m:
            return m.group(0)
    return ""


def discover_html(source: SourceSpec, html_text: str) -> list[dict[str, Any]]:
    if source.key == "communist_caucus":
        return discover_communist_bulletin(source, html_text)
    soup = BeautifulSoup(html_text, "html.parser")
    out, seen = [], set()
    base_domain = urllib.parse.urlparse(source.url).netloc.lower().replace("www.", "")
    for a in soup.find_all("a", href=True):
        title = normalize_space(a.get_text(" ") or a.get("title", "") or a.get("aria-label", ""))
        href = canonicalize_url(urllib.parse.urljoin(source.url, a.get("href", "")))
        if not title or len(title) < 3 or href in seen:
            continue
        parsed = urllib.parse.urlparse(href)
        if parsed.scheme not in {"http", "https"}:
            continue
        if source.same_domain_only and parsed.netloc.lower().replace("www.", "") != base_domain:
            continue
        if re.search(r"\.(png|jpg|jpeg|gif|webp|css|js)(\?|$)", href.lower()):
            continue
        seen.add(href)
        snippet = normalize_space((a.parent.get_text(" ") if a.parent else "")[:900])
        out.append(make_candidate(source, title[:450], href, nearby_date_hint(a), snippet, "anchor_wide"))
    return out


# -----------------------------------------------------------------------------
# Mechanical Feed Discovery
# -----------------------------------------------------------------------------
def discover_all(selected_source: Optional[str], resolved_host: str = "") -> list[dict[str, Any]]:
    catalog, seen_global = [], set()
    for source in SOURCES:
        if selected_source and selected_source.lower() not in source.key.lower() and selected_source.lower() not in source.name.lower():
            continue
        log(f"\nDiscovering {source.name}...")
        data = fetch_url(source.url)
        if not data:
            continue
        try:
            if source.source_type == "rss":
                items = parse_any_feed(data, source)
            elif source.source_type == "linktree":
                items = parse_linktree_json(source, decode_bytes(data))
            else:
                items = discover_html(source, decode_bytes(data))
        except Exception as e:
            log(f"  [X] discovery failed: {type(e).__name__}: {e}")
            continue

        filtered_items = filter_candidates(items)
        rejected = len(items) - len(filtered_items)
        if rejected:
            log(f"  rejected {rejected} non-article/podcast/nav candidate(s)")
        if resolved_host:
            before_host = len(filtered_items)
            filtered_items = [c for c in filtered_items if candidate_matches_resolved_host(c, resolved_host)]
            host_rejected = before_host - len(filtered_items)
            if host_rejected:
                log(f"  host filter rejected {host_rejected} candidate(s) not on {normalized_url_host(resolved_host)}")

        unique = []
        for c in filtered_items:
            if c["url"] not in seen_global:
                seen_global.add(c["url"])
                unique.append(c)
                upsert_candidate(c)
        catalog.extend(unique)
        log(f"  found {len(unique)} candidate(s)")
        for i, c in enumerate(unique[:12], 1):
            log(f"    {i:02d}. {short(c['title'], 95)}")
        if len(unique) > 12:
            log(f"    ... plus {len(unique)-12} more")
    write_discovered(catalog)
    return catalog


def archived_article_keys() -> tuple[set[str], set[str]]:
    """Return candidate IDs and canonical links already represented in intel_feed."""
    with db_session() as conn:
        rows = conn.execute("""
            SELECT COALESCE(candidate_id, ''), COALESCE(link, '')
            FROM intel_feed
        """).fetchall()
    candidate_ids = {candidate_id for candidate_id, _link in rows if candidate_id}
    links = {canonicalize_url(link) for _candidate_id, link in rows if link}
    return candidate_ids, links


def select_candidates_mechanically(
    items: list[dict[str, Any]],
    sample_per_source: int,
    archived_candidate_ids: set[str],
    archived_links: set[str],
    refresh_existing: bool = False,
) -> list[dict[str, Any]]:
    if not items:
        return []

    if refresh_existing:
        eligible = items
        already_archived: list[dict[str, Any]] = []
    else:
        eligible = []
        already_archived = []
        for c in items:
            if c["id"] in archived_candidate_ids or canonicalize_url(c["url"]) in archived_links:
                already_archived.append(c)
            else:
                eligible.append(c)

    for c in already_archived:
        mark_candidate(c["id"], False, "skipped_already_archived", "present in intel_feed")

    k = sample_per_source if sample_per_source > 0 else len(eligible)
    selected = eligible[:k]
    for c in selected:
        mark_candidate(c["id"], True, "selected_mechanical", "latest unarchived feed item")

    selected_ids = {c["id"] for c in selected}
    for c in eligible:
        if c["id"] not in selected_ids:
            mark_candidate(c["id"], False, "skipped_mechanical", "outside sample threshold")

    source = items[0]["source"]
    log(
        f"  selected {len(selected)}/{len(eligible)} unarchived from {source}; "
        f"skipped {len(already_archived)} already archived"
    )
    return selected


# -----------------------------------------------------------------------------
# Processed Link URL Diagnostic Tree Creation
# -----------------------------------------------------------------------------
def generate_processed_links_tree() -> None:
    """Extracts all ingested article URLs and maps them into a diagnostic JSON node tree for LLM analysis."""
    with db_session() as conn:
        rows = conn.execute("SELECT link FROM intel_feed WHERE link IS NOT NULL AND link != ''").fetchall()
        
    tree: dict[str, Any] = {}
    for (link,) in rows:
        parsed = urllib.parse.urlparse(link)
        host = parsed.netloc.lower()
        if not host:
            continue
            
        parts = [p for p in parsed.path.split("/") if p]
        
        curr = tree.setdefault(host, {})
        for part in parts:
            curr = curr.setdefault(part, {})
            
    PROCESSED_LINKS_JSON.write_text(json.dumps(tree, indent=2, ensure_ascii=False), encoding="utf-8")
    log(f"Processed links diagnostic tree written: {PROCESSED_LINKS_JSON}")


# -----------------------------------------------------------------------------
# RSS 2.0 feed and dynamic Cloudflare Pages endpoint
# -----------------------------------------------------------------------------
RSS_CAUCUS_MAP = {
    "Groundwork": ["Building Up (Groundwork)", "Power Map Mag (Groundwork)"],
    "Caracol": ["Caracol"],
    "Communist Caucus": ["Communist Caucus Bulletin", "Spadework"],
    "Emerge": ["Emerge", "Partisan Magazine"],
    "Libertarian Socialist Caucus": ["LSC Pamphlets", "LSC Statements"],
    "Liberation": ["Liberation"],
    "Marxist Unity Group": ["Marxist Unity Group", "Light & Air (MUG)"],
    "North Star": ["North Star Caucus Blog"],
    "Red Star": ["Zenith (Red Star)", "Red Star Newsletter"],
    "21st Century Socialism": ["21st Century Socialism"],
    "Reform and Revolution": ["Reform & Revolution"],
    "Bread and Roses": ["Socialist Call (Bread & Roses)"],
    "Socialist Majority Caucus": ["The Agitator (SMC)"],
}
RSS_SOURCE_TO_CAUCUS = {
    source: caucus for caucus, sources in RSS_CAUCUS_MAP.items() for source in sources
}


def _rss_rfc822(value: str) -> str:
    from email.utils import format_datetime
    dt = parse_date_robust(value) or datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return format_datetime(dt.astimezone(timezone.utc))


def _append_rss_item(channel: ET.Element, article: dict[str, Any]) -> None:
    item = ET.SubElement(channel, "item")
    ET.SubElement(item, "title").text = article.get("title") or "Untitled"
    ET.SubElement(item, "link").text = article.get("link") or ""
    guid = ET.SubElement(item, "guid", {"isPermaLink": "false"})
    guid.text = f"caucus-commons:{article.get('id', '')}"
    ET.SubElement(item, "pubDate").text = _rss_rfc822(article.get("pub_date") or article.get("date_fetched") or "")
    if article.get("author") and article["author"].lower() != "author unknown":
        ET.SubElement(item, "author").text = article["author"]
    ET.SubElement(item, "category").text = article.get("caucus") or "Caucus Commons"
    ET.SubElement(item, "category").text = article.get("source") or "Publication"
    ET.SubElement(item, "description").text = article.get("description") or ""


def _build_static_rss(articles: list[dict[str, Any]]) -> bytes:
    rss = ET.Element("rss", {"version": "2.0"})
    channel = ET.SubElement(rss, "channel")
    ET.SubElement(channel, "title").text = "Caucus Commons"
    ET.SubElement(channel, "link").text = "https://onquarryrd.pages.dev/caucuscommons.html"
    ET.SubElement(channel, "description").text = "Writings and updates collected by Caucus Commons."
    ET.SubElement(channel, "language").text = "en-us"
    ET.SubElement(channel, "lastBuildDate").text = _rss_rfc822(now_iso())
    ET.SubElement(channel, "generator").text = "Caucus Commons / DSA Reader"
    for article in articles:
        _append_rss_item(channel, article)
    return ET.tostring(rss, encoding="utf-8", xml_declaration=True)


def generate_rss_feed() -> None:
    """Write the default feed, filter data, and the /rss Pages Worker endpoint."""
    with db_session() as conn:
        rows = conn.execute("""
            SELECT id, source, title, link, pub_date, author, description,
                   date_fetched, article_text
            FROM intel_feed
            WHERE COALESCE(metadata_status, '') != 'skipped_short_article'
              AND COALESCE(link, '') != ''
            ORDER BY COALESCE(pub_date, date_fetched) DESC
        """).fetchall()
    articles = []
    for article_id, source, title, link, pub_date, author, description, date_fetched, article_text in rows:
        caucus = RSS_SOURCE_TO_CAUCUS.get(source)
        if not caucus:
            continue
        articles.append({
            "id": article_id or stable_id(source or "", link or "", title or ""),
            "source": source or "", "caucus": caucus, "title": title or "Untitled",
            "link": link or "", "pub_date": pub_date or "", "author": author or "",
            "description": description or "", "date_fetched": date_fetched or "",
            "text": article_text or "",
        })
    RSS_FILE.write_bytes(_build_static_rss(articles))
    RSS_DATA_FILE.write_text(json.dumps({"articles": articles}, ensure_ascii=False), encoding="utf-8")
    PAGES_WORKER_FILE.write_text(r"""const esc = value => String(value ?? "").replace(/[&<>"']/g, ch => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&apos;"}[ch]));
const tokens = query => {
  const include = [], exclude = [];
  const pattern = /(-?)"([^"]+)"|(-?)([^ "]+)/g;
  let match;
  while ((match = pattern.exec(query || "")) !== null) {
    const negative = (match[1] || match[3]) === "-";
    const value = (match[2] || match[4] || "").trim().toLowerCase();
    if (value && value !== "-") (negative ? exclude : include).push(value);
  }
  return {include, exclude};
};
const matches = (article, query) => {
  const parsed = tokens(query);
  const haystack = [article.text, article.title, article.author, article.description, article.source, article.caucus].join("\n").toLowerCase();
  return parsed.include.every(term => haystack.includes(term)) && parsed.exclude.every(term => !haystack.includes(term));
};
const rfc822 = value => new Date(value || Date.now()).toUTCString();
export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (url.pathname !== "/rss" && url.pathname !== "/rss/") return env.ASSETS.fetch(request);
    const dataResponse = await env.ASSETS.fetch(new URL("/caucuscommons-feed-data.json", url));
    if (!dataResponse.ok) return new Response("Feed data unavailable", {status: 503});
    const {articles} = await dataResponse.json();
    const caucuses = new Set(url.searchParams.getAll("caucus"));
    const publications = new Set(url.searchParams.getAll("publication"));
    const query = url.searchParams.get("q") || "";
    const filtered = articles.filter(article =>
      (!caucuses.size || caucuses.has(article.caucus)) &&
      (!publications.size || publications.has(article.source)) &&
      (!query || matches(article, query))
    );
    const label = [caucuses.size ? [...caucuses].join(", ") : "All caucuses", publications.size ? [...publications].join(", ") : "All publications", query ? `Search: ${query}` : ""].filter(Boolean).join(" | ");
    const items = filtered.map(a => `<item><title>${esc(a.title)}</title><link>${esc(a.link)}</link><guid isPermaLink="false">caucus-commons:${esc(a.id)}</guid><pubDate>${esc(rfc822(a.pub_date || a.date_fetched))}</pubDate><author>${esc(a.author)}</author><category>${esc(a.caucus)}</category><category>${esc(a.source)}</category><description>${esc(a.description)}</description></item>`).join("");
    const xml = `<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>Caucus Commons — ${esc(label)}</title><link>${esc(new URL("/caucuscommons.html", url).href)}</link><description>Customized Caucus Commons feed.</description><language>en-us</language><lastBuildDate>${new Date().toUTCString()}</lastBuildDate>${items}</channel></rss>`;
    return new Response(xml, {headers: {"Content-Type":"application/rss+xml; charset=utf-8", "Cache-Control":"public, max-age=300"}});
  }
};
""", encoding="utf-8")
    log(f"RSS feed written: {RSS_FILE}")
    log(f"Configurable RSS endpoint assets written: {RSS_DATA_FILE}, {PAGES_WORKER_FILE}")


# -----------------------------------------------------------------------------
# Dynamic Dashboard Generation (Sort & Filter Integrated)
# -----------------------------------------------------------------------------



def generate_extraction_diagnostics() -> None:
    """Write a compact JSON diagnostic table for extraction quality review."""
    with db_session() as conn:
        rows = conn.execute("""
            SELECT source, title, link, pub_date, content_type, local_source_path,
                   local_text_path, metadata_status, article_text_method,
                   COALESCE(article_classification, ''), length(COALESCE(article_text, ''))
            FROM intel_feed
            ORDER BY source, title
        """).fetchall()

    diagnostics = []
    for source, title, link, pub_date, content_type, local_source, local_text, status, method, classification, text_len in rows:
        diagnostics.append({
            "source": source or "",
            "title": title or "",
            "url": link or "",
            "host": normalized_url_host(link or ""),
            "pub_date": pub_date or "",
            "content_type": content_type or "",
            "local_source_path": local_source or "",
            "local_text_path": local_text or "",
            "metadata_status": status or "",
            "article_text_method": method or "",
            "article_classification": classification or "",
            "text_length": int(text_len or 0),
        })

    EXTRACTION_DIAGNOSTICS_JSON.write_text(json.dumps(diagnostics, indent=2, ensure_ascii=False), encoding="utf-8")
    log(f"Extraction diagnostics written: {EXTRACTION_DIAGNOSTICS_JSON}")


def generate_dashboard() -> None:
    with db_session() as conn:
        rows = conn.execute("""
            SELECT source, title, link, pub_date, author, description, content_type, image_url, local_source_path, local_text_path, metadata_status, date_fetched, article_classification, article_text
            FROM intel_feed
            WHERE COALESCE(metadata_status, '') != 'skipped_short_article'
            ORDER BY COALESCE(pub_date, date_fetched) DESC, source
        """).fetchall()
        selected = conn.execute("SELECT COUNT(*) FROM candidates WHERE selected=1").fetchone()[0]
        total = conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0]

    CAUCUS_MAP = {
        "North Star": {"sources": ["North Star Caucus Blog"], "color": "rgb(254, 254, 254)", "text": "black"},
        "Socialist Majority Caucus": {"sources": ["The Agitator (SMC)"], "color": "rgb(221, 84, 78)", "text": "white"},
        "Groundwork": {"sources": ["Building Up (Groundwork)", "Power Map Mag (Groundwork)"], "color": "rgb(79, 138, 54)", "text": "white"},
        "Bread and Roses": {"sources": ["Socialist Call (Bread & Roses)"], "color": "rgb(202, 74, 64)", "text": "white"},
        "Caracol": {"sources": ["Caracol"], "color": "rgb(249, 220, 117)", "text": "black"},
        "Communist Caucus": {"sources": ["Communist Caucus Bulletin", "Spadework"], "color": "rgb(229, 139, 135)", "text": "black"},
        "Libertarian Socialist Caucus": {"sources": ["LSC Pamphlets", "LSC Statements"], "color": "rgb(6, 8, 7)", "text": "white"},
        "Emerge": {"sources": ["Emerge", "Partisan Magazine"], "color": "rgb(222, 112, 122)", "text": "white"},
        "Marxist Unity Group": {"sources": ["Marxist Unity Group", "Light & Air (MUG)"], "color": "rgb(117, 139, 245)", "text": "white"},
        "Reform and Revolution": {"sources": ["Reform & Revolution"], "color": "rgb(111, 51, 64)", "text": "white"},
        "Red Star": {"sources": ["Zenith (Red Star)", "Red Star Newsletter"], "color": "rgb(236, 97, 92)", "text": "white"},
        "21st Century Socialism": {"sources": ["21st Century Socialism"], "color": "#ffcd00", "text": "black"},
        "Liberation": {"sources": ["Liberation"], "color": "rgb(217, 57, 51)", "text": "white"}
    }
    
    SOURCE_TO_CAUCUS = {}
    for caucus, data in CAUCUS_MAP.items():
        for s in data["sources"]:
            SOURCE_TO_CAUCUS[s] = caucus

    import statistics
    from datetime import timedelta

    articles = []
    for row in rows:
        source, title, link, pub_date, author, desc, ctype, image_url, local_source, local_text, status, date_fetched, classification, text = row
        iso_date = parse_to_iso(pub_date or date_fetched)
        dt_obj = parse_date_robust(pub_date or date_fetched) or datetime.now(timezone.utc)
        # Normalize mixed parsed dates before sorting: parse_date_robust() can
        # return offset-aware datetimes for ISO/RFC dates and offset-naive
        # datetimes for strptime()-parsed dates.
        if dt_obj.tzinfo is None:
            dt_obj = dt_obj.replace(tzinfo=timezone.utc)
        else:
            dt_obj = dt_obj.astimezone(timezone.utc)
        caucus = SOURCE_TO_CAUCUS.get(source, "Unknown")
        if caucus == "Unknown":
            continue
        
        articles.append({
            "source": source or "",
            "caucus": caucus,
            "title": title or "Untitled",
            "link": link or "",
            "iso_date": iso_date,
            "display_date": format_display_date(pub_date or date_fetched),
            "author": author or "",
            "desc": desc or "",
            "ctype": ctype or "article",
            "local_source": local_source or "",
            "local_text": local_text or "",
            "status": status or "",
            "classification": classification or "",
            "text": text or "",
            "dt": dt_obj,
            "dt_sec": dt_obj.timestamp()
        })

    full_articles = [a for a in articles if a["classification"] == "article_full"]
    full_articles.sort(key=lambda x: x["dt"])
    
    periods = []
    if full_articles:
        min_dt = full_articles[0]["dt"]
        max_dt = full_articles[-1]["dt"]
        N_PERIODS = 4
        best_boundaries = None
        best_score = float('inf')
        
        total_duration = (max_dt - min_dt).total_seconds()
        if total_duration > 0 and len(full_articles) >= N_PERIODS:
            time_b = [min_dt + timedelta(seconds=total_duration * i / N_PERIODS) for i in range(1, N_PERIODS)]
            count_b = [full_articles[int(len(full_articles) * i / N_PERIODS)]["dt"] for i in range(1, N_PERIODS)]
            
            for alpha in [i/10.0 for i in range(11)]:
                test_b = [time_b[i] + (count_b[i] - time_b[i]) * alpha for i in range(N_PERIODS-1)]
                bounds = [min_dt] + test_b + [max_dt]
                lengths = [(bounds[i+1] - bounds[i]).total_seconds() for i in range(N_PERIODS)]
                counts = [sum(1 for a in full_articles if bounds[i] <= a["dt"] <= bounds[i+1]) for i in range(N_PERIODS)]
                
                mean_l = sum(lengths)/N_PERIODS
                std_l = statistics.stdev(lengths) if N_PERIODS > 1 else 0
                norm_std_l = std_l / mean_l if mean_l > 0 else 0
                
                mean_c = sum(counts)/N_PERIODS
                std_c = statistics.stdev(counts) if N_PERIODS > 1 else 0
                norm_std_c = std_c / mean_c if mean_c > 0 else 0
                
                score = norm_std_l + norm_std_c
                if score < best_score:
                    best_score = score
                    best_boundaries = bounds
            periods = best_boundaries
        else:
            periods = [min_dt, max_dt]

    def clean_dashboard_blurb(value: str) -> str:
        """Remove recurring Reform & Revolution boilerplate from dashboard blurbs."""
        return normalize_space((value or "").replace("A Marxist Caucus in the Democratic Socialists of America ", ""))

    js_articles = []
    for a in sorted(articles, key=lambda x: x["dt"], reverse=True):
        js_articles.append({
            "source": a["source"],
            "caucus": a["caucus"],
            "title": a["title"],
            "link": a["link"],
            "display_date": a["display_date"],
            "author": a["author"],
            "desc": clean_dashboard_blurb(a["desc"]),
            "local_source": a["local_source"],
            "local_text": a["local_text"],
            "text": clean_dashboard_blurb(a["text"]),
            "dt_sec": a["dt_sec"]
        })
        
    js_periods = []
    if periods and len(periods) > 1:
        for i in range(len(periods)-1):
            js_periods.append({
                "start": periods[i].timestamp() - 1,
                "end": periods[i+1].timestamp() + 1,
                "label": f"{periods[i].strftime('%b %Y')} - {periods[i+1].strftime('%b %Y')}"
            })
    js_periods.reverse()
    
    articles_json = json.dumps(js_articles).replace("</", "<\\/")
    periods_json = json.dumps(js_periods).replace("</", "<\\/")
    caucus_json = json.dumps(CAUCUS_MAP).replace("</", "<\\/")

    doc = f"""<!doctype html>
<html>
<head>
<meta charset='utf-8'>
<meta name='viewport' content='width=device-width, initial-scale=1'>
<link rel='icon' href='assets/caucuscommons.ico' sizes='any'>
<title>Caucus Commons</title>
<style>
/* MIA-derived, intentionally plain document styling. */
html {{ margin: 0; padding: 0; }}
body {{
  margin: 0;
  padding: 0;
  background: #660000;
  color: #000000;
  font-family: Arial, Helvetica, sans-serif;
}}
a:link {{ color: #990000; }}
a:visited {{ color: #993333; }}
a:hover {{ text-decoration: underline; }}
p.title {{
  color: #ffcc00;
  font-family: Arial, Helvetica, sans-serif;
  font-size: 9pt;
  font-weight: bold;
  line-height: 130%;
  margin: 1%;
  text-align: left;
  text-indent: 0;
}}
.title-bar {{
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  gap: 1em;
}}
.title-bar p.title:last-child {{ text-align: right; }}
.title-actions {{ margin: 1%; text-align: right; }}
.title-actions p.title {{ margin: 0; text-align: right; }}
.rss-link-button {{ background: none; border: 0; color: #ffcc00; cursor: pointer; font: inherit; font-weight: bold; padding: 0; text-decoration: underline; }}
.rss-modal {{ display: none; position: fixed; inset: 0; z-index: 1000; background: rgba(0,0,0,.6); padding: 5vh 1rem; box-sizing: border-box; }}
.rss-modal.open {{ display: block; }}
.rss-dialog {{ max-width: 680px; margin: 0 auto; background: white; border: .5em solid #ffcc33; padding: 1rem; color: #000; }}
.rss-dialog h2 {{ margin-top: 0; color: #330000; }}
.rss-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }}
.rss-field {{ display: flex; flex-direction: column; gap: .3rem; margin-bottom: .8rem; }}
.rss-field select {{ min-height: 10rem; }}
.rss-field input, .rss-field select {{ box-sizing: border-box; width: 100%; padding: .35rem; }}
.rss-actions {{ display: flex; flex-wrap: wrap; gap: .5rem; margin-top: 1rem; }}
.rss-url {{ display: block; overflow-wrap: anywhere; margin-top: .8rem; font-size: 9pt; }}
blockquote {{
  margin-left: 5%;
  margin-right: 5%;
}}
div.border {{
  width: auto;
  padding: 1em;
  background: #ffffff;
  border: 0.5em solid #ffcc33;
}}
h1 {{
  margin: 0.25em 6% 0.35em 6%;
  color: #330000;
  font-family: "Hoefler Text", "Century Schoolbook", Georgia, serif;
  font-size: 42pt;
  font-weight: bold;
  line-height: 95%;
  text-align: center;
}}
p {{
  margin-left: 6%;
  margin-right: 6%;
  font-family: Arial, Helvetica, sans-serif;
  font-size: 12pt;
  line-height: 150%;
}}
p.fst {{ text-indent: 0; text-align: justify; font-family: "Hoefler Text", "Century Schoolbook", Georgia, serif; }}
p.toc {{ line-height: 160%; text-indent: 0; }}
hr {{
  width: 88%;
  border: 0;
  height: 1px;
  background: #500000;
}}
.controls-wrapper {{
  margin: 1em 6% 1.25em 6%;
  padding: 0.75em 0;
  border-bottom: 1px solid #500000;
}}
.controls-row {{
  display: flex;
  flex-wrap: wrap;
  gap: 0.75em;
  margin-top: 0.75em;
}}
.control-group {{
  display: flex;
  flex-direction: column;
  gap: 0.25em;
  flex: 1 1 12em;
}}
.control-group label {{
  color: #330000;
  font-family: Arial, Helvetica, sans-serif;
  font-size: 9pt;
  font-weight: bold;
}}
.control-group input {{
  box-sizing: border-box;
  width: 100%;
  padding: 0.3em 0.4em;
  border: 1px solid #999999;
  background: #ffffff;
  color: #000000;
  font-family: Arial, Helvetica, sans-serif;
  font-size: 10pt;
}}
.control-group input:focus {{
  outline: 1px solid #990000;
}}
.caucus-bubbles {{
  display: block;
  line-height: 210%;
}}
.bubble {{
  display: inline-block;
  margin: 0 0.35em 0.35em 0;
  padding: 0.12em 0.45em;
  border: 1px solid #333333;
  cursor: pointer;
  font-family: Arial, Helvetica, sans-serif;
  font-size: 9pt;
  font-weight: bold;
  line-height: 150%;
  user-select: none;
}}
.bubble.active {{ outline: 2px solid #000000; }}
.bubble.inactive {{ opacity: 0.35; outline: none; }}
#articles-container {{
  margin: 0 6% 1em 6%;
}}
.line-item {{
  padding: 0.75em 0;
  border-top: 1px dotted #999999;
  background: transparent;
}}
.line-item-header {{
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  gap: 1em;
  flex-wrap: wrap;
}}
.line-item-title {{
  margin: 0;
  color: #330000;
  font-family: Arial, Helvetica, sans-serif;
  font-size: 15pt;
  font-weight: bold;
  line-height: 130%;
}}
.caucus-label {{
  border: 1px solid #333333;
  padding: 0.1em 0.4em;
  font-family: Arial, Helvetica, sans-serif;
  font-size: 8pt;
  font-weight: bold;
  white-space: nowrap;
}}
.line-item-meta {{
  margin: 0.3em 0;
  color: #666666;
  font-family: Arial, Helvetica, sans-serif;
  font-size: 9pt;
  font-style: normal;
}}
.line-item-desc {{
  margin: 0.35em 0 0.45em 0;
  color: #000000;
  font-size: 11pt;
  line-height: 145%;
  text-align: justify;
}}
.period-group {{
  margin-bottom: 1em;
  border: 1px solid #ffcc33;
  background: #ffffff;
}}
.period-header {{
  padding: 0.45em 0.65em;
  background: #eeeeee;
  color: #330000;
  cursor: pointer;
  display: flex;
  justify-content: space-between;
  gap: 1em;
  font-family: Arial, Helvetica, sans-serif;
  font-size: 10pt;
  font-weight: bold;
  user-select: none;
}}
.period-header:hover {{ background: #dddddd; }}
.period-content {{
  display: none;
  padding: 0 0.75em 0.5em 0.75em;
}}
.period-content.open {{ display: block; }}
.period-content .line-item:first-child {{ border-top: 0; }}
@media (max-width: 720px) {{
  blockquote {{ margin-left: 2%; margin-right: 2%; }}
  div.border {{ padding: 0.75em; border-width: 0.35em; }}
  h1 {{ font-size: 30pt; }}
  p, .controls-wrapper, #articles-container {{ margin-left: 3%; margin-right: 3%; }}
  .line-item-header {{ display: block; }}
  .caucus-label {{ display: inline-block; margin-top: 0.35em; }}
}}
</style>
</head>
<body>
<div class="title-bar">
<p class="title">Contact: <a href="mailto:upturn_sassy8s@icloud.com">upturn_sassy8s@icloud.com</a> 2026</p>
<div class="title-actions">
<p class="title">{html.escape(RUN_STARTED_AT.strftime("%B %d, %Y %I:%M:%S %p %Z"))}</p>
<p class="title"><button type="button" class="rss-link-button" id="rss-open">RSS Feed</button></p>
</div>
</div>
<div class="rss-modal" id="rss-modal" role="dialog" aria-modal="true" aria-labelledby="rss-heading">
<div class="rss-dialog">
<h2 id="rss-heading">Configure RSS Feed</h2>
<p>Select zero or more values. No selection means all. Hold Command on macOS or Control on Windows to select several options.</p>
<div class="rss-grid">
<div class="rss-field"><label for="rss-caucuses">Caucuses</label><select id="rss-caucuses" multiple></select></div>
<div class="rss-field"><label for="rss-publications">Publications</label><select id="rss-publications" multiple></select></div>
</div>
<div class="rss-field"><label for="rss-keywords">Keyword search</label><input id="rss-keywords" type="text" placeholder='Words, &quot;exact phrase&quot;, or -excluded'></div>
<div class="rss-actions"><button type="button" id="rss-open-feed">Open Feed</button><button type="button" id="rss-copy">Copy Feed URL</button><button type="button" id="rss-default">Default Feed</button><button type="button" id="rss-close">Close</button></div>
<a class="rss-url" id="rss-url" href="caucuscommons.xml">caucuscommons.xml</a>
</div>
</div>
<blockquote>
<div class="border">
<h1>Caucus Commons</h1>
<p class="fst">Caucus Commons is an aggregator for various publications from caucuses in the <a href="https://act.dsausa.org/donate/membership/">Democratic Socialists of America</a>.</p>
<hr />

<div class="controls-wrapper">
    <div class="caucus-bubbles" id="bubbles-container"></div>
    <div class="controls-row">
        <div class="control-group">
            <label>Date Start:</label> <input type="date" id="date-start">
        </div>
        <div class="control-group">
            <label>Date End:</label> <input type="date" id="date-end">
        </div>
        <div class="control-group search-group" style="flex: 2 1 300px;">
            <label>Search Full Text:</label> <input type="text" id="search-box" placeholder='Words, &quot;exact phrase&quot;, or -excluded'>
        </div>
    </div>
</div>

<div id="articles-container"></div>
</div>
</blockquote>
<script>
const CAUCUS_MAP = {caucus_json};
const PERIODS = {periods_json};
const ARTICLES = {articles_json};

document.addEventListener("DOMContentLoaded", () => {{
    const rssModal = document.getElementById("rss-modal");
    const rssCaucuses = document.getElementById("rss-caucuses");
    const rssPublications = document.getElementById("rss-publications");
    const rssKeywords = document.getElementById("rss-keywords");
    const rssUrl = document.getElementById("rss-url");
    Object.keys(CAUCUS_MAP).forEach(caucus => rssCaucuses.add(new Option(caucus, caucus)));
    const selectedValues = select => new Set(Array.from(select.selectedOptions, option => option.value));
    function updateRssPublications() {{
        const chosen = selectedValues(rssCaucuses);
        const previous = selectedValues(rssPublications);
        const available = Object.entries(CAUCUS_MAP).filter(([caucus]) => !chosen.size || chosen.has(caucus)).flatMap(([, data]) => data.sources).sort();
        rssPublications.innerHTML = "";
        available.forEach(source => {{ const option = new Option(source, source); option.selected = previous.has(source); rssPublications.add(option); }});
        updateRssUrl();
    }}
    function updateRssUrl() {{
        const url = new URL("/rss", window.location.origin);
        selectedValues(rssCaucuses).forEach(value => url.searchParams.append("caucus", value));
        selectedValues(rssPublications).forEach(value => url.searchParams.append("publication", value));
        if (rssKeywords.value.trim()) url.searchParams.set("q", rssKeywords.value.trim());
        rssUrl.href = url.href; rssUrl.textContent = url.href;
    }}
    document.getElementById("rss-open").onclick = () => {{ rssModal.classList.add("open"); updateRssPublications(); }};
    document.getElementById("rss-close").onclick = () => rssModal.classList.remove("open");
    document.getElementById("rss-open-feed").onclick = () => window.open(rssUrl.href, "_blank", "noopener");
    document.getElementById("rss-copy").onclick = async () => {{ await navigator.clipboard.writeText(rssUrl.href); }};
    document.getElementById("rss-default").onclick = () => window.open("caucuscommons.xml", "_blank", "noopener");
    rssCaucuses.addEventListener("change", updateRssPublications);
    rssPublications.addEventListener("change", updateRssUrl);
    rssKeywords.addEventListener("input", updateRssUrl);
    rssModal.addEventListener("click", event => {{ if (event.target === rssModal) rssModal.classList.remove("open"); }});
    const bCont = document.getElementById("bubbles-container");
    const searchBox = document.getElementById("search-box");
    const dateStart = document.getElementById("date-start");
    const dateEnd = document.getElementById("date-end");
    const container = document.getElementById("articles-container");
    
    let selectedCaucuses = new Set();

    Object.keys(CAUCUS_MAP).forEach(c => {{
        const b = document.createElement("div");
        b.className = "bubble active";
        b.style.backgroundColor = CAUCUS_MAP[c].color;
        b.style.color = CAUCUS_MAP[c].text;
        if (c === "North Star") b.style.border = "1px solid #ccc"; 
        b.innerText = c;
        b.onclick = () => {{
            if (selectedCaucuses.has(c)) {{
                selectedCaucuses.delete(c);
            }} else {{
                selectedCaucuses.add(c);
            }}
            updateBubblesVisuals();
            render();
        }};
        bCont.appendChild(b);
    }});

    function updateBubblesVisuals() {{
        Array.from(bCont.children).forEach(child => {{
            const c = child.innerText;
            if (selectedCaucuses.size === 0 || selectedCaucuses.has(c)) {{
                child.classList.replace("inactive", "active");
                if (!child.classList.contains("active")) child.classList.add("active");
            }} else {{
                child.classList.replace("active", "inactive");
                if (!child.classList.contains("inactive")) child.classList.add("inactive");
            }}
        }});
    }}

    function createLineItem(a) {{
        const div = document.createElement("div");
        div.className = "line-item";
        const cColor = CAUCUS_MAP[a.caucus] ? CAUCUS_MAP[a.caucus].color : "#999";
        const cText = CAUCUS_MAP[a.caucus] ? CAUCUS_MAP[a.caucus].text : "#fff";
        div.innerHTML = `
            <div class="line-item-header">
                <h3 class="line-item-title"><a href="${{a.link}}" target="_blank">${{a.title}}</a></h3>
                <span class="caucus-label" style="background:${{cColor}}; color:${{cText}}">${{a.caucus}}</span>
            </div>
            <div class="line-item-meta">${{a.source}} · ${{a.display_date}} · ${{a.author}}</div>
            <div class="line-item-desc">${{a.desc}}</div>
        `;
        return div;
    }}

    function parseSearchQuery(query) {{
        const include = [];
        const exclude = [];
        // Supports words, "exact phrases", -words, and -"excluded phrases".
        const tokenPattern = /(-?)"([^"]+)"|(-?)([^ "]+)/g;
        let match;
        while ((match = tokenPattern.exec(query)) !== null) {{
            const negative = (match[1] || match[3]) === "-";
            const value = (match[2] || match[4] || "").trim().toLowerCase();
            if (!value || value === "-") continue;
            (negative ? exclude : include).push(value);
        }}
        return {{ include, exclude }};
    }}

    function articleMatchesSearch(a, parsedQuery) {{
        // Search remains independent of the removed display links. Full archived
        // article text, title, author, description, publication, and caucus remain indexed.
        const haystack = [
            a.text, a.title, a.author, a.desc, a.source, a.caucus
        ].map(value => value || "").join("\\n").toLowerCase();
        return parsedQuery.include.every(term => haystack.includes(term)) &&
               parsedQuery.exclude.every(term => !haystack.includes(term));
    }}

    function render() {{
        const searchVal = searchBox.value.trim();
        const parsedQuery = parseSearchQuery(searchVal);
        const startVal = dateStart.value ? new Date(dateStart.value).getTime() / 1000 : null;
        const endVal = dateEnd.value ? new Date(dateEnd.value).getTime() / 1000 + 86400 : null;
        
        let filtered = ARTICLES.filter(a => {{
            if (selectedCaucuses.size > 0 && !selectedCaucuses.has(a.caucus)) return false;
            if (startVal && a.dt_sec < startVal) return false;
            if (endVal && a.dt_sec > endVal) return false;
            if (searchVal && !articleMatchesSearch(a, parsedQuery)) return false;
            return true;
        }});

        container.innerHTML = "";

        if (selectedCaucuses.size > 0 || searchVal || startVal || endVal) {{
            filtered.forEach(a => container.appendChild(createLineItem(a)));
            if (filtered.length === 0) container.innerHTML = "<p>No articles found matching the selected criteria.</p>";
        }} else {{
            PERIODS.forEach((p, i) => {{
                const pArts = filtered.filter(a => a.dt_sec >= p.start && a.dt_sec <= p.end);
                if (pArts.length === 0) return;
                
                const gDiv = document.createElement("div");
                gDiv.className = "period-group";
                
                const hdr = document.createElement("div");
                hdr.className = "period-header";
                
                const content = document.createElement("div");
                content.className = "period-content";
                
                const updateHdr = () => {{
                    const isOpen = content.classList.contains("open");
                    hdr.innerHTML = `<span>${{p.label}}</span> <span>${{pArts.length}} items ${{isOpen ? "▲" : "▼"}}</span>`;
                }};
                
                hdr.onclick = () => {{
                    content.classList.toggle("open");
                    updateHdr();
                }};
                
                if (i === 0) content.classList.add("open");
                updateHdr();
                
                pArts.forEach(a => content.appendChild(createLineItem(a)));
                
                gDiv.appendChild(hdr);
                gDiv.appendChild(content);
                container.appendChild(gDiv);
            }});
            
            const outsiders = filtered.filter(a => !PERIODS.some(p => a.dt_sec >= p.start && a.dt_sec <= p.end));
            if (outsiders.length > 0) {{
                const outDiv = document.createElement("div");
                outDiv.className = "period-group";
                const hdr = document.createElement("div");
                hdr.className = "period-header";
                const content = document.createElement("div");
                content.className = "period-content";
                
                hdr.onclick = () => {{
                    content.classList.toggle("open");
                    hdr.innerHTML = `<span>Other Dates</span> <span>${{outsiders.length}} items ${{content.classList.contains("open") ? "▲" : "▼"}}</span>`;
                }};
                hdr.innerHTML = `<span>Other Dates</span> <span>${{outsiders.length}} items ▼</span>`;
                
                outsiders.forEach(a => content.appendChild(createLineItem(a)));
                outDiv.appendChild(hdr);
                outDiv.appendChild(content);
                container.appendChild(outDiv);
            }}
        }}
    }}

    searchBox.addEventListener("input", render);
    dateStart.addEventListener("change", render);
    dateEnd.addEventListener("change", render);

    updateBubblesVisuals();
    render();
}});
</script>
</body>
</html>"""
    doc = doc.replace("A Marxist Caucus in the Democratic Socialists of America ", "")
    DASHBOARD_FILE.write_text(doc, encoding="utf-8")
    log(f"Dashboard written: {DASHBOARD_FILE}")


# -----------------------------------------------------------------------------
# Core Loop
# -----------------------------------------------------------------------------
def run(args: argparse.Namespace) -> None:
    init_db(reset=args.reset_db)

    if args.detected_schema and not args.source and not args.resolved_host:
        log("[!] --detected-schema without --source or --resolved-host will fetch broadly and skip after fetch")

    catalog = discover_all(args.source, args.resolved_host)

    groups: dict[str, list[dict[str, Any]]] = {}
    for c in catalog:
        groups.setdefault(c["source_key"], []).append(c)

    selected: list[dict[str, Any]] = []
    archived_candidate_ids, archived_links = archived_article_keys()
    log("\nExecuting mechanical chronological selection...")
    for _source_key, items in groups.items():
        selected.extend(select_candidates_mechanically(
            items,
            args.sample_per_source,
            archived_candidate_ids,
            archived_links,
            args.refresh_existing,
        ))

    log(f"\nArchiving sources and extracting structural metadata for {len(selected)} item(s)...")
    for c in selected:
        process_candidate(c, args)

    generate_rss_feed()
    generate_dashboard()
    generate_processed_links_tree()
    generate_extraction_diagnostics()
    if not args.no_cloudflare_deploy:
        deploy_to_cloudflare_pages()
    log("Job finished successfully.")


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Deterministic DSA Source Archiver")
    p.add_argument("--reset-db", action="store_true", help="Clear tables on startup")
    p.add_argument("--source", default=None, help="Process specified source substring only")
    p.add_argument("--resolved-host", default="", help="Only discover/process article candidates whose resolved URL host matches this host, e.g. www.dsanorthstar.org")
    p.add_argument("--detected-schema", default="", help="After fetching, only archive pages whose detected platform schema matches this value, e.g. weebly, wordpress, squarespace, substack, ghost, medium")
    p.add_argument("--min-article-chars", type=int, default=0, help="Minimum extracted article text length used with --skip-short-articles")
    p.add_argument("--skip-short-articles", action="store_true", help="Mark extracted items shorter than --min-article-chars as skipped_short_article and omit them from the dashboard")
    p.add_argument("--sample-per-source", type=int, default=2, help="Max articles per feed source (0 for all)")
    p.add_argument(
        "--no-cloudflare-deploy",
        action="store_true",
        help="Generate all files but skip the Cloudflare Pages production deployment",
    )
    p.add_argument(
        "--refresh-existing",
        action="store_true",
        help="Re-fetch and overwrite articles already represented in intel_feed",
    )
    return p.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(run(parse_args(sys.argv[1:])))