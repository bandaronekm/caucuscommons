#!/usr/bin/env python3

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from datetime import datetime, timezone


SCRIPT_DIR = Path(__file__).resolve().parent
CREDENTIALS_FILE = SCRIPT_DIR / "cloudflare_api_token.txt"

PROJECT_NAME = "onquarryrd"
API_ROOT = "https://api.cloudflare.com/client/v4"


def load_credentials():
    values = {}

    for raw_line in CREDENTIALS_FILE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()

        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()

    token = values.get("CLOUDFLARE_API_TOKEN", "")
    account_id = values.get("CLOUDFLARE_ACCOUNT_ID", "")

    if not token or not account_id:
        raise RuntimeError(
            f"Missing Cloudflare credentials in {CREDENTIALS_FILE}"
        )

    return token, account_id


def api_request(url, token, method="GET", retries=6):
    request = urllib.request.Request(
        url,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "Caucus-Commons-Deployment-Cleanup/1.0",
        },
    )

    for attempt in range(retries):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                body = json.loads(response.read().decode("utf-8"))

            if not body.get("success"):
                raise RuntimeError(
                    f"Cloudflare API error: {body.get('errors')}"
                )

            return body

        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")

            if exc.code == 429 or 500 <= exc.code < 600:
                delay = min(2 ** attempt, 30)
                print(
                    f"Cloudflare returned HTTP {exc.code}; "
                    f"retrying in {delay} seconds..."
                )
                time.sleep(delay)
                continue

            raise RuntimeError(
                f"Cloudflare API returned HTTP {exc.code}: {error_body}"
            ) from exc

    raise RuntimeError("Cloudflare API request failed after all retries")


def get_project(token, account_id):
    project = urllib.parse.quote(PROJECT_NAME, safe="")
    url = (
        f"{API_ROOT}/accounts/{account_id}/pages/projects/{project}"
    )
    return api_request(url, token)["result"]


def list_all_deployments(token, account_id):
    project = urllib.parse.quote(PROJECT_NAME, safe="")
    deployments = []
    page = 1
    per_page = 25

    while True:
        query = urllib.parse.urlencode(
            {"page": page, "per_page": per_page}
        )
        url = (
            f"{API_ROOT}/accounts/{account_id}/pages/projects/"
            f"{project}/deployments?{query}"
        )

        body = api_request(url, token)
        batch = body.get("result", [])

        if not batch:
            break

        deployments.extend(batch)

        result_info = body.get("result_info", {})
        total_pages = result_info.get("total_pages")

        if total_pages is not None and page >= int(total_pages):
            break

        if len(batch) < per_page:
            break

        page += 1

    return deployments


def delete_deployment(
    token,
    account_id,
    deployment_id,
    force=False,
):
    project = urllib.parse.quote(PROJECT_NAME, safe="")
    deployment = urllib.parse.quote(deployment_id, safe="")

    url = (
        f"{API_ROOT}/accounts/{account_id}/pages/projects/"
        f"{project}/deployments/{deployment}"
    )

    if force:
        url += "?force=true"

    api_request(url, token, method="DELETE")


def _deployment_local_day(deployment):
    """Return deployment creation date in the Mac's current local timezone."""
    value = str(deployment.get("created_on") or "").strip()
    if not value:
        return ""
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return ""
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone().date().isoformat()


def prune_to_daily_snapshots(force=True, delay=0.20):
    """Keep canonical production and newest production deployment per local day."""
    token, account_id = load_credentials()
    project = get_project(token, account_id)
    canonical_id = (project.get("canonical_deployment") or {}).get("id")
    if not canonical_id:
        raise RuntimeError("Cloudflare project did not report a canonical deployment")
    deployments = list_all_deployments(token, account_id)
    if not deployments:
        raise RuntimeError("Cloudflare returned no deployments; refusing cleanup")
    deployments.sort(key=lambda item: item.get("created_on", ""), reverse=True)
    protected_ids = {canonical_id}
    protected_days = set()
    for deployment in deployments:
        if deployment.get("environment") != "production":
            continue
        day = _deployment_local_day(deployment)
        deployment_id = deployment.get("id")
        if day and deployment_id and day not in protected_days:
            protected_days.add(day)
            protected_ids.add(deployment_id)
    if not protected_days:
        raise RuntimeError("No dated production deployments recognized; refusing cleanup")
    to_delete = [d for d in deployments if d.get("id") and d.get("id") not in protected_ids]
    deleted = failed = 0
    for deployment in to_delete:
        try:
            delete_deployment(token, account_id, deployment["id"], force=force)
            deleted += 1
        except Exception as exc:
            failed += 1
            print(f"Retention cleanup failed for {deployment['id']}: {exc}")
        time.sleep(max(delay, 0))
    return {
        "total": len(deployments), "kept": len(deployments) - deleted,
        "deleted": deleted, "failed": failed,
        "protected_days": len(protected_days), "canonical_id": canonical_id,
    }


def main():
    parser = argparse.ArgumentParser(
        description="Clean up old Cloudflare Pages deployments"
    )
    parser.add_argument(
        "--keep",
        type=int,
        default=20,
        help="Number of newest deployments to retain",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete deployments; otherwise perform a dry run",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Allow deletion of aliased non-production deployments",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.20,
        help="Delay between delete requests in seconds",
    )
    args = parser.parse_args()

    if args.keep < 2:
        raise RuntimeError(
            "--keep must be at least 2 so rollback history is retained"
        )

    token, account_id = load_credentials()

    project = get_project(token, account_id)
    canonical = project.get("canonical_deployment") or {}
    canonical_id = canonical.get("id")

    deployments = list_all_deployments(token, account_id)
    deployments.sort(
        key=lambda item: item.get("created_on", ""),
        reverse=True,
    )

    protected_ids = {
        item.get("id")
        for item in deployments[:args.keep]
        if item.get("id")
    }

    if canonical_id:
        protected_ids.add(canonical_id)

    to_delete = [
        item
        for item in deployments
        if item.get("id") not in protected_ids
    ]

    print(f"Project:              {PROJECT_NAME}")
    print(f"Total deployments:    {len(deployments)}")
    print(f"Newest retained:      {args.keep}")
    print(f"Canonical deployment: {canonical_id or 'unknown'}")
    print(f"Deployments to delete:{len(to_delete):>6}")
    print()

    if not args.execute:
        print("DRY RUN: nothing will be deleted.")
        print("First 20 deletion candidates:")

        for deployment in to_delete[:20]:
            print(
                "  "
                f"{deployment.get('created_on', 'unknown date')}  "
                f"{deployment.get('id', 'unknown id')}  "
                f"{deployment.get('environment', 'unknown environment')}"
            )

        print()
        print("To perform the deletion, run:")
        print(
            "python3 cleanup_cloudflare_deployments.py "
            f"--keep {args.keep} --execute --force"
        )
        return

    deleted = 0
    failed = 0

    for number, deployment in enumerate(to_delete, start=1):
        deployment_id = deployment.get("id")

        if not deployment_id:
            continue

        try:
            delete_deployment(
                token,
                account_id,
                deployment_id,
                force=args.force,
            )
            deleted += 1
            print(
                f"[{number}/{len(to_delete)}] Deleted {deployment_id}"
            )
        except Exception as exc:
            failed += 1
            print(
                f"[{number}/{len(to_delete)}] "
                f"FAILED {deployment_id}: {exc}"
            )

        time.sleep(max(args.delay, 0))

    print()
    print(f"Deleted successfully: {deleted}")
    print(f"Failed:               {failed}")
    print(f"Retained:             {len(deployments) - deleted}")


if __name__ == "__main__":
    main()
