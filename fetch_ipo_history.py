from __future__ import annotations

import argparse
import csv
import json
import re
import time
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlencode, urljoin
from urllib.request import HTTPCookieProcessor, Request, build_opener

from bs4 import BeautifulSoup


SOURCE_URL = "https://www.capitalmarket.com/markets/IPOs/ipo-historic-table"
NAME_CACHE_PATH = Path("ipo_name_cache.json")
COLUMNS = [
    "list_date",
    "ipo_name",
    "issue_size_inr_crore",
    "qib_subscription",
    "nii_subscription",
    "rii_subscription",
    "total_subscription",
    "offer_price_inr",
    "list_price_inr",
    "listing_gains_pct",
    "cmp_bse_inr",
    "cmp_nse_inr",
    "current_gains_pct",
    "details_url",
]


def subtract_months(value: date, months: int) -> date:
    month_index = value.year * 12 + value.month - 1 - months
    year, zero_based_month = divmod(month_index, 12)
    month = zero_based_month + 1
    month_lengths = [
        31,
        29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
        31, 30, 31, 30, 31, 31, 30, 31, 30, 31,
    ]
    return date(year, month, min(value.day, month_lengths[month - 1]))


def parse_page(html: bytes) -> tuple[list[dict[str, str]], str | None, dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    table = next(
        (
            candidate
            for candidate in soup.find_all("table")
            if "List Date" in {header.get_text(" ", strip=True) for header in candidate.find_all("th")}
        ),
        None,
    )
    if table is None:
        raise RuntimeError("IPO history table not found; the page layout may have changed")

    rows = []
    for row in table.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) != 13:
            continue
        values = [cell.get_text(" ", strip=True) for cell in cells]
        details_link = cells[1].find("a", href=True)
        details_url = urljoin(SOURCE_URL, details_link["href"]) if details_link else ""
        rows.append(dict(zip(COLUMNS, values + [details_url])))

    pager = soup.find(id=re.compile(r"dtpgrGain$"))
    active_page = pager.find(class_="ActivePage") if pager else None
    current_page = int(active_page.get_text(strip=True)) if active_page else 1
    numbered_links = [
        link
        for link in pager.find_all("a", href=True)
        if link.get_text(strip=True).isdigit() and int(link.get_text(strip=True)) > current_page
    ] if pager else []
    next_link = min(numbered_links, key=lambda link: int(link.get_text(strip=True)), default=None)
    if next_link is None and pager:
        next_link = pager.find("a", string=lambda text: bool(text and text.strip() == "Next"))
    match = re.search(r"__doPostBack\('([^']+)'", next_link.get("href", "")) if next_link else None
    hidden_fields = {
        field["name"]: field.get("value", "")
        for field in soup.select('form input[type="hidden"][name]')
    }
    return rows, match.group(1) if match else None, hidden_fields


def fetch_full_name(row: dict[str, str]) -> str:
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            request = Request(
                row["details_url"],
                headers={"User-Agent": "Mozilla/5.0 (compatible; IPO-history-export/1.0)"},
            )
            html = build_opener().open(request, timeout=12).read()
            soup = BeautifulSoup(html, "html.parser")
            name_cell = soup.select_one("td.GridHeadL.ipo-heed")
            if name_cell is None:
                raise RuntimeError("full-name field not found")
            return name_cell.get_text(" ", strip=True)
        except Exception as error:
            last_error = error
            if attempt < 2:
                time.sleep(attempt + 1)
    raise RuntimeError(str(last_error))


def enrich_full_names(rows: list[dict[str, str]]) -> None:
    try:
        name_cache: dict[str, str] = json.loads(NAME_CACHE_PATH.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        name_cache = {}

    for row in rows:
        if row["details_url"] in name_cache:
            row["ipo_name"] = name_cache[row["details_url"]]

    truncated_rows = [row for row in rows if "..." in row["ipo_name"] and row["details_url"]]
    if not truncated_rows:
        return

    print(f"Resolving {len(truncated_rows)} truncated IPO names")
    completed = 0
    cache_lock = Lock()
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fetch_full_name, row): row for row in truncated_rows}
        for future in as_completed(futures):
            row = futures[future]
            try:
                row["ipo_name"] = future.result()
                with cache_lock:
                    name_cache[row["details_url"]] = row["ipo_name"]
                    temporary_path = NAME_CACHE_PATH.with_suffix(".tmp")
                    temporary_path.write_text(
                        json.dumps(name_cache, indent=2, sort_keys=True),
                        encoding="utf-8",
                    )
                    temporary_path.replace(NAME_CACHE_PATH)
            except Exception as error:
                print(f"Warning: kept truncated name for {row['details_url']}: {error}")
            completed += 1
            if completed % 20 == 0 or completed == len(truncated_rows):
                print(f"Resolved {completed}/{len(truncated_rows)} names")


def fetch_history(months: int) -> list[dict[str, str]]:
    opener = build_opener(HTTPCookieProcessor())
    headers = {"User-Agent": "Mozilla/5.0 (compatible; IPO-history-export/1.0)"}
    html = opener.open(Request(SOURCE_URL, headers=headers)).read()
    collected: dict[tuple[str, str], dict[str, str]] = {}
    cutoff: date | None = None

    while True:
        page_rows, event_target, hidden_fields = parse_page(html)
        if not page_rows:
            break
        parsed_dates = [datetime.strptime(row["list_date"], "%d %b %Y").date() for row in page_rows]
        cutoff = cutoff or subtract_months(max(parsed_dates), months)
        for row, list_date in zip(page_rows, parsed_dates):
            if list_date >= cutoff:
                collected[(row["list_date"], row["details_url"])] = row

        oldest_date = min(parsed_dates)
        print(f"Fetched through {oldest_date:%d %b %Y} ({len(collected)} rows)")
        if oldest_date < cutoff or not event_target:
            break

        hidden_fields["__EVENTTARGET"] = event_target
        hidden_fields["__EVENTARGUMENT"] = ""
        request = Request(
            SOURCE_URL,
            data=urlencode(hidden_fields).encode("utf-8"),
            headers={**headers, "Content-Type": "application/x-www-form-urlencoded"},
        )
        html = opener.open(request).read()

    rows = list(collected.values())
    enrich_full_names(rows)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description="Export Capital Market IPO history to CSV")
    parser.add_argument("--months", type=int, default=12, help="Months to fetch (default: 12)")
    parser.add_argument("--output", type=Path, default=Path("ipo_history.csv"))
    args = parser.parse_args()
    if args.months < 1:
        parser.error("--months must be at least 1")

    rows = fetch_history(args.months)
    with args.output.open("w", newline="", encoding="utf-8-sig") as output_file:
        writer = csv.DictWriter(output_file, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    print(f"Wrote {len(rows)} rows to {args.output}")


if __name__ == "__main__":
    main()