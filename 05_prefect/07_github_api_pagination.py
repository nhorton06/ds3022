"""
Work with Claude Code (CC) to complete this prefect flow.

The best way to do this is to open CC in the root dir of this
project, and your prompt can reference @05_prefect/07_github_api_pagination.py
to ask for a plan for how to complete the steps. the instructors
inline comments already add to the prompt - i.e. to paginate over that
API endpoint for a certain number of pages, and to store the output
in a single duckdb table. Ask CC to enumerate the steps you will need
to do this, and REQUIRE it to work with you on each task, one by one.

Read what CC generates/suggests with each step, and ask yourself if you
understand the python it has generated. IF YOU DO NOT UNDERSTAND IT WELL,
this is your chance to learn a bit more python! Ask CC to explain the specific
lines that are unclear to you, and have a back and forth with CC until you
get a sense for what is going on. You know about loops, and you know about
fetching remote web content, and so the growth here is understanding how
to cycle through a series of related links to build up all 1000 records.
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
import duckdb
import httpx
from prefect import flow, task
from prefect.logging import get_run_logger
from prefect.task_runners import ThreadPoolTaskRunner


# commit history for the git/git repository
TOKEN = os.environ["GH_PAT"]
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
URL = "https://api.github.com/repos/git/git/commits"
PER_PAGE = 100
PAGES = 10

# local log file next to this script (07_github_api_pagination.log), no matter
# where it's run from. Attaching the handler to prefect's run loggers captures
# every print() in the task (log_prints=True), plus prefect's own state
# messages and full tracebacks on failure.
LOG_FILE = Path(__file__).with_suffix(".log")
file_handler = logging.FileHandler(LOG_FILE)
file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
for name in ("prefect.flow_runs", "prefect.task_runs"):
    logging.getLogger(name).addHandler(file_handler)

# ------ only one task ------
# create a single prefect task that loops through 10 pages
# of git/git commits and stores each one into a duckdb table.
# add more tasks and more data if you like, and do more with it.
# if successful you should have 1000 records in your db.

# local duckdb file, created if missing
conn = duckdb.connect("actions.duckdb")  

@task(name="ingest", log_prints=True, retries=3, retry_delay_seconds=5)
def ingest_data():
    # work with claude to fetch 100 records for each page
    # and pull 10 pages total. This could be done in series,
    # or if you want to look at previous prefect .map examples
    # you could pull some/all pages concurrently.
    #
    # for help with github api pagination see
    # https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api?apiVersion=2026-03-10
    #
    # httpx parses it into response.links. The "next" URL already includes 
    # per_page and page, so params are only needed on the first request. 
    # The loop ends when there is no "next" link (last page).

    # this task already comes with a name, retry settings, and anything
    # you print() is written to the log.
    # visit https://sds-prefect.pods.uvarc.io/v2/runs to review your flows

    # if you need to see a working example of this, see this file in this repo
    # 04_apis/05_pagination.py

    logger = get_run_logger()   # prefect's logger: goes to the UI and LOG_FILE

    # follow GitHub's "next" links until there are no more pages or we hit PAGES
    commits = []
    url = URL
    params = {"per_page": PER_PAGE}   # only needed on the first request
    page = 1
    while url and page <= PAGES:
        try:
            response = httpx.get(url, headers=HEADERS, params=params)
            response.raise_for_status()   # 4xx/5xx -> exception
        except httpx.HTTPError as e:
            # log which page broke, then re-raise so prefect marks the task
            # failed and retries it
            logger.error(f"fetch failed on page {page}: {e}")
            raise
        batch = response.json()
        commits.extend(batch)
        print(f"page {page}: {len(batch)} commits (total {len(commits)})")

        url = response.links.get("next", {}).get("url")   # None on the last page
        params = None   # the "next" url already carries per_page and page
        page += 1

    # one row per commit: a few flattened columns plus the full raw JSON.
    # sha is the primary key so re-runs (or prefect retries) replace rows
    # instead of duplicating them.
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS commits (
                sha          VARCHAR PRIMARY KEY,
                author_name  VARCHAR,
                author_email VARCHAR,
                commit_date  TIMESTAMPTZ,
                message      VARCHAR,
                raw          JSON
            )
        """)
        rows = [
            (
                c["sha"],
                c["commit"]["author"]["name"],
                c["commit"]["author"]["email"],
                datetime.fromisoformat(c["commit"]["author"]["date"]),
                c["commit"]["message"],
                json.dumps(c),
            )
            for c in commits
        ]
        conn.executemany("INSERT OR REPLACE INTO commits VALUES (?, ?, ?, ?, ?, ?)", rows)
        count = conn.execute("SELECT COUNT(*) FROM commits").fetchone()[0]
    except (KeyError, ValueError, duckdb.Error) as e:
        # KeyError: a commit missing an expected field; ValueError: bad date;
        # duckdb.Error: table/insert problem (e.g. db file locked)
        logger.error(f"storing commits failed: {e!r}")
        raise

    print(f"commits table now has {count} rows")
    return count


# name the flow something you can identify as yourself
@flow(name="dxx6gu-flow")
def main_flow():
    count = ingest_data()
    print(f"done: {count} rows in actions.duckdb")

# it doesn't seem this flow executes when you run it. Why?
# fix it ;^)
# the decorators only *define* the flow; nothing called it. Run it only when
# this file is executed directly (not when imported by another module).
if __name__ == "__main__":
    main_flow()
