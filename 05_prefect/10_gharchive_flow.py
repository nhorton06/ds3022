import gzip
import json
from collections import Counter
import requests
from prefect import flow, task


URL = "https://data.gharchive.org/2025-01-01-{hour}.json.gz"


@task(retries=3)
def count_events_for_hour(hour):
    counts = Counter()
    try:
        with requests.get(URL.format(hour=hour), stream=True, timeout=60) as r:
            r.raise_for_status()  # fail on HTTP errors so retries=3 kicks in
            # decompress the gzip stream as it downloads, one line (event) at a time
            with gzip.GzipFile(fileobj=r.raw) as gz:
                for line in gz:
                    counts[json.loads(line)["type"]] += 1
    except Exception as e:
        # report the failure, then re-raise so Prefect marks the task failed and retries
        print(f"hour {hour} failed: {e}")
        raise
    print(f"hour {hour}: {counts.total()} events")
    return dict(counts)


@task
def combine_counts(hourly_counts):
    try:
        totals = Counter()
        for hc in hourly_counts:
            totals.update(hc)  # add each hour's {event_type: count} into the running total
        # most_common() sorts by count, highest first; dict keeps that order
        ordered = dict(totals.most_common())
    except Exception as e:
        print(f"combine_counts failed: {e}")
        raise
    for event_type, count in ordered.items():
        print(f"{event_type}: {count}")
    return ordered


@flow(log_prints=True)
def gharchive_flow(hours: list[int] = list(range(1, 4))):
    # take the range and create a prefect .map over the hours
    hourly = count_events_for_hour.map(hours).result()
    totals = combine_counts(hourly)
    for k, v in totals.items():
        print(k, v)


if __name__ == "__main__":
    gharchive_flow()
