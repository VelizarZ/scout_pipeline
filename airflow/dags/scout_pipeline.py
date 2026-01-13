import os
import time
import requests

from airflow import DAG
from airflow.decorators import task
from datetime import datetime

API_BASE = os.getenv("SCOUT_API_BASE_URL", "http://api:8000")

with DAG(
    dag_id="scout_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["scout"],
) as dag:

    @task
    def extract_players(n_players: int = 1000, batch_size: int = 100):
        """
        Fetches all players via the rate-limited API.
        Strategy:
          - batch_size=100 (max per minute)
          - sleep ~60s between batches
          - if 429, backoff and retry the same id
        Returns: list[dict]
        """
        session = requests.Session()
        out = []

        for start in range(1, n_players + 1, batch_size):
            end = min(start + batch_size - 1, n_players)

            for player_id in range(start, end + 1):
                url = f"{API_BASE}/player/{player_id}"

                while True:
                    r = session.get(url, timeout=15)

                    if r.status_code == 200:
                        out.append(r.json())
                        break

                    if r.status_code == 404:
                        # safe guard
                        break

                    if r.status_code == 429:
                        # graceful handling: wait and retry
                        time.sleep(2.0)
                        continue

                    # other errors: fail fast (visible in Airflow logs)
                    r.raise_for_status()

            # Respect 100 req/min hard limit:
            if end < n_players:
                time.sleep(60)

        return out

    extract_players()
