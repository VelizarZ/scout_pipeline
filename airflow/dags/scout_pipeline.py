import os
import time
import random
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional

from dateutil import parser
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils import timezone

API_BASE = os.getenv("SCOUT_API_BASE_URL", "http://api:8000")
MIN_SECONDS_BETWEEN_CALLS = float(os.getenv("SCOUT_MIN_SECONDS_BETWEEN_CALLS", "0.65"))
MAX_RETRIES = int(os.getenv("SCOUT_MAX_RETRIES", "6"))

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=4),
}


def clean_string(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned if cleaned and cleaned != "-" and cleaned.lower() != "nan" else None


def parse_market_value(value: Optional[str]) -> Optional[str]:
    if not value or str(value).strip() in {"-", ""} or str(value).lower() == "nan":
        return None
    return str(value).strip() or None


def parse_agent_id(value: Optional[str]) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip()
    if s in {"", "-"} or s.lower() == "nan":
        return None
    try:
        return int(float(s))
    except (ValueError, TypeError):
        return None


def parse_date(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    value_str = str(value).strip()
    if value_str in {"", "-"} or value_str.lower() == "nan":
        return None

    date_formats = ["%d-%b-%y", "%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d"]
    for fmt in date_formats:
        try:
            return datetime.strptime(value_str, fmt)
        except ValueError:
            pass

    try:
        return parser.parse(value_str)
    except (ValueError, TypeError):
        print(f"Warning: Could not parse date: {value_str}")
        return None


with DAG(
    dag_id="scout_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["scout", "data-extraction"],
    max_active_runs=1,
    default_args=default_args,
    template_searchpath=["/opt/airflow/sql"],
) as dag:

    @task
    def get_player_ids() -> List[int]:
        """Fetch all player IDs from the API."""
        url = f"{API_BASE}/players/ids"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        ids = data.get("ids", [])
        print(f"Found {len(ids)} player IDs to process")
        return [int(x) for x in ids]

    @task(pool="api_pool")
    def extract_single_player(player_id: int) -> Dict:
        """Extract data for a single player with rate limiting and retries."""
        session = requests.Session()
        url = f"{API_BASE}/player/{player_id}"

        for attempt in range(MAX_RETRIES):
            try:
                time.sleep(MIN_SECONDS_BETWEEN_CALLS)
                r = session.get(url, timeout=15)

                if r.status_code == 200:
                    return {"success": True, "player_id": player_id, "data": r.json()}
                if r.status_code == 404:
                    return {"success": False, "player_id": player_id, "error": "not_found"}
                if r.status_code == 429:
                    backoff = min(60.0, (2 ** attempt)) + random.uniform(0, 0.5)
                    print(f"Rate limit hit for player {player_id}, waiting {backoff:.2f}s")
                    time.sleep(backoff)
                    continue

                r.raise_for_status()

            except requests.RequestException as e:
                if attempt == MAX_RETRIES - 1:
                    return {"success": False, "player_id": player_id, "error": str(e)}
                time.sleep(min(60.0, 2 ** attempt))

        return {"success": False, "player_id": player_id, "error": "max_retries_exceeded"}

    @task
    def transform_players(results: List[Dict]) -> List[Dict]:
        """Clean and transform player data."""
        transformed = []
        for result in results:
            if not result.get("success"):
                continue

            player_data = result.get("data", {})
            p = {
                "id": int(player_data.get("id", 0)),
                "first_name": clean_string(player_data.get("firstName")),
                "last_name": clean_string(player_data.get("lastName")),
                "team_name": clean_string(player_data.get("team_name")),
                "market_value": parse_market_value(player_data.get("market_value")),
                "agent": clean_string(player_data.get("agent")),
                "agent_id": parse_agent_id(player_data.get("agent_id")),
                "contract_expiry": parse_date(player_data.get("contract_expiry")),
            }

            transformed.append(p)

        print(f"Transformed {len(transformed)} players successfully")
        return transformed

    @task
    def load_players(transformed_players: List[Dict]) -> None:
        """Load players into the database."""
        if not transformed_players:
            print("No players to load")
            return

        from airflow.providers.postgres.hooks.postgres import PostgresHook
        hook = PostgresHook(postgres_conn_id="scout_db")
        loaded_count = 0
        error_count = 0
        now = timezone.utcnow()

        for player in transformed_players:
            try:
                hook.run(
                    """
                    INSERT INTO players (
                        id, first_name, last_name, team_name,
                        market_value, agent, agent_id, contract_expiry, updated_at
                    )
                    VALUES (
                        %(id)s, %(first_name)s, %(last_name)s, %(team_name)s,
                        %(market_value)s, %(agent)s, %(agent_id)s, %(contract_expiry)s, %(updated_at)s
                    )
                    ON CONFLICT (id)
                    DO UPDATE SET
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name,
                        team_name = EXCLUDED.team_name,
                        market_value = EXCLUDED.market_value,
                        agent = EXCLUDED.agent,
                        agent_id = EXCLUDED.agent_id,
                        contract_expiry = EXCLUDED.contract_expiry,
                        updated_at = EXCLUDED.updated_at
                    """,
                    parameters={**player, "updated_at": now},
                )
                loaded_count += 1
            except Exception as e:
                error_count += 1
                print(f"Error loading player {player.get('id')}: {e}")

        print(f"Load complete: {loaded_count} successful, {error_count} errors")



    # Main pipeline flow
    ids = get_player_ids()
    results = extract_single_player.expand(player_id=ids)
    transformed = transform_players(results)
    load_result = load_players(transformed)

    # Set task dependencies
    ids >> results >> transformed >> load_result