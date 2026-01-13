import os
import time
import random
import requests
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from dateutil import parser

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

API_BASE = os.getenv("SCOUT_API_BASE_URL", "http://api:8000")
MIN_SECONDS_BETWEEN_CALLS = float(os.getenv("SCOUT_MIN_SECONDS_BETWEEN_CALLS", "0.65"))
MAX_RETRIES = int(os.getenv("SCOUT_MAX_RETRIES", "6"))

# Database connection parameters
DB_HOST = os.getenv("SCOUT_PG_HOST", "postgres")
DB_NAME = os.getenv("SCOUT_PG_DB", "scout")
DB_USER = os.getenv("SCOUT_PG_USER", "scout")
DB_PASSWORD = os.getenv("SCOUT_PG_PASSWORD", "scout")

# Create connection string
DB_CONN_STRING = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=4),
}

with DAG(
    dag_id="scout_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["scout", "data-extraction"],
    max_active_runs=1,
    default_args=default_args,
) as dag:

    def get_db_hook():
        """Get PostgresHook for database operations."""
        try:
            # Try using PostgresHook with connection ID first
            return PostgresHook(postgres_conn_id='scout_db')
        except Exception:
            # Fall back to direct connection using psycopg2
            import psycopg2
            from contextlib import contextmanager
            
            class SimpleHook:
                """Simple wrapper around psycopg2 to mimic PostgresHook interface."""
                def __init__(self):
                    self.conn_params = {
                        'host': DB_HOST,
                        'database': DB_NAME,
                        'user': DB_USER,
                        'password': DB_PASSWORD,
                        'port': 5432
                    }
                
                @contextmanager
                def _get_connection(self):
                    """Get a database connection with proper cleanup."""
                    conn = psycopg2.connect(**self.conn_params)
                    try:
                        yield conn
                        conn.commit()
                    except Exception:
                        conn.rollback()
                        raise
                    finally:
                        conn.close()
                
                def run(self, sql, parameters=None):
                    """Execute SQL statement with optional parameters."""
                    with self._get_connection() as conn:
                        cur = conn.cursor()
                        try:
                            if parameters:
                                cur.execute(sql, parameters)
                            else:
                                cur.execute(sql)
                        finally:
                            cur.close()
            
            return SimpleHook()

    @task
    def initialize_database():
        """Initialize database schema if it doesn't exist."""
        hook = get_db_hook()
        
        # Read and execute schema SQL
        schema_path = "/opt/airflow/sql/schema.sql"
        try:
            with open(schema_path, 'r') as f:
                schema_sql = f.read()
            # Execute schema creation (split by semicolon for multiple statements)
            for statement in schema_sql.split(';'):
                statement = statement.strip()
                if statement:
                    try:
                        hook.run(statement)
                    except Exception as e:
                        # Ignore errors for existing objects
                        if "already exists" not in str(e).lower() and "duplicate" not in str(e).lower():
                            print(f"Schema statement warning: {e}")
            print("Database schema initialized successfully")
        except Exception as e:
            print(f"Schema initialization note: {e}")
            # Continue even if schema already exists

    @task
    def get_player_ids() -> List[int]:
        """Fetch all player IDs from API."""
        url = f"{API_BASE}/players/ids"
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        data = r.json()
        ids = data.get("ids", [])
        print(f"Found {len(ids)} player IDs to process")
        return [int(x) for x in ids]

    @task
    def extract_single_player(player_id: int) -> Dict:
        """Extract data for a single player with retry logic and rate limit handling."""
        session = requests.Session()
        url = f"{API_BASE}/player/{player_id}"
        
        for attempt in range(MAX_RETRIES):
            try:
                time.sleep(MIN_SECONDS_BETWEEN_CALLS)
                r = session.get(url, timeout=15)
                
                if r.status_code == 200:
                    return {"success": True, "player_id": player_id, "data": r.json()}
                elif r.status_code == 404:
                    return {"success": False, "player_id": player_id, "error": "not_found"}
                elif r.status_code == 429:
                    # Exponential backoff with jitter for rate limit
                    backoff = min(60.0, (2 ** attempt)) + random.uniform(0, 0.5)
                    print(f"Rate limit hit for player {player_id}, waiting {backoff:.2f}s")
                    time.sleep(backoff)
                    continue
                else:
                    r.raise_for_status()
                    
            except requests.RequestException as e:
                if attempt == MAX_RETRIES - 1:
                    return {"success": False, "player_id": player_id, "error": str(e)}
                time.sleep(2 ** attempt)
        
        return {"success": False, "player_id": player_id, "error": "max_retries_exceeded"}

    def clean_string(value: Optional[str]) -> Optional[str]:
        """Clean string values: strip whitespace, handle None/empty."""
        if value is None:
            return None
        cleaned = str(value).strip()
        return cleaned if cleaned and cleaned != '-' and cleaned.lower() != 'nan' else None

    def parse_market_value(value: Optional[str]) -> Optional[str]:
        """Parse and normalize market value. Keep as string to preserve format."""
        if not value or value == '-' or value.lower() == 'nan':
            return None
        # Clean and return as-is (e.g., "€1.50m", "€175k")
        cleaned = str(value).strip()
        return cleaned if cleaned else None

    def parse_agent_id(value: Optional[str]) -> Optional[int]:
        """Parse agent_id to integer, handling empty strings and None."""
        if not value or value == '-' or value == '' or str(value).lower() == 'nan':
            return None
        try:
            return int(float(str(value)))
        except (ValueError, TypeError):
            return None

    def parse_date(value: Optional[str]) -> Optional[datetime]:
        """Parse contract expiry date from various formats."""
        if not value or value == '-' or value == '' or str(value).lower() == 'nan':
            return None
        
        value_str = str(value).strip()
        
        # Common date formats in the data
        date_formats = [
            "%d-%b-%y",      # 31-Dec-28
            "%d-%b-%Y",      # 31-Dec-2028
            "%d-%m-%Y",      # 31-12-2028
            "%Y-%m-%d",      # 2028-12-31
        ]
        
        for fmt in date_formats:
            try:
                return datetime.strptime(value_str, fmt)
            except ValueError:
                continue
        
        # Try dateutil parser as fallback
        try:
            return parser.parse(value_str)
        except (ValueError, TypeError):
            print(f"Warning: Could not parse date: {value_str}")
            return None

    @task
    def transform_players(results: List[Dict]) -> List[Dict]:
        """Transform and clean player data."""
        transformed = []
        
        for result in results:
            if not result.get('success'):
                continue
            
            player_data = result.get('data', {})
            
            # Extract and clean fields
            transformed_player = {
                'id': int(player_data.get('id', 0)),
                'first_name': clean_string(player_data.get('firstName')),
                'last_name': clean_string(player_data.get('lastName')),
                'team_name': clean_string(player_data.get('team_name')),
                'market_value': parse_market_value(player_data.get('market_value')),
                'agent': clean_string(player_data.get('agent')),
                'agent_id': parse_agent_id(player_data.get('agent_id')),
                'contract_expiry': parse_date(player_data.get('contract_expiry')),
            }
            
            # Data quality checks
            issues = []
            if not transformed_player['first_name']:
                issues.append("missing_first_name")
            if not transformed_player['last_name']:
                issues.append("missing_last_name")
            
            transformed_player['_data_quality_issues'] = issues
            transformed.append(transformed_player)
        
        print(f"Transformed {len(transformed)} players successfully")
        return transformed

    @task
    def load_players(transformed_players: List[Dict]) -> Dict:
        """Load transformed player data to PostgreSQL database."""
        if not transformed_players:
            return {"loaded": 0, "errors": 0}
        
        hook = get_db_hook()
        
        loaded_count = 0
        error_count = 0
        
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
                    parameters={
                        'id': player['id'],
                        'first_name': player['first_name'],
                        'last_name': player['last_name'],
                        'team_name': player['team_name'],
                        'market_value': player['market_value'],
                        'agent': player['agent'],
                        'agent_id': player['agent_id'],
                        'contract_expiry': player['contract_expiry'],
                        'updated_at': datetime.now()
                    }
                )
                loaded_count += 1
            except Exception as e:
                error_count += 1
                print(f"Error loading player {player.get('id')}: {e}")
        
        # Also load agent information if available
        agents_loaded = 0
        for player in transformed_players:
            if player.get('agent_id') and player.get('agent'):
                try:
                    hook.run(
                        """
                        INSERT INTO agents (agent_id, agent_name, updated_at)
                        VALUES (%(agent_id)s, %(agent_name)s, %(updated_at)s)
                        ON CONFLICT (agent_id) 
                        DO UPDATE SET agent_name = EXCLUDED.agent_name, updated_at = EXCLUDED.updated_at
                        """,
                        parameters={
                            'agent_id': player['agent_id'],
                            'agent_name': player['agent'],
                            'updated_at': datetime.now()
                        }
                    )
                    agents_loaded += 1
                except Exception as e:
                    pass  # Silently continue if agent insert fails
        
        return {
            "loaded": loaded_count,
            "errors": error_count,
            "agents_loaded": agents_loaded
        }

    @task
    def log_pipeline_run(load_result: Dict, extraction_results: List[Dict]) -> None:
        """Log pipeline execution to database."""
        successful = [r for r in extraction_results if r.get('success')]
        failed = [r for r in extraction_results if not r.get('success')]
        
        try:
            hook = get_db_hook()
            
            hook.run(
                """
                INSERT INTO pipeline_runs (
                    execution_date, status, players_extracted, 
                    players_loaded, players_failed, completed_at
                )
                VALUES (
                    %(execution_date)s, %(status)s, %(extracted)s,
                    %(loaded)s, %(failed)s, %(completed_at)s
                )
                """,
                parameters={
                    'execution_date': datetime.now(),
                    'status': 'success' if load_result.get('errors', 0) == 0 else 'partial',
                    'extracted': len(successful),
                    'loaded': load_result.get('loaded', 0),
                    'failed': len(failed),
                    'completed_at': datetime.now()
                }
            )
        except Exception as e:
            print(f"Warning: Could not log pipeline run: {e}")

    @task
    def generate_report(results: List[Dict], load_result: Dict) -> None:
        """Generate and log extraction summary."""
        successful = [r for r in results if r.get('success')]
        failed = [r for r in results if not r.get('success')]
        
        report = {
            "total": len(results),
            "successful": len(successful),
            "failed": len(failed),
            "loaded_to_db": load_result.get('loaded', 0),
            "load_errors": load_result.get('errors', 0),
            "failed_ids": [r['player_id'] for r in failed[:10]]  # Limit for readability
        }
        
        print("=" * 60)
        print("EXTRACTION REPORT")
        print("=" * 60)
        print(f"Total players processed: {report['total']}")
        print(f"Successfully extracted: {report['successful']}")
        print(f"Failed extractions: {report['failed']}")
        print(f"Loaded to database: {report['loaded_to_db']}")
        print(f"Load errors: {report['load_errors']}")
        if report['failed_ids']:
            print(f"Sample failed IDs: {report['failed_ids']}")
        print("=" * 60)
        
        if len(results) > 0 and len(failed) / len(results) > 0.05:  # Alert if >5% failure
            print(f"WARNING: High failure rate - {len(failed)}/{len(results)} failed")

    # Define task dependencies
    init_db = initialize_database()
    ids = get_player_ids()
    results = extract_single_player.expand(player_id=ids)
    transformed = transform_players(results)
    load_result = load_players(transformed)
    log_pipeline_run(load_result, results)
    generate_report(results, load_result)
    
    # Set dependencies
    init_db >> ids >> results >> transformed >> load_result
    load_result >> log_pipeline_run(load_result, results)
    load_result >> generate_report(results, load_result)
