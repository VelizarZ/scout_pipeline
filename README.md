# Scout Data Pipeline

A complete data engineering pipeline for ingesting, transforming, and loading player data from a rate-limited API into a PostgreSQL database using Apache Airflow.

## Overview

This project implements a production-ready data pipeline that:
- Serves player data via a FastAPI application with strict rate limiting (100 requests/minute)
- Extracts all player data while respecting API rate limits
- Transforms and cleans the data to handle real-world data quality issues
- Loads the data into a normalized PostgreSQL database schema
- Tracks pipeline execution and data quality metrics

## Architecture

```
┌─────────────┐
│   FastAPI   │  Serves player data with rate limiting
│   (Port 8000)│
└──────┬──────┘
       │
       │ HTTP Requests (rate limited)
       │
┌──────▼──────────────────────────────┐
│      Apache Airflow DAG             │
│  ┌──────────────────────────────┐   │
│  │ 1. Extract (with retry logic) │   │
│  │ 2. Transform & Clean          │   │
│  │ 3. Load to PostgreSQL         │   │
│  └──────────────────────────────┘   │
└──────┬──────────────────────────────┘
       │
       │ SQL INSERT/UPDATE
       │
┌──────▼──────┐
│  PostgreSQL │  Stores normalized player data
│  (Port 5432)│
└─────────────┘
```

## Project Structure

```
scout_pipeline/
├── app/                    # FastAPI application
│   ├── main.py            # API with rate limiting
│   ├── players.csv        # Source data (1000 players)
│   ├── requirements.txt   # Python dependencies
│   └── Dockerfile         # Container definition
│
├── airflow/                # Airflow DAGs and configuration
│   ├── dags/
│   │   └── scout_pipeline.py  # Main ETL pipeline
│   ├── requirements.txt   # Airflow dependencies
│   └── Dockerfile         # Airflow container
│
├── sql/                    # Database schema
│   └── schema.sql         # PostgreSQL schema definition
│
├── docker-compose.yml      # Orchestrates all services
└── README.md              # This file
```

## Features

### Part 1: Mock Data Source (FastAPI)
- ✅ RESTful API serving player data
- ✅ Hard rate limit: 100 requests per minute
- ✅ Returns 429 status code when rate limit exceeded
- ✅ Endpoint: `GET /player/{id}`

### Part 2: Data Pipeline (Airflow)
- ✅ **Extraction**: Fetches all 1,000 players with rate limit compliance
- ✅ **Rate Limit Handling**: Exponential backoff on 429 errors
- ✅ **Transformation**: Data cleaning and type normalization
- ✅ **Loading**: Structured PostgreSQL schema with proper data types
- ✅ **Error Handling**: Graceful failure handling without pipeline crashes
- ✅ **Monitoring**: Pipeline execution logging and reporting

### Part 3: Database Schema
- ✅ Normalized schema with proper data types
- ✅ Indexes for common query patterns
- ✅ Separate tables for players and agents
- ✅ Pipeline execution tracking

### Part 4: Containerization & Documentation
- ✅ Complete docker-compose setup
- ✅ One-command deployment
- ✅ Comprehensive README
- ✅ Data dictionary (see below)

## Quick Start

### Prerequisites
- Docker and Docker Compose installed
- At least 4GB of available RAM

### Running the Pipeline

1. **Clone and navigate to the project:**
   ```bash
   cd scout_pipeline
   ```

2. **Start all services:**
   ```bash
   docker-compose up -d
   ```

3. **Wait for services to initialize** (about 1-2 minutes):
   ```bash
   docker-compose ps
   ```

4. **Access the services:**
   - **FastAPI**: http://localhost:8000
   - **Airflow UI**: http://localhost:8080 (admin/admin)
   - **PostgreSQL**: localhost:5432 (scout/scout)

5. **Trigger the pipeline:**
   - Open Airflow UI at http://localhost:8080
   - Find the `scout_pipeline` DAG
   - Click the play button to trigger a run

6. **Monitor progress:**
   - View DAG runs in Airflow UI
   - Check logs for detailed execution information

### Testing the API

```bash
# Get a single player
curl http://localhost:8000/player/743075

# Get all player IDs
curl http://localhost:8000/players/ids

# Test rate limiting (make 101 requests quickly)
for i in {1..101}; do curl http://localhost:8000/player/743075; done
```

### Querying the Database

```bash
# Connect to PostgreSQL
docker exec -it scout_postgres psql -U scout -d scout

# Example queries:
SELECT COUNT(*) FROM players;
SELECT * FROM players LIMIT 10;
SELECT team_name, COUNT(*) FROM players GROUP BY team_name ORDER BY COUNT(*) DESC;
SELECT * FROM pipeline_runs ORDER BY execution_date DESC LIMIT 5;
```

## Data Dictionary

### `players` Table
Stores the main player information.

| Column | Type | Description | Notes |
|--------|------|-------------|-------|
| `id` | INTEGER | Primary key, player ID | Unique identifier from source data |
| `first_name` | VARCHAR(255) | Player's first name | May be NULL for some records |
| `last_name` | VARCHAR(255) | Player's last name | May be NULL for some records |
| `team_name` | VARCHAR(255) | Current team | NULL if player is free agent |
| `market_value` | VARCHAR(50) | Market value | Format: "€1.50m", "€175k", or NULL |
| `agent` | VARCHAR(255) | Agent name | NULL if no agent |
| `agent_id` | INTEGER | Agent identifier | Foreign key to agents table, NULL if no agent |
| `contract_expiry` | DATE | Contract expiration date | NULL if no contract or unknown |
| `created_at` | TIMESTAMP | Record creation timestamp | Auto-populated |
| `updated_at` | TIMESTAMP | Last update timestamp | Auto-updated on each run |

**Indexes:**
- Primary key on `id`
- Index on `team_name` for team-based queries
- Index on `agent_id` for agent-based queries
- Index on `contract_expiry` for contract analysis
- Index on `updated_at` for change tracking

### `agents` Table
Normalized agent information.

| Column | Type | Description |
|--------|------|-------------|
| `agent_id` | INTEGER | Primary key, agent identifier |
| `agent_name` | VARCHAR(255) | Agent or agency name |
| `created_at` | TIMESTAMP | Record creation timestamp |
| `updated_at` | TIMESTAMP | Last update timestamp |

### `pipeline_runs` Table
Tracks pipeline execution history.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | SERIAL | Primary key |
| `dag_run_id` | VARCHAR(255) | Airflow DAG run identifier |
| `execution_date` | TIMESTAMP | When the pipeline ran |
| `status` | VARCHAR(50) | 'success' or 'partial' |
| `players_extracted` | INTEGER | Number of players successfully extracted |
| `players_loaded` | INTEGER | Number of players loaded to database |
| `players_failed` | INTEGER | Number of failed extractions |
| `started_at` | TIMESTAMP | Pipeline start time |
| `completed_at` | TIMESTAMP | Pipeline completion time |
| `error_message` | TEXT | Error details if any |

## Data Transformation & Cleaning

The pipeline implements comprehensive data cleaning:

1. **String Cleaning:**
   - Strips whitespace
   - Converts empty strings, "-", and "nan" to NULL
   - Handles encoding issues

2. **Market Value:**
   - Preserved as string to maintain format (e.g., "€1.50m", "€175k")
   - Empty values converted to NULL

3. **Agent ID:**
   - Parsed to INTEGER
   - Empty values converted to NULL

4. **Date Parsing:**
   - Handles multiple date formats (e.g., "31-Dec-28", "30-Jun-2025")
   - Uses dateutil parser as fallback
   - Invalid dates logged and set to NULL

5. **Data Quality Checks:**
   - Flags records with missing critical fields
   - Tracks data quality issues for monitoring

## Rate Limiting Strategy

The pipeline respects the 100 requests/minute limit by:

1. **Minimum Delay**: 0.65 seconds between requests (allows ~92 requests/minute with buffer)
2. **Exponential Backoff**: On 429 errors, waits with exponential backoff (2^attempt seconds)
3. **Retry Logic**: Up to 6 retries per player with increasing delays
4. **Graceful Degradation**: Failed extractions are logged but don't crash the pipeline

**Expected Runtime**: ~11-15 minutes for 1,000 players (accounting for rate limits and retries)

## Configuration

### Environment Variables

**FastAPI (app):**
- No configuration needed (uses default port 8000)

**Airflow:**
- `SCOUT_API_BASE_URL`: API endpoint (default: http://api:8000)
- `SCOUT_PG_HOST`: PostgreSQL host (default: postgres)
- `SCOUT_PG_DB`: Database name (default: scout)
- `SCOUT_PG_USER`: Database user (default: scout)
- `SCOUT_PG_PASSWORD`: Database password (default: scout)
- `SCOUT_MIN_SECONDS_BETWEEN_CALLS`: Rate limit delay (default: 0.65)
- `SCOUT_MAX_RETRIES`: Max retries per player (default: 6)

### Modifying Rate Limits

To adjust the rate limiting behavior, modify `docker-compose.yml`:

```yaml
environment:
  SCOUT_MIN_SECONDS_BETWEEN_CALLS: "0.65"  # Increase for slower rate
  SCOUT_MAX_RETRIES: "6"                    # Increase for more retries
```

## Troubleshooting

### Services won't start
```bash
# Check logs
docker-compose logs

# Restart services
docker-compose down
docker-compose up -d
```

### Airflow connection errors
The pipeline uses a connection ID `scout_db`. If you see connection errors:
1. Open Airflow UI → Admin → Connections
2. Create connection with ID: `scout_db`
3. Type: Postgres
4. Host: postgres
5. Schema: scout
6. Login: scout
7. Password: scout

Alternatively, the DAG will attempt to use environment variables directly.

### High failure rate
- Check API health: `curl http://localhost:8000/players/ids`
- Verify rate limiting isn't too aggressive
- Check Airflow task logs for specific errors

### Database connection issues
```bash
# Test database connection
docker exec -it scout_postgres psql -U scout -d scout -c "SELECT COUNT(*) FROM players;"
```

## Development

### Running Locally (without Docker)

1. **FastAPI:**
   ```bash
   cd app
   pip install -r requirements.txt
   uvicorn main:app --reload
   ```

2. **Airflow:**
   ```bash
   # Requires PostgreSQL for Airflow metadata
   export AIRFLOW_HOME=~/airflow
   airflow db init
   airflow webserver
   airflow scheduler
   ```

3. **PostgreSQL:**
   ```bash
   # Install PostgreSQL locally or use Docker
   createdb scout
   psql scout < sql/schema.sql
   ```

## Schema Design Rationale

1. **Normalized Structure**: Separate `agents` table to avoid duplication and enable agent analytics
2. **Flexible Market Value**: Stored as VARCHAR to preserve original format (€1.50m, €175k)
3. **Nullable Fields**: Many fields are nullable to handle missing data gracefully
4. **Indexes**: Strategic indexes on commonly queried fields (team, agent, contract expiry)
5. **Audit Trail**: `created_at` and `updated_at` timestamps for change tracking
6. **Pipeline Monitoring**: `pipeline_runs` table enables historical analysis of pipeline performance

## Performance Considerations

- **Batch Processing**: Uses Airflow's dynamic task mapping for parallel extraction
- **Connection Pooling**: Reuses database connections efficiently
- **Indexes**: Optimized for common query patterns
- **Rate Limiting**: Built-in delays prevent API overload

## Visualization

See `GCP_DEPLOYMENT_IDEAS.md` for visualization options including:
- Google Looker Studio integration
- Apache Superset setup
- Grafana for monitoring
- Metabase setup
- Custom Streamlit dashboards
- GCP deployment strategies

**Pre-built Views**: See `sql/dashboard_views.sql` for sample SQL views that can be used with any BI tool.

## Future Enhancements

Potential improvements:
- Add data validation rules and constraints
- Implement incremental loading (only changed players)
- Add data quality dashboards
- Implement change data capture (CDC)
- Add API authentication
- Implement data partitioning for large-scale deployments
- GCP deployment automation (Terraform)
- Real-time dashboard updates

## License

This project is created for the EnskAI Data Engineer technical challenge.

## Contact

For questions or issues, please refer to the project documentation or create an issue in the repository.
