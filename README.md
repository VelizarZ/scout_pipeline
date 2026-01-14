# Scout Data Pipeline

A complete pipeline for ingesting, transforming, and loading player data from a rate-limited API into a PostgreSQL database using Apache Airflow.


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


Run the application inside the root folder with
```console
docker-compose up --build
```
After Airflow webserver is running, open Airflow in your local browser and go to:
```
localhost:8080
```
with username: `airflow`
and password: `airflow`  
After logging, you have to set up the api_pool from Admin/Pools with a 100 Slots.

A tool like DBeaver can be used to quickly connect to the PostgreSQL instance and check the players table before and after the ingestions.
The DB instance is at 
```
localhost:5432
```
