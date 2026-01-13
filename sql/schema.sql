-- Scout Pipeline Database Schema
-- This schema stores player data extracted from the API

-- Players table - main table for storing player information
CREATE TABLE IF NOT EXISTS players (
    id INTEGER PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    team_name VARCHAR(255),
    market_value VARCHAR(50),  -- Stored as string to handle various formats (e.g., "€1.50m", "-")
    agent VARCHAR(255),
    agent_id INTEGER,
    contract_expiry DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for common queries
CREATE INDEX IF NOT EXISTS idx_players_team_name ON players(team_name);
CREATE INDEX IF NOT EXISTS idx_players_agent_id ON players(agent_id);
CREATE INDEX IF NOT EXISTS idx_players_contract_expiry ON players(contract_expiry);
CREATE INDEX IF NOT EXISTS idx_players_updated_at ON players(updated_at);

-- Agents table - normalized agent information
CREATE TABLE IF NOT EXISTS agents (
    agent_id INTEGER PRIMARY KEY,
    agent_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Foreign key constraint (optional, can be added if referential integrity is needed)
-- ALTER TABLE players ADD CONSTRAINT fk_players_agent FOREIGN KEY (agent_id) REFERENCES agents(agent_id);

-- Pipeline execution log table - tracks pipeline runs
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    dag_run_id VARCHAR(255),
    execution_date TIMESTAMP,
    status VARCHAR(50),
    players_extracted INTEGER DEFAULT 0,
    players_loaded INTEGER DEFAULT 0,
    players_failed INTEGER DEFAULT 0,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_execution_date ON pipeline_runs(execution_date);
CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status ON pipeline_runs(status);

-- Data quality metrics table
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    metric_id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES pipeline_runs(run_id),
    metric_name VARCHAR(100),
    metric_value INTEGER,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
