
-- Create the dedicated scout database
CREATE DATABASE scout;

-- Ensure the airflow user owns the database
ALTER DATABASE scout OWNER TO airflow;

-- Switch to the scout database for subsequent commands
\connect scout;

-- Create the players table in the scout database
CREATE TABLE IF NOT EXISTS players (
    id INTEGER PRIMARY KEY,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    team_name VARCHAR(255),
    market_value VARCHAR(50),
    agent VARCHAR(255),
    agent_id INTEGER,
    contract_expiry DATE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
