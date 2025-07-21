-- migrate:up

CREATE TABLE IF NOT EXISTS token_id_scores (
    block_start INTEGER NOT NULL,
    block_start_stake INTEGER NOT NULL,
    block_end INTEGER NOT NULL,
    delta_stake REAL NOT NULL,
    growth_info TEXT NOT NULL,
    scores TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transfers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    status TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'failed')),
    origin_coldkey TEXT NOT NULL,
    destination_coldkey TEXT NOT NULL,
    destination_h160 TEXT NOT NULL,
    origin_hotkey TEXT NOT NULL,
    destination_hotkey TEXT NOT NULL,
    amount REAL DEFAULT 0.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- migrate:down
DROP TABLE IF EXISTS token_id_scores;
DROP TABLE IF EXISTS transfers;