CREATE TABLE IF NOT EXISTS card_transactions (
    transaction_id VARCHAR(64) PRIMARY KEY,
    card_number_hash VARCHAR(256),
    merchant_id VARCHAR(64),
    amount DECIMAL(18,2),
    currency VARCHAR(8),
    txn_ts DATETIME,
    ingest_ts DATETIME,
    fraud_flag_high_amount INT
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(64) PRIMARY KEY,
    name VARCHAR(200),
    dob DATE,
    email VARCHAR(200)
);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id INT IDENTITY(1,1) PRIMARY KEY,
    transaction_id VARCHAR(64),
    alert_ts DATETIME,
    reason VARCHAR(256)
);
