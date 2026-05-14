CREATE TABLE IF NOT EXISTS audit.etl_run_log (
    run_id SERIAL PRIMARY KEY,
    pipeline_name TEXT NOT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMP,
    status TEXT NOT NULL,
    rows_read INTEGER DEFAULT 0,
    rows_loaded INTEGER DEFAULT 0,
    rows_rejected INTEGER DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS audit.file_load_log (
    file_load_id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES audit.etl_run_log(run_id),
    source_file TEXT NOT NULL,
    target_table TEXT NOT NULL,
    rows_loaded INTEGER DEFAULT 0,
    loaded_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit.data_quality_results (
    check_id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES audit.etl_run_log(run_id),
    table_name TEXT NOT NULL,
    check_name TEXT NOT NULL,
    check_status TEXT NOT NULL,
    failed_rows INTEGER DEFAULT 0,
    checked_at TIMESTAMP DEFAULT NOW()
);