CREATE TABLE IF NOT EXISTS measurements (
    time            TIMESTAMPTZ      NOT NULL,
    col_a           TEXT             NOT NULL,
    col_b           TEXT             NOT NULL,
    col_c           INTEGER          NOT NULL,
    col_d           DOUBLE PRECISION,
    col_e           TEXT,
    col_f           TEXT,
    nullable_col    TEXT,
    UNIQUE NULLS NOT DISTINCT (time, col_a, col_b, col_c, col_e, nullable_col)
);

SELECT create_hypertable(
    'measurements',
    by_range('time'),
    if_not_exists => TRUE
);

ALTER TABLE measurements
    SET (timescaledb.compress,
         timescaledb.compress_segmentby = 'col_a, col_b',
         timescaledb.compress_orderby = 'time DESC');
