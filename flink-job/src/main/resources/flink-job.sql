    CREATE TABLE transactions(
        account_id       BIGINT,
        amount           BIGINT,
        transaction_time TIMESTAMP(3),
        WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'transactions',
        'properties.bootstrap.servers' = 'kafka:9092',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'csv'
    );

    CREATE TABLE reports(
        account_id BIGINT,
        log_ts     TIMESTAMP(3),
        amount     BIGINT,
        PRIMARY KEY (account_id, log_ts) NOT ENFORCED
    ) WITH (
        'connector' = 'hudi',
        'path' = '/tmp/reports',
        'table.type' = 'MERGE_ON_READ',
        'hoodie.table.name' = 'reports'
    );

    INSERT INTO reports
        SELECT account_id, window_start AS log_ts, SUM(amount) AS amount
        FROM TABLE(
            TUMBLE(TABLE transactions, DESCRIPTOR(transaction_time), INTERVAL '1' HOURS)
        )
        GROUP BY account_id, window_start, window_end;

