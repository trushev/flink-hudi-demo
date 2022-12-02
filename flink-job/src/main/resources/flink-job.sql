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

CREATE TABLE send_report(
    account_id       BIGINT,
    amount           BIGINT,
    transaction_time TIMESTAMP(3)
) WITH (
    'connector' = 'hudi',
    'path' = '/tmp/send_report',
    'table.type' = 'MERGE_ON_READ',
    'hoodie.table.name' = 'send_report',
    'hoodie.datasource.write.recordkey.field' = 'account_id'
);

INSERT INTO send_report SELECT account_id, amount, transaction_time FROM transactions;
