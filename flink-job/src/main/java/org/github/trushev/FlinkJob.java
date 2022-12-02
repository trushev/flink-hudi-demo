package org.github.trushev;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

@SuppressWarnings({"SqlNoDataSourceInspection", "SqlDialectInspection"})
public class FlinkJob {
    private static final String CHECKPOINTING_OPTION = "checkpointing";

    public static void main(String[] args) throws Exception {
        //language=SQL
        String sourceDDL = "" +
                "CREATE TABLE transactions (\n" +
                "    account_id  BIGINT,\n" +
                "    amount      BIGINT,\n" +
                "    transaction_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'transactions',\n" +
                "    'properties.bootstrap.servers' = 'kafka:9092',\n" +
                "    'scan.startup.mode' = 'earliest-offset',\n" +
                "    'format'    = 'csv'\n" +
                ")";
        //language=SQL
        String sinkDDL = "" +
                "CREATE TABLE reports(\n" +
                "    account_id BIGINT,\n" +
                "    log_ts     TIMESTAMP(3),\n" +
                "    amount     BIGINT\n," +
                "    PRIMARY KEY (account_id, log_ts) NOT ENFORCED" +
                ") WITH (\n" +
                "    'connector' = 'hudi',\n" +
                "    'path' = '/tmp/reports',\n" +
                "    'table.type' = 'MERGE_ON_READ',\n" +
                "    'hoodie.table.name' = 'reports'\n" +
                ")";
        //language=SQL
        String query = ""  +
                "INSERT INTO reports " +
                "SELECT account_id, window_start AS log_ts, SUM(amount) AS amount " +
                " FROM TABLE( " +
                "   TUMBLE(TABLE transactions, DESCRIPTOR(transaction_time), INTERVAL '1' HOURS)) " +
                " GROUP BY account_id, window_start, window_end";

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(executionEnvironment(ParameterTool.fromArgs(args)));
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);
        tEnv.executeSql(query).await();
    }

    private static StreamExecutionEnvironment executionEnvironment(ParameterTool params) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        if (params.has(CHECKPOINTING_OPTION)) {
            env.enableCheckpointing(1000);
        }
        return env;
    }
}
