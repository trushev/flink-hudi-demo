package org.github.trushev;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class FlinkJob {
    private static final String CHECKPOINTING_OPTION = "checkpointing";
    private static final String SQL_QUERIES_PATH = "flink-job.sql";

    public static void main(String[] args) throws Exception {
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(executionEnvironment(ParameterTool.fromArgs(args)));

        //language=SQL
        tEnv.executeSql("CREATE TABLE transactions (\n" +
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
                ")"
        );
        //language=SQL
        tEnv.executeSql("CREATE TABLE send_report(\n" +
                "    account_id       BIGINT,\n" +
                "    amount           BIGINT,\n" +
                "    transaction_time TIMESTAMP(3)\n" +
                ") WITH (\n" +
                "    'connector' = 'hudi',\n" +
                "    'path' = '/tmp/send_report',\n" +
                "    'table.type' = 'MERGE_ON_READ',\n" +
                "    'hoodie.table.name' = 'send_report',\n" +
                "    'hoodie.datasource.write.recordkey.field' = 'account_id'\n" +
                ")"
        );
        //language=SQL
        tEnv.executeSql("INSERT INTO send_report SELECT account_id, amount, transaction_time FROM transactions").await();
    }

    private static StreamExecutionEnvironment executionEnvironment(ParameterTool params) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (params.has(CHECKPOINTING_OPTION)) {
            env.enableCheckpointing(1000);
        }
        return env;
    }

    private static List<String> sqlQueries() throws URISyntaxException, IOException {
        Path path = Paths.get(Objects.requireNonNull(FlinkJob.class.getClassLoader().getResource(SQL_QUERIES_PATH)).toURI());
        String queries = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        return Arrays.stream(queries.split(";")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }
}
