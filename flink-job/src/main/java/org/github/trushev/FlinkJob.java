package org.github.trushev;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

@SuppressWarnings({"SqlNoDataSourceInspection", "SqlDialectInspection"})
public class FlinkJob {
    private static final String CHECKPOINTING = "checkpointing";
    private static final String BOOTSTRAP = "bootstrap";
    private static final String TABLE_PATH = "table-path";
    private static final String TABLE_TYPE = "table-type";
    private static final String TABLE_OPTIONS = "table-options";
    private static final String PARALLELISM = "parallelism";
    private static final String STRINGS = "strings";

    public static void main(String[] args) throws Exception {
        System.out.println("Running flink job");
        Configuration hadoopConf = getHadoopConf();
        System.out.println("_______________________________________________");
        System.out.println("hadoopConf");
        hadoopConf.getPropsWithPrefix("").forEach((k, v) -> System.out.println(k + " -> " + v));
        System.out.println("_______________________________________________");

        ParameterTool parameterTool = ParameterTool.fromArgs(args);

        int parallelism = parameterTool.getInt(PARALLELISM, 1);
        long checkpointing = parameterTool.getLong(CHECKPOINTING, 5000);
        String bootstrap = parameterTool.get(BOOTSTRAP, "localhost:9092");
        boolean useStrings = parameterTool.getBoolean(STRINGS, true);
        String tablePath = parameterTool.get(TABLE_PATH, "/tmp/reports");
        String tableType = parameterTool.get(TABLE_TYPE, "MERGE_ON_READ");
        String tableOptions = parameterTool.get(TABLE_OPTIONS, "");

        String options = String.format(
                "Running with options:%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s;%s=%s",
                CHECKPOINTING, checkpointing,
                BOOTSTRAP, bootstrap,
                TABLE_PATH, tablePath,
                TABLE_TYPE, tablePath,
                PARALLELISM, parallelism,
                STRINGS, useStrings,
                TABLE_OPTIONS, tableOptions
        );
        System.out.println(options);

        StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment()
                .setParallelism(parallelism);
        if (checkpointing != -1) {
            env.enableCheckpointing(checkpointing);
        }

//        if (true) {
//            env.setStateBackend(new HashMapStateBackend());
//            env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints-directory");
//        }

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //language=SQL
        String sourceDDL = String.format("" +
                "CREATE TABLE transactions ( \n" +
                "    account_id  BIGINT, \n" +
                "    amount      BIGINT, \n" +
                "    transaction_time TIMESTAMP(3), \n" +
                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND \n" +
                ") WITH ( \n" +
                "    'connector' = 'kafka', \n" +
                "    'topic'     = 'transactions', \n" +
                "    'properties.bootstrap.servers' = '%s', \n" +
                "    'scan.startup.mode' = 'earliest-offset', \n" +
                "    'format'    = 'csv' \n" +
                ")",
                bootstrap
        );
        tEnv.executeSql(sourceDDL);

        String sinkDDL = hudiTable(tablePath, tableType, useStrings, tableOptions);
        System.out.println(sinkDDL);
        tEnv.executeSql(sinkDDL);

        //language=SQL
        String query = "" +
                "INSERT INTO reports  \n" +
                "SELECT  \n" +
                "    account_id,  \n" +
                "    window_end AS log_ts,  \n" +

                "    CAST(MIN(amount) AS INT),  \n" +
                "    CAST(SUM(amount) AS BIGINT),  \n" +
                "    CAST(AVG(amount) AS DOUBLE),  \n" +
                (useStrings ? "    CAST(MAX(amount) AS STRING),  \n" : "") +

                "    CAST(MIN(amount) AS INT),  \n" +
                "    CAST(SUM(amount) AS BIGINT),  \n" +
                "    CAST(AVG(amount) AS DOUBLE),  \n" +
                (useStrings ? "    CAST(MAX(amount) AS STRING),  \n" : "") +

                "    CAST(MIN(amount) AS INT),  \n" +
                "    CAST(SUM(amount) AS BIGINT),  \n" +
                "    CAST(AVG(amount) AS DOUBLE),  \n" +
                (useStrings ? "    CAST(MAX(amount) AS STRING),  \n" : "") +

                "    CAST(MIN(amount) AS INT),  \n" +
                "    CAST(SUM(amount) AS BIGINT),  \n" +
                "    CAST(AVG(amount) AS DOUBLE),  \n" +
                (useStrings ? "    CAST(MAX(amount) AS STRING),  \n" : "") +

                "    CAST(MIN(amount) AS INT),  \n" +
                "    CAST(SUM(amount) AS BIGINT),  \n" +
                "    CAST(AVG(amount) AS DOUBLE),  \n" +
                (useStrings ? "    CAST(MAX(amount) AS STRING),  \n" : "") +

                "    CAST(MIN(amount) AS INT),  \n" +
                "    CAST(SUM(amount) AS BIGINT),  \n" +
                "    CAST(AVG(amount) AS DOUBLE),  \n" +
                (useStrings ? "    CAST(MAX(amount) AS STRING),  \n" : "") +

                "    CAST(MIN(amount) AS INT),  \n" +
                "    CAST(SUM(amount) AS BIGINT),  \n" +
                "    CAST(AVG(amount) AS DOUBLE),  \n" +
                (useStrings ? "    CAST(MAX(amount) AS STRING),  \n" : "") +

                "    CAST(MIN(amount) AS INT),  \n" +
                "    CAST(SUM(amount) AS BIGINT),  \n" +
                "    CAST(AVG(amount) AS DOUBLE),  \n" +
                (useStrings ? "    CAST(MAX(amount) AS STRING),  \n" : "") +

                "    CAST(MIN(amount) AS INT),  \n" +
                "    CAST(SUM(amount) AS BIGINT),  \n" +
                "    CAST(AVG(amount) AS DOUBLE),  \n" +
                (useStrings ? "    CAST(MAX(amount) AS STRING),  \n" : "") +

                "    CAST(MIN(amount) AS INT),  \n" +
                "    CAST(SUM(amount) AS BIGINT),  \n" +
                (useStrings ? "    CAST(AVG(amount) AS DOUBLE),  \n" : "    CAST(AVG(amount) AS DOUBLE)  \n") +
                (useStrings ? "    CAST(MAX(amount) AS STRING)  \n" : "") +

                "  FROM TABLE(TUMBLE(TABLE transactions, DESCRIPTOR(transaction_time), INTERVAL '1' HOUR ))  \n" +
                "  GROUP BY account_id, window_start, window_end  \n";
        tEnv.executeSql(query).await();
    }

    public static String hudiTable(String tablePath, String tableType, boolean useStrings, String tableOptions) {
        String options = Arrays.stream(tableOptions.split(","))
                .filter(s -> s.contains("="))
                .map(s -> {
                    String[] split = s.split("=");
                    return split.length != 2
                            ? ""
                            : String.format("'%s'='%s',", split[0], split[1]);
                })
                .collect(Collectors.joining("\n"))
                .trim();
        //language=SQL
        return String.format("" +
                        "CREATE TABLE reports(  \n" +
                        "    account_id BIGINT,  \n" +
                        "    log_ts     TIMESTAMP(3),  \n" +

                        "    amount_min     INT  \n," +
                        "    amount_sum     BIGINT  \n," +
                        "    amount_avg     DOUBLE  \n," +
                        (useStrings ? "    amount_max     STRING  \n," : "") +

                        "    amount_min1     INT  \n," +
                        "    amount_sum1     BIGINT  \n," +
                        "    amount_avg1     DOUBLE  \n," +
                        (useStrings ? "    amount_max1     STRING  \n," : "") +

                        "    amount_min2     INT  \n," +
                        "    amount_sum2     BIGINT  \n," +
                        "    amount_avg2     DOUBLE  \n," +
                        (useStrings ? "    amount_max2     STRING  \n," : "") +

                        "    amount_min3     INT  \n," +
                        "    amount_sum3     BIGINT  \n," +
                        "    amount_avg3     DOUBLE  \n," +
                        (useStrings ? "    amount_max3     STRING  \n," : "") +

                        "    amount_min4     INT  \n," +
                        "    amount_sum4     BIGINT  \n," +
                        "    amount_avg4     DOUBLE  \n," +
                        (useStrings ? "    amount_max4     STRING  \n," : "") +

                        "    amount_min5     INT  \n," +
                        "    amount_sum5     BIGINT  \n," +
                        "    amount_avg5     DOUBLE  \n," +
                        (useStrings ? "    amount_max5     STRING  \n," : "") +

                        "    amount_min6     INT  \n," +
                        "    amount_sum6     BIGINT  \n," +
                        "    amount_avg6     DOUBLE  \n," +
                        (useStrings ? "    amount_max6     STRING  \n," : "") +

                        "    amount_min7     INT  \n," +
                        "    amount_sum7     BIGINT  \n," +
                        "    amount_avg7     DOUBLE  \n," +
                        (useStrings ? "    amount_max7     STRING  \n," : "") +

                        "    amount_min8     INT  \n," +
                        "    amount_sum8     BIGINT  \n," +
                        "    amount_avg8     DOUBLE  \n," +
                        (useStrings ? "    amount_max8     STRING  \n," : "") +

                        "    amount_min9     INT  \n," +
                        "    amount_sum9     BIGINT  \n," +
                        "    amount_avg9     DOUBLE  \n," +
                        (useStrings ? "    amount_max9     STRING  \n," : "") +

                        "    PRIMARY KEY (account_id, log_ts) NOT ENFORCED  \n" +
                        ") WITH (  \n" +
                        "    'connector' = 'hudi',  \n" +
                        "    'path' = '%s',  \n" +
                        "    'table.type' = '%s',  \n" +
                        "    'metadata.enabled' = 'true',  \n" +
                        "    'hoodie.metadata.index.column.stats.enable' = 'true',  \n" +
                        (useStrings ? "    'hoodie.metadata.index.column.stats.column.list' = 'amount_min,amount_sum,amount_avg,amount_max',  \n" : "    'hoodie.metadata.index.column.stats.column.list' = 'amount_min,amount_sum,amount_avg',  \n") +
                        "    'read.data.skipping.enabled' = 'true',  \n" +
                        options + "\n" +
                        "    'hoodie.table.name' = 'reports'  \n" +
                        ")",
                tablePath,
                tableType
        );
    }

    public static org.apache.hadoop.conf.Configuration getHadoopConf() {
        // create hadoop configuration with hadoop conf directory configured.
        org.apache.hadoop.conf.Configuration hadoopConf = null;
        String[] strings = possibleHadoopConfPaths(new org.apache.flink.configuration.Configuration());
        System.out.println("PossiblePaths: " + Arrays.toString(strings));
        for (String possibleHadoopConfPath : strings) {
            hadoopConf = getHadoopConfiguration(possibleHadoopConfPath);
            if (hadoopConf != null) {
                System.out.println("Break: " + possibleHadoopConfPath);
                break;
            }
        }
        if (hadoopConf == null) {
            hadoopConf = new org.apache.hadoop.conf.Configuration();
        }
        return hadoopConf;
    }

    public static String[] possibleHadoopConfPaths(
            org.apache.flink.configuration.Configuration flinkConfiguration) {
        String[] possiblePaths = new String[4];
        possiblePaths[0] = flinkConfiguration.getString(ConfigConstants.PATH_HADOOP_CONFIG, null);
        System.out.println("ConfigConstants.PATH_HADOOP_CONFIG: " + possiblePaths[0]);
        possiblePaths[1] = System.getenv("HADOOP_CONF_DIR");
        System.out.println("HADOOP_CONF_DIR: " + possiblePaths[1]);

        System.out.println("HADOOP_HOME: " + System.getenv("HADOOP_HOME"));
        if (System.getenv("HADOOP_HOME") != null) {
            possiblePaths[2] = System.getenv("HADOOP_HOME") + "/conf";
            possiblePaths[3] = System.getenv("HADOOP_HOME") + "/etc/hadoop"; // hadoop 2.2
            System.out.println("HADOOP_HOME:" + possiblePaths[3]);
        }
        return Arrays.stream(possiblePaths).filter(Objects::nonNull).toArray(String[]::new);
    }

    private static org.apache.hadoop.conf.Configuration getHadoopConfiguration(String hadoopConfDir) {
        if (new File(hadoopConfDir).exists()) {
            org.apache.hadoop.conf.Configuration hadoopConfiguration = new org.apache.hadoop.conf.Configuration();
            File coreSite = new File(hadoopConfDir, "core-site.xml");
            if (coreSite.exists()) {
                Path p = new Path(coreSite.getAbsolutePath());
                System.out.println("Loading file: " + p);
                hadoopConfiguration.addResource(p);
            }
            File hdfsSite = new File(hadoopConfDir, "hdfs-site.xml");
            if (hdfsSite.exists()) {
                Path p = new Path(hdfsSite.getAbsolutePath());
                System.out.println("Loading file: " + p);
                hadoopConfiguration.addResource(p);
            }
            File yarnSite = new File(hadoopConfDir, "yarn-site.xml");
            if (yarnSite.exists()) {
                Path p = new Path(yarnSite.getAbsolutePath());
                System.out.println("Loading file: " + p);
                hadoopConfiguration.addResource(p);
            }
            // Add mapred-site.xml. We need to read configurations like compression codec.
            File mapredSite = new File(hadoopConfDir, "mapred-site.xml");
            if (mapredSite.exists()) {
                Path p = new Path(mapredSite.getAbsolutePath());
                System.out.println("Loading file: " + p);
                hadoopConfiguration.addResource(p);
            }
            return hadoopConfiguration;
        }
        return null;
    }
}
