package com.stream.realtime.lululemon.Data_source;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink_Doris_SQL {
    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment Tenv = StreamTableEnvironment.create(env);

        Tenv.executeSql(
                "CREATE TABLE t_kafka_gmv_topn_source ( " +
                        "  order_date           STRING, " +
                        "  window_start         STRING, " +
                        "  window_end           STRING, " +
                        "  gmv                  STRING, " +
                        "  top5_ids             STRING, " +
                        "  top5_product_ids     STRING " +
                        ") WITH ( " +
                        "  'connector' = 'kafka', " +
                        "  'topic' = 'gmv_topn_topic', " +
                        "  'properties.bootstrap.servers' = '172.17.55.4:9092', " +
                        "  'properties.group.id' = 'flink_doris_consumer', " +
                        "  'scan.startup.mode' = 'earliest-offset', " +
                        "  'format' = 'json', " +
                        "  'json.ignore-parse-errors' = 'true' " +
                        ")"
        );

//        Tenv.executeSql("select * from t_kafka_gmv_topn_source").print();

        Tenv.executeSql(
                "CREATE TABLE report_gmv_topn ( " +
                        "  order_date           STRING, " +
                        "  window_start_time    STRING, " +
                        "  window_end_time      STRING, " +
                        "  gmv_total            STRING, " +
                        "  top_user_ids         STRING, " +
                        "  top_product_ids      STRING, " +
                        "  ds                   DATE " + // ✅ Doris 分区字段
                        ") WITH ( " +
                        "  'connector' = 'doris', " +
                        "  'fenodes' = '192.168.200.31:18030', " +
                        "  'table.identifier' = 'biggta_realtime_report_v3.report_gmv_topn', " +
                        "  'username' = 'root', " +
                        "  'password' = '', " +
                        "  'sink.label-prefix' = 'gmv_topn_', " +
                        "  'sink.enable-2pc' = 'false', " + // 避免事务提交等待
                        "  'sink.buffer-flush.interval' = '2s', " + // 2秒刷一次
                        "  'sink.properties.format' = 'json', " +
                        "  'sink.properties.strip_outer_array' = 'true' " +
                        ")"
        );





        Tenv.executeSql(
                "INSERT INTO report_gmv_topn " +
                        "SELECT " +
                        "  order_date, " +
                        "  window_start AS window_start_time, " +
                        "  window_end AS window_end_time, " +
                        "  gmv AS gmv_total, " +
                        "  top5_ids AS top_user_ids, " +
                        "  top5_product_ids AS top_product_ids, " +
                        "  TO_DATE(window_end) AS ds " +
                        "FROM t_kafka_gmv_topn_source"
        ).await();



        System.out.println("✅ 写入 Doris 完成，请到 Doris 中查询结果");




        env.execute("Flink_Doris_SQL");
    }
}
