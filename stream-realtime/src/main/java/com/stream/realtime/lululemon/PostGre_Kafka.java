package com.stream.realtime.lululemon;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// 维度表到kfk

public class PostGre_Kafka {
    @SneakyThrows
    public static void main(String[] args) {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE postgres_source (\n" +
                "    ts BIGINT,\n" +
                "    id BIGINT,\n" +
                "    user_id STRING,\n" +
                "    uname STRING,\n" +
                "    phone_num STRING,\n" +
                "    birthday DATE,\n" +
                "    gender INT,\n" +
                "    address STRING,\n" +
                "    age_group STRING,\n" +
                "    constellation STRING,\n" +
                "    PRIMARY KEY (ts, id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'postgres-cdc',\n" +
                "    'hostname' = '172.17.55.4',\n" +
                "    'port' = '5432',\n" +
                "    'username' = 'postgres',\n" +
                "    'password' = 'Hth1028,./',\n" +
                "    'database-name' = 'spider_db',\n" +
                "    'schema-name' = 'public',\n" +
                "    'table-name' = 'user_info_enhanced',\n" +
                "    'decoding.plugin.name' = 'pgoutput',\n" +
                "    'slot.name' = 'flink_cdc_slot'\n" +
                ");");


        tableEnv.executeSql("CREATE TABLE kafka_sink (\n" +
                "    ts BIGINT,\n" +
                "    id BIGINT,\n" +
                "    user_id STRING,\n" +
                "    uname STRING,\n" +
                "    phone_num STRING,\n" +
                "    birthday DATE,\n" +
                "    gender INT,\n" +
                "    address STRING,\n" +
                "    age_group STRING,\n" +
                "    constellation STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic' = 'user_info_enhanced',\n" +
                "    'properties.bootstrap.servers' = '172.17.55.4:9092',\n" +
                "    'format' = 'canal-json'\n" +
                ");");



        tableEnv.executeSql("INSERT INTO kafka_sink \n" +
                "SELECT * FROM postgres_source;");




        // 执行作业
        env.execute("PostGre_Hbase");
    }
}