package com.stream.realtime.lululemon;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


// 将postgresql里的用户表 添加其他属性 生成一个新的表
public class PostgreSQLUserInfoCDC3 {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 删除已存在的表
        tableEnv.executeSql("DROP TABLE IF EXISTS user_info_base_source");
        tableEnv.executeSql("DROP TABLE IF EXISTS user_info_insert_only");
        tableEnv.executeSql("DROP TABLE IF EXISTS user_info_enhanced");

        // 创建源表
        tableEnv.executeSql(
                "CREATE TABLE user_info_base_source (\n" +
                        "  id INT,\n" +
                        "  user_id STRING,\n" +
                        "  uname STRING,\n" +
                        "  phone_num STRING,\n" +
                        "  birthday STRING,\n" +  // 或者使用 DATE 类型
                        "  gender INT,\n" +
                        "  address STRING,\n" +
                        "   ts STRING  ,\n" +
                        "  PRIMARY KEY(id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "  'connector' = 'postgres-cdc',\n" +
                        "  'hostname' = '172.17.55.4',\n" +
                        "  'port' = '5432',\n" +
                        "  'username' = 'postgres',\n" +
                        "  'password' = 'Hth1028,./',\n" +
                        "  'database-name' = 'spider_db',\n" +
                        "  'schema-name' = 'public',\n" +
                        "  'table-name' = 'user_info_base',\n" +
                        "  'decoding.plugin.name' = 'pgoutput',\n" +
                        "  'slot.name' = 'flink_user_slot',\n" +
                        "  'debezium.snapshot.mode' = 'initial'\n" +
                        ")"
        );








        // 创建目标表
        tableEnv.executeSql(
                "CREATE TABLE user_info_enhanced (\n" +
                        "  ts BIGINT,\n" +
                        "  id BIGINT,\n" +
                        "  user_id STRING,\n" +
                        "  uname STRING,\n" +
                        "  phone_num STRING,\n" +
                        "  birthday DATE,\n" +
                        "  gender INT,\n" +
                        "  address STRING,\n" +
                        "  age_group STRING,\n" +
                        "  constellation STRING,\n" +
                        "  PRIMARY KEY (ts, id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "  'connector' = 'jdbc',\n" +
                        "  'url' = 'jdbc:postgresql://172.17.55.4:5432/spider_db',\n" +
                        "  'table-name' = 'user_info_enhanced',\n" +
                        "  'username' = 'postgres',\n" +
                        "  'password' = 'Hth1028,./'\n" +
                        ")"
        );

        // 创建临时视图
        tableEnv.executeSql(
                "CREATE TEMPORARY VIEW user_info_insert_only AS\n" +
                        "SELECT \n" +
                        "    ts,\n" +
                        "    id,\n" +
                        "    user_id,\n" +
                        "    uname,\n" +
                        "    phone_num,\n" +
                        "    birthday,\n" +
                        "    gender,\n" +
                        "    address,\n" +
                        "    CASE\n" +
                        "      WHEN y BETWEEN 1960 AND 1969 THEN '60后'\n" +
                        "      WHEN y BETWEEN 1970 AND 1979 THEN '70后'\n" +
                        "      WHEN y BETWEEN 1980 AND 1989 THEN '80后'\n" +
                        "      WHEN y BETWEEN 1990 AND 1999 THEN '90后'\n" +
                        "      WHEN y BETWEEN 2000 AND 2009 THEN '00后'\n" +
                        "      ELSE '其他'\n" +
                        "    END AS age_group,\n" +
                        "    CASE\n" +
                        "      WHEN (m = 3 AND d >= 21) OR (m = 4 AND d <= 19) THEN '白羊'\n" +
                        "      WHEN (m = 4 AND d >= 20) OR (m = 5 AND d <= 20) THEN '金牛'\n" +
                        "      WHEN (m = 5 AND d >= 21) OR (m = 6 AND d <= 21) THEN '双子'\n" +
                        "      WHEN (m = 6 AND d >= 22) OR (m = 7 AND d <= 22) THEN '巨蟹'\n" +
                        "      WHEN (m = 7 AND d >= 23) OR (m = 8 AND d <= 22) THEN '狮子'\n" +
                        "      WHEN (m = 8 AND d >= 23) OR (m = 9 AND d <= 22) THEN '处女'\n" +
                        "      WHEN (m = 9 AND d >= 23) OR (m = 10 AND d <= 23) THEN '天秤'\n" +
                        "      WHEN (m = 10 AND d >= 24) OR (m = 11 AND d <= 22) THEN '天蝎'\n" +
                        "      WHEN (m = 11 AND d >= 23) OR (m = 12 AND d <= 21) THEN '射手'\n" +
                        "      WHEN (m = 12 AND d >= 22) OR (m = 1 AND d <= 19) THEN '摩羯'\n" +
                        "      WHEN (m = 1 AND d >= 20) OR (m = 2 AND d <= 18) THEN '水瓶'\n" +
                        "      WHEN (m = 2 AND d >= 19) OR (m = 3 AND d <= 20) THEN '双鱼'\n" +
                        "      ELSE '未知'\n" +
                        "    END AS constellation\n" +
                        "FROM (\n" +
                        "    SELECT \n" +
                        "           ts,\n" +
                        "           id,\n" +
                        "           user_id,\n" +
                        "           uname,\n" +
                        "           phone_num,\n" +
                        "           birthday,\n" +
                        "           gender,\n" +
                        "           address,\n" +
                        "           CAST(SUBSTRING(birthday, 1, 4) AS INT) AS y,\n" +
                        "           CAST(SUBSTRING(birthday, 6, 2) AS INT) AS m,\n" +
                        "           CAST(SUBSTRING(birthday, 9, 2) AS INT) AS d\n" +
                        "    FROM user_info_base_source\n" +
                        "    WHERE birthday LIKE '____-__-__'\n" +
                        "      AND CAST(SUBSTRING(birthday, 1, 4) AS INT) BETWEEN 1900 AND 2025\n" +
                        "      AND CAST(SUBSTRING(birthday, 6, 2) AS INT) BETWEEN 1 AND 12\n" +
                        "      AND CAST(SUBSTRING(birthday, 9, 2) AS INT) BETWEEN 1 AND 31\n" +
                        ") t"
        );

        // 插入数据到目标表
        tableEnv.executeSql(
                "INSERT INTO user_info_enhanced\n" +
                        "SELECT \n" +
                        "    CAST(ts AS BIGINT) as ts,\n" +
                        "    CAST(id AS BIGINT) as id,\n" +
                        "    user_id,\n" +
                        "    uname,\n" +
                        "    phone_num,\n" +
                        "    TO_DATE(birthday) as birthday,\n" +
                        "    gender,\n" +
                        "    address,\n" +
                        "    age_group,\n" +
                        "    constellation\n" +
                        "FROM user_info_insert_only"
        );





        env.execute("PostgreSQL User Info CDC");
    }
}