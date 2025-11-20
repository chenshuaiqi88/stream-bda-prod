package com.stream.realtime.lululemon;

import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * 从 SQLServer CDC 读取评论流
 * 1. 自动进行 GBK 解码
 * 2. 根据评论内容打标签（p1 / p2 / p3）
 * 增强：全流程增加日志，方便定位问题
 */
public class SqlServer_CDC {

    private static final Logger log = LoggerFactory.getLogger(SqlServer_CDC.class);

    /** 自定义函数：GBK 解码 */
    public static class GbkDecodeFunction extends ScalarFunction {
        private static final Logger log = LoggerFactory.getLogger(GbkDecodeFunction.class);
        public String eval(String input) {
            if (input == null) return null;
            try {
                byte[] bytes = input.getBytes(StandardCharsets.ISO_8859_1);
                String result = new String(bytes, "GBK");
                log.debug("[GbkDecodeFunction] 解码前:{} , 解码后:{}", input, result);
                return result;
            } catch (Exception e) {
                log.error("[GbkDecodeFunction] 解码异常,原始字符串:{}", input, e);
                return input;
            }
        }
    }

    /** 自定义函数：评论分级 */
    public static class CommentLevelFunction extends ScalarFunction {
        private static final Logger log = LoggerFactory.getLogger(CommentLevelFunction.class);
        public String eval(String comment) {
            if (comment == null) {
                log.debug("[CommentLevelFunction] 输入为null,返回默认p3");
                return "p3";
            }
            String c = comment.toLowerCase();
            // p1关键词
            String[] p1_keywords = {"政治", "反动", "色情", "黄赌毒", "毒品", "颠覆", "港独", "台独",
                    "反党", "猥亵", "建国", "台湾", "情妇"};
            for (String k : p1_keywords) {
                if (c.contains(k)) {
                    log.info("[CommentLevelFunction] 命中p1关键词:{}, 原始评论:{}", k, comment);
                    return "p1";
                }
            }
            // p2关键词
            String[] p2_keywords = {"傻逼", "妈的", "猪", "垃圾", "滚", "捅我", "狗", "废物", "你爹",
                    "智障", "sb", "操", "艹", "对线", "地狱笑话",
                    "东北人", "河南人", "山东人", "穷鬼"};
            for (String k : p2_keywords) {
                if (c.contains(k)) {
                    log.info("[CommentLevelFunction] 命中p2关键词:{}, 原始评论:{}", k, comment);
                    return "p2";
                }
            }
            log.debug("[CommentLevelFunction] 未命中任何敏感词,返回p3, 原始评论:{}", comment);
            return "p3";
        }
    }

    @SneakyThrows
    public static void main(String[] args) {
        System.out.println(">>> main() 被调用了");

        org.apache.logging.log4j.LogManager
                .getLogger(SqlServer_CDC.class)
                .info(">>> log4j2 info 输出测试");

        log.info("[SqlServer_CDC] ====== 开始启动 Flink SQLServer CDC 任务 ======");

        // 1. 环境初始化
        StreamExecutionEnvironment env;
        StreamTableEnvironment tableEnv;
        try {
            log.info("[SqlServer_CDC] 初始化 StreamExecutionEnvironment");
            env = StreamExecutionEnvironment.getExecutionEnvironment();
            env.setParallelism(1);

            log.info("[SqlServer_CDC] 初始化 StreamTableEnvironment");
            tableEnv = StreamTableEnvironment.create(env);
            log.info("[SqlServer_CDC] 环境初始化完成");
        } catch (Exception e) {
            log.error("[SqlServer_CDC] 环境初始化失败", e);
            throw e;
        }

        // 2. 注册自定义函数
        try {
            log.info("[SqlServer_CDC] 注册 UDF -> gbk_decode");
            tableEnv.createTemporaryFunction("gbk_decode", GbkDecodeFunction.class);

            log.info("[SqlServer_CDC] 注册 UDF -> comment_level");
            tableEnv.createTemporaryFunction("comment_level", CommentLevelFunction.class);
            log.info("[SqlServer_CDC] 全部 UDF 注册完成");
        } catch (Exception e) {
            log.error("[SqlServer_CDC] UDF 注册失败", e);
            throw e;
        }

        // 3. 创建 CDC 源表
        String sourceDDL = "CREATE TABLE orders_portrait_source (\n" +
                "    order_id STRING,\n" +
                "    user_id STRING,\n" +
                "    product_id STRING,\n" +
                "    total_amount STRING,\n" +
                "    product_name STRING,\n" +
                "    product_class STRING,\n" +
                "    ds STRING,\n" +
                "    ts STRING,\n" +
                "    `comment` STRING\n" +
                ") WITH (\n" +
                "    'connector' = 'sqlserver-cdc',\n" +
                "    'hostname' = 'localhost',\n" +
                "    'port' = '1433',\n" +
                "    'username' = 'sa',\n" +
                "    'password' = 'Hth1028,./',\n" +
                "    'database-name' = 'realtime_v3',\n" +
                "    'table-name' = 'dbo.orders_portrait_stream',\n" +
                "    'server-time-zone' = 'Asia/Shanghai',\n" +
                "    'scan.startup.mode' = 'initial',\n" +
                "    'scan.incremental.snapshot.enabled' = 'false'\n" +
                ")";
        try {
            log.info("[SqlServer_CDC] 开始执行 CDC 源表 DDL");
            tableEnv.executeSql(sourceDDL);
            log.info("[SqlServer_CDC] CDC 源表创建成功");
        } catch (Exception e) {
            log.error("[SqlServer_CDC] CDC 源表 DDL 执行失败", e);
            throw e;
        }

        // 4. 创建临时视图（解码+分级）
        String viewDDL = "CREATE TEMPORARY VIEW orders_decoded AS\n" +
                "SELECT \n" +
                "    order_id,\n" +
                "    user_id,\n" +
                "    product_id,\n" +
                "    total_amount,\n" +
                "    gbk_decode(product_name) AS product_name,\n" +
                "    gbk_decode(product_class) AS product_class,\n" +
                "    ds,\n" +
                "    ts,\n" +
                "    `comment`,\n" +
                "    comment_level(`comment`) AS comment_level\n" +
                "FROM orders_portrait_source";
        try {
            log.info("[SqlServer_CDC] 创建临时视图 orders_decoded");
            tableEnv.executeSql(viewDDL);
            log.info("[SqlServer_CDC] 临时视图创建成功");
        } catch (Exception e) {
            log.error("[SqlServer_CDC] 临时视图创建失败", e);
            throw e;
        }

        // 5. 创建 Doris Sink 表
        String sinkDDL = "CREATE TABLE doris_orders_sink (\n" +
                "  order_id STRING,\n" +
                "  user_id STRING,\n" +
                "  product_id STRING,\n" +
                "  ds BIGINT,\n" +
                "  ts BIGINT,\n" +
                "  `comment` STRING,\n" +
                "  comment_level STRING\n" +
                ") WITH (\n" +
                "  'connector' = 'doris',\n" +
                "  'fenodes' = '192.168.200.31:18030',\n" +
                "  'table.identifier' = 'bigdata_realtime_lululemon_report_v3.orders_portrait',\n" +
                "  'username' = 'root',\n" +
                "  'password' = '',\n" +
                "  'sink.buffer-count' = '5',\n" +
                "  'sink.buffer-size' = '10210',\n" +
                "  'sink.enable-2pc' = 'false',\n" +
                "  'sink.properties.format' = 'json',\n" +
                "  'sink.properties.read_json_by_line' = 'true'\n" +
                ")";
        try {
            log.info("[SqlServer_CDC] 创建 Doris Sink 表");
            tableEnv.executeSql(sinkDDL);
            log.info("[SqlServer_CDC] Doris Sink 表创建成功");
        } catch (Exception e) {
            log.error("[SqlServer_CDC] Doris Sink 表创建失败", e);
            throw e;
        }

        // 6. 写入 Doris
        try {
            log.info("[SqlServer_CDC] 开始从视图 orders_decoded 查询数据并写入 Doris");
            Table sourceTable = tableEnv.sqlQuery(
                    "SELECT order_id, user_id, product_id,\n" +
                            "       CAST(ds AS BIGINT) AS ds,\n" +
                            "       CAST(ts AS BIGINT) AS ts,\n" +
                            "       `comment`, comment_level\n" +
                            "FROM orders_decoded");
            sourceTable.executeInsert("doris_orders_sink");
            log.info("[SqlServer_CDC] 数据写入作业已提交，请在 Flink Web UI 查看运行情况");
        } catch (Exception e) {
            log.error("[SqlServer_CDC] 数据写入 Doris 失败", e);
            throw e;
        }




    }
}