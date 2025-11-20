package com.stream.realtime.lululemon.Two_stream_join.First_version;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.UUID;

/**
 * 时间窗口 Join 处理器 - 修复 JMX 重复注册问题
 */
public class WindowedCommentLogJoiner {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(5000);

        // 生成唯一的消费者组ID
        String uniqueGroupId = "window-joiner-" + UUID.randomUUID().toString().substring(0, 8);
        System.out.println("使用消费者组ID: " + uniqueGroupId);

        // 构建 Kafka 数据源 - 使用自定义属性避免 JMX 冲突
        KafkaSource<String> commentSource = buildKafkaSourceWithUniqueClient(
                "172.17.55.4:9092",
                "comment-security-results",
                uniqueGroupId,
                OffsetsInitializer.latest()
        );

        KafkaSource<String> logsSource = buildKafkaSourceWithUniqueClient(
                "172.17.55.4:9092",
                "realtime_v3_logs",
                uniqueGroupId,
                OffsetsInitializer.latest()
        );

        // 读取原始 JSON 数据流
        DataStream<String> commentJsonStream = env.fromSource(
                commentSource,
                WatermarkStrategy.noWatermarks(),
                "comment-security-source"
        ).name("comment-json-stream");

        DataStream<String> logsJsonStream = env.fromSource(
                logsSource,
                WatermarkStrategy.noWatermarks(),
                "realtime-logs-source"
        ).name("logs-json-stream");

        // 先输出两个表的原始 JSON 数据
        commentJsonStream
                .map(json -> {
                    System.out.println("🔵 ===== COMMENT 表原始数据 =====");
                    System.out.println("🔵 " + json);
                    System.out.println("🔵 ==============================");
                    return json;
                })
                .name("comment-json-printer");

        logsJsonStream
                .map(json -> {
                    System.out.println("🟢 ===== LOG 表原始数据 =====");
                    System.out.println("🟢 " + json);
                    System.out.println("🟢 =========================");
                    return json;
                })
                .name("logs-json-printer");

        // 解析数据流用于 Join
        DataStream<SecurityComment> commentStream = commentJsonStream
                .map(new CommentParser())
                .filter(comment -> comment != null)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<SecurityComment>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.ts != null ? event.ts : System.currentTimeMillis())
                )
                .name("comment-stream");

        DataStream<RealtimeLog> logsStream = logsJsonStream
                .map(new LogParser())
                .filter(log -> log != null)
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<RealtimeLog>forMonotonousTimestamps()
                                .withTimestampAssigner((event, timestamp) -> event.ts != null ? event.ts : System.currentTimeMillis())
                )
                .name("logs-stream");

        // 使用 Interval Join（时间间隔 Join）
        DataStream<JoinedResult> joinedStream = commentStream
                .keyBy(comment -> comment.orderId)
                .intervalJoin(logsStream.keyBy(log -> log.orderId))
                .between(Time.minutes(-10), Time.minutes(5))
                .process(new CommentLogJoinProcessor())
                .name("comment-log-joiner");

        // 输出 Join 结果到控制台
        joinedStream
                .map(result -> {
                    String json = result.toJson();
                    System.out.println("🎯 ===== JOIN 结果 =====");
                    System.out.println("🎯 " + json);
                    System.out.println("🎯 =====================");
                    return json;
                })
                .name("join-result-printer");

        System.out.println("启动时间窗口 Join 程序...");
        System.out.println("关联条件: order_id");
        System.out.println("时间窗口: comment 时间前后10分钟");
        System.out.println("输入主题: comment-security-results, realtime_v3_logs");
        System.out.println("消费者组ID: " + uniqueGroupId);

        env.execute("Windowed Comment-Log Joiner");
    }

    /**
     * 构建 Kafka 源，避免 JMX 冲突
     */
    private static KafkaSource<String> buildKafkaSourceWithUniqueClient(String bootServerList,
                                                                        String kafkaTopic,
                                                                        String group,
                                                                        OffsetsInitializer offset) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", bootServerList);
        props.setProperty("group.id", group);

        // 禁用 JMX 和指标收集
        props.setProperty("enable.metrics", "false");
        props.setProperty("metrics.sample.window.ms", "0");

        // 使用唯一的 client.id
        String uniqueClientId = group + "-" + kafkaTopic + "-" + System.currentTimeMillis();
        props.setProperty("client.id", uniqueClientId);

        return KafkaSource.<String>builder()
                .setBootstrapServers(bootServerList)
                .setTopics(kafkaTopic)
                .setGroupId(group)
                .setStartingOffsets(offset)
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(props)
                .setProperty("flink.partition-discovery.interval-millis", String.valueOf(10 * 1000))
                .build();
    }

    // CommentParser, LogParser, CommentLogJoinProcessor 保持不变
    public static class CommentParser implements MapFunction<String, SecurityComment> {
        @Override
        public SecurityComment map(String json) throws Exception {
            try {
                System.out.println("🔵 开始解析 COMMENT JSON: " + json);

                SecurityComment comment = SecurityComment.fromJson(json);
                if (comment.orderId != null && comment.ts != null) {
                    System.out.println("🔵 解析成功 - OrderID: " + comment.orderId +
                            ", 时间: " + comment.ts +
                            ", 敏感级别: " + comment.sensitiveLevel);
                    return comment;
                } else {
                    System.out.println("⚠️ 评论数据缺少必要字段，跳过处理");
                    return null;
                }
            } catch (Exception e) {
                System.err.println("❌ 解析安全评论数据失败: " + json);
                return null;
            }
        }
    }

    public static class LogParser implements MapFunction<String, RealtimeLog> {
        @Override
        public RealtimeLog map(String json) throws Exception {
            try {
                System.out.println("🟢 开始解析 LOG JSON: " + json);

                RealtimeLog log = RealtimeLog.fromJson(json);
                if (log.orderId != null && log.ts != null) {
                    System.out.println("🟢 解析成功 - OrderID: " + log.orderId +
                            ", 时间: " + log.ts +
                            ", IP: " + log.getIpAddress());
                    return log;
                } else {
                    System.out.println("⚠️ 日志数据缺少必要字段，跳过处理");
                    return null;
                }
            } catch (Exception e) {
                System.err.println("❌ 解析实时日志数据失败: " + json);
                return null;
            }
        }
    }

    public static class CommentLogJoinProcessor
            extends ProcessJoinFunction<SecurityComment, RealtimeLog, JoinedResult> {

        @Override
        public void processElement(SecurityComment comment, RealtimeLog log,
                                   Context ctx, Collector<JoinedResult> out) throws Exception {

            System.out.println("🎯 ===== 找到匹配数据 =====");
            System.out.println("🎯 Comment 数据:");
            System.out.println("🎯   OrderID: " + comment.orderId);
            System.out.println("🎯   时间: " + comment.ts);
            System.out.println("🎯   敏感级别: " + comment.sensitiveLevel);
            System.out.println("🎯   评论内容: " + comment.userComment);
            System.out.println("🎯 Log 数据:");
            System.out.println("🎯   OrderID: " + log.orderId);
            System.out.println("🎯   时间: " + log.ts);
            System.out.println("🎯   IP: " + log.getIpAddress());
            System.out.println("🎯   LogID: " + log.logId);

            // 将 log 的 IP 地址添加到 comment 中
            comment.ipAddress = log.getIpAddress();

            // 输出 enriched comment
            out.collect(new JoinedResult(comment));

            System.out.println("✅ 成功添加 IP 地址: " + comment.ipAddress);
            System.out.println("🎯 =======================");
        }
    }
}