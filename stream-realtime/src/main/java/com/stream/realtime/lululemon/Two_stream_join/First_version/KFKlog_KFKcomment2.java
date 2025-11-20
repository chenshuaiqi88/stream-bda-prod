package com.stream.realtime.lululemon.Two_stream_join.First_version;

import com.stream.core.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import com.alibaba.fastjson2.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * 双流 CoProcessFunction Join 处理器 - 基于 UserID 连接
 * 2表联查
 */
public class KFKlog_KFKcomment2 {

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取 comment-security-results 主题
        KafkaSource<String> commentSource = KafkaUtils.buildKafkaSource(
                "172.17.55.4:9092",
                "comment-security-results",
                "flink-comment-reader",
                OffsetsInitializer.earliest()
        );

        // 读取 realtime_v3_logs 主题
        KafkaSource<String> logsSource = KafkaUtils.buildKafkaSource(
                "172.17.55.4:9092",
                "realtime_v3_logs",
                "flink-logs-reader",
                OffsetsInitializer.earliest()
        );

        // 处理 comment-security-results 数据
        DataStream<String> commentStream = env.fromSource(
                commentSource,
                WatermarkStrategy.noWatermarks(),
                "comment-security-source"
        );

        // 处理 realtime_v3_logs 数据
        DataStream<String> logsStream = env.fromSource(
                logsSource,
                WatermarkStrategy.noWatermarks(),
                "realtime-logs-source"
        );

        // 输出原始数据用于调试
        commentStream
                .map(record -> {
                    System.out.println("🔵 [comment-security-results] " + record);
                    return record;
                })
                .name("comment-processor");

        logsStream
                .map(record -> {
                    System.out.println("🟢 [realtime_v3_logs] " + record);
                    return record;
                })
                .name("logs-processor");

        // 解析数据流
        DataStream<CommentData> parsedCommentStream = commentStream
                .map(new CommentParser())
                .filter(comment -> comment != null)
                .name("parsed-comment-stream");

        DataStream<LogData> parsedLogStream = logsStream
                .map(new LogParser())
                .filter(log -> log != null)
                .name("parsed-log-stream");

        // 使用 CoProcessFunction 进行连接 - 基于 userId
        SingleOutputStreamOperator<JoinResult> joinedStream = parsedCommentStream
                .keyBy(comment -> comment.userId)
                .connect(parsedLogStream.keyBy(log -> log.userId))
                .process(new CommentLogCoProcessFunction())
                .name("co-process-join");

        // 输出 Join 结果
        joinedStream
                .map(result -> {
                    String json = result.toJson();
                    System.out.println("🎯 ===== JOIN 结果 =====");
                    System.out.println("🎯 " + json);
                    System.out.println("🎯 =====================");
                    return json;
                })
                .name("join-result-printer");

        env.execute("KFKlog_KFKcomment with CoProcessFunction - UserID Join");
    }

    /**
     * Comment 数据模型
     */
    public static class CommentData {
        public String orderId;
        public String userId;
        public String sensitiveLevel;
        public Boolean isBlocked;
        public Integer banDays;
        public String triggeredKeyword;
        public Double totalAmount;
        public String consumptionLevel;
        public String userComment;
        public String ds;
        public Long ts;
        public String productId;

        public CommentData(String orderId, String userId, String sensitiveLevel, Boolean isBlocked,
                           Integer banDays, String triggeredKeyword, Double totalAmount,
                           String consumptionLevel, String userComment, String ds, Long ts, String productId) {
            this.orderId = orderId;
            this.userId = userId;
            this.sensitiveLevel = sensitiveLevel;
            this.isBlocked = isBlocked;
            this.banDays = banDays;
            this.triggeredKeyword = triggeredKeyword;
            this.totalAmount = totalAmount;
            this.consumptionLevel = consumptionLevel;
            this.userComment = userComment;
            this.ds = ds;
            this.ts = ts;
            this.productId = productId;
        }
    }

    /**
     * Log 数据模型
     */
    public static class LogData {
        public String orderId;
        public String userId;
        public String productId;
        public String logId;
        public Long ts;
        public String ipAddress;

        public LogData(String orderId, String userId, String productId, String logId, Long ts, String ipAddress) {
            this.orderId = orderId;
            this.userId = userId;
            this.productId = productId;
            this.logId = logId;
            this.ts = ts;
            this.ipAddress = ipAddress;
        }
    }

    /**
     * Join 结果模型
     */
    public static class JoinResult {
        public CommentData comment;
        public LogData log;
        public Long joinTime;

        public JoinResult(CommentData comment, LogData log) {
            this.comment = comment;
            this.log = log;
            this.joinTime = System.currentTimeMillis();
        }

        public String toJson() {
            JSONObject json = new JSONObject();
            json.put("join_time", joinTime);

            if (comment != null) {
                JSONObject commentJson = new JSONObject();
                commentJson.put("order_id", comment.orderId);
                commentJson.put("user_id", comment.userId);
                commentJson.put("sensitive_level", comment.sensitiveLevel);
                commentJson.put("is_blocked", comment.isBlocked);
                commentJson.put("ban_days", comment.banDays);
                commentJson.put("triggered_keyword", comment.triggeredKeyword);
                commentJson.put("total_amount", comment.totalAmount);
                commentJson.put("consumption_level", comment.consumptionLevel);
                commentJson.put("user_comment", comment.userComment);
                commentJson.put("ds", comment.ds);
                commentJson.put("ts", comment.ts);
                commentJson.put("product_id", comment.productId);
                json.put("comment", commentJson);
            }

            if (log != null) {
                JSONObject logJson = new JSONObject();
                logJson.put("order_id", log.orderId);
                logJson.put("user_id", log.userId);
                logJson.put("product_id", log.productId);
                logJson.put("log_id", log.logId);
                logJson.put("ts", log.ts);
                logJson.put("ip_address", log.ipAddress);
                json.put("log", logJson);
            }

            return json.toJSONString();
        }
    }

    /**
     * Comment 数据解析器
     */
    public static class CommentParser implements MapFunction<String, CommentData> {
        @Override
        public CommentData map(String json) throws Exception {
            try {
                JSONObject jsonObject = JSONObject.parseObject(json);
                String orderId = jsonObject.getString("order_id");
                String userId = jsonObject.getString("user_id");
                String sensitiveLevel = jsonObject.getString("sensitive_level");
                Boolean isBlocked = jsonObject.getBoolean("is_blocked");
                Integer banDays = jsonObject.getInteger("blacklist_duration_days");
                String triggeredKeyword = jsonObject.getString("triggered_keyword");
                Double totalAmount = jsonObject.getDouble("total_amount");
                String consumptionLevel = jsonObject.getString("consumption_level");
                String userComment = jsonObject.getString("user_comment");
                String ds = jsonObject.getString("ds");
                Long ts = jsonObject.getLong("ts");
                String productId = jsonObject.getString("product_id");

                System.out.println("🔵 解析 Comment - UserID: " + userId + ", OrderID: " + orderId + ", 时间: " + ts);
                return new CommentData(orderId, userId, sensitiveLevel, isBlocked, banDays,
                        triggeredKeyword, totalAmount, consumptionLevel,
                        userComment, ds, ts, productId);
            } catch (Exception e) {
                System.err.println("❌ 解析 Comment 数据失败: " + json);
                return null;
            }
        }
    }

    /**
     * Log 数据解析器
     */
    public static class LogParser implements MapFunction<String, LogData> {
        @Override
        public LogData map(String json) throws Exception {
            try {
                System.out.println("🟢 开始解析 Log 数据: " + json);

                JSONObject jsonObject = JSONObject.parseObject(json);
                String orderId = jsonObject.getString("order_id");
                String userId = jsonObject.getString("user_id");
                String productId = jsonObject.getString("product_id");
                String logId = jsonObject.getString("log_id");

                // 处理带小数点的 timestamp
                Long ts;
                Object tsObj = jsonObject.get("ts");
                if (tsObj instanceof Double) {
                    ts = ((Double) tsObj).longValue();
                } else if (tsObj instanceof Float) {
                    ts = ((Float) tsObj).longValue();
                } else {
                    ts = jsonObject.getLong("ts");
                }

                // 解析嵌套的 IP 地址
                String ipAddress = null;
                if (jsonObject.containsKey("gis")) {
                    JSONObject gis = jsonObject.getJSONObject("gis");
                    if (gis != null && gis.containsKey("ip")) {
                        ipAddress = gis.getString("ip");
                    }
                }

                System.out.println("🟢 解析 Log 成功 - UserID: " + userId +
                        ", OrderID: " + orderId +
                        ", 时间: " + ts +
                        ", IP: " + ipAddress);
                return new LogData(orderId, userId, productId, logId, ts, ipAddress);
            } catch (Exception e) {
                System.err.println("❌ 解析 Log 数据失败: " + json);
                System.err.println("❌ 错误信息: " + e.getMessage());
                e.printStackTrace();
                return null;
            }
        }
    }

    /**
     * Comment-Log CoProcessFunction 连接处理器 - 基于 UserID
     */
    public static class CommentLogCoProcessFunction extends CoProcessFunction<CommentData, LogData, JoinResult> {

        private transient ValueState<CommentData> commentState;
        private transient ValueState<List<LogData>> logsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // 初始化 Comment 状态
            ValueStateDescriptor<CommentData> commentDescriptor =
                    new ValueStateDescriptor<>("commentState", CommentData.class);
            commentState = getRuntimeContext().getState(commentDescriptor);

            // 初始化 Logs 状态 - 使用 TypeHint 解决泛型问题
            ValueStateDescriptor<List<LogData>> logsDescriptor =
                    new ValueStateDescriptor<>("logsState", TypeInformation.of(new TypeHint<List<LogData>>() {}));
            logsState = getRuntimeContext().getState(logsDescriptor);
        }

        @Override
        public void processElement1(CommentData comment, Context ctx, Collector<JoinResult> out) throws Exception {
            System.out.println("🔵 处理 Comment - UserID: " + comment.userId +
                    ", OrderID: " + comment.orderId +
                    ", 时间: " + comment.ts);

            // 检查当前状态中已有的 Comment
            CommentData existingComment = commentState.value();
            if (existingComment != null) {
                System.out.println("🔵 状态中已有 Comment - UserID: " + existingComment.userId);
            }

            // 存储 Comment 到状态
            commentState.update(comment);

            // 检查 Log 状态
            List<LogData> logs = logsState.value();
            if (logs != null) {
                System.out.println("🔵 当前 Logs 状态数量: " + logs.size());
                for (LogData log : logs) {
                    System.out.println("🔵 状态中 Log - UserID: " + log.userId + ", OrderID: " + log.orderId);
                }
            } else {
                System.out.println("🔵 当前 Logs 状态为空");
            }

            // 检查是否有匹配的 Log 记录（基于 userId）
            if (logs != null && !logs.isEmpty()) {
                for (LogData log : logs) {
                    if (comment.userId.equals(log.userId)) {
                        System.out.println("🎯 ===== CO-PROCESS JOIN 成功 (Comment触发) =====");
                        System.out.println("🎯 Comment UserID: " + comment.userId + " | OrderID: " + comment.orderId + " | 时间: " + comment.ts);
                        System.out.println("🎯 Log UserID: " + log.userId + " | OrderID: " + log.orderId + " | 时间: " + log.ts + " | IP: " + log.ipAddress);
                        System.out.println("🎯 时间差: " + (comment.ts - log.ts) + "ms");
                        out.collect(new JoinResult(comment, log));
                    }
                }
            }
        }

        @Override
        public void processElement2(LogData log, Context ctx, Collector<JoinResult> out) throws Exception {
            System.out.println("🟢 处理 Log - UserID: " + log.userId +
                    ", OrderID: " + log.orderId +
                    ", 时间: " + log.ts);

            // 存储 Log 到状态（添加到列表）
            List<LogData> logs = logsState.value();
            if (logs == null) {
                logs = new ArrayList<>();
                System.out.println("🟢 创建新的 Logs 列表");
            } else {
                System.out.println("🟢 现有 Logs 列表大小: " + logs.size());
            }
            logs.add(log);
            logsState.update(logs);

            // 检查是否有匹配的 Comment 记录（基于 userId）
            CommentData comment = commentState.value();
            if (comment != null) {
                System.out.println("🟢 状态中 Comment - UserID: " + comment.userId + ", OrderID: " + comment.orderId);
                if (comment.userId.equals(log.userId)) {
                    System.out.println("🎯 ===== CO-PROCESS JOIN 成功 (Log触发) =====");
                    System.out.println("🎯 Comment UserID: " + comment.userId + " | OrderID: " + comment.orderId + " | 时间: " + comment.ts);
                    System.out.println("🎯 Log UserID: " + log.userId + " | OrderID: " + log.orderId + " | 时间: " + log.ts + " | IP: " + log.ipAddress);
                    System.out.println("🎯 时间差: " + (comment.ts - log.ts) + "ms");
                    out.collect(new JoinResult(comment, log));
                } else {
                    System.out.println("❌ UserID 不匹配 - Comment: " + comment.userId + ", Log: " + log.userId);
                }
            } else {
                System.out.println("🟢 状态中无 Comment 数据");
            }
        }
    }
}