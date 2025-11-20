package com.stream.realtime.lululemon.comment;

import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

// 新增Kafka相关导入
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * 评论处理器   输出
 */
public class CommentProcessor {

    public static void main(String[] args) throws Exception {
        // 加载敏感词库
        SecurityConfig.loadP0WordsFromFile("D:\\idea\\daima\\zg6\\stream-bda-prod\\stream-realtime\\src\\main\\java\\com\\stream\\realtime\\lululemon\\comment\\a.txt");

        // 设置Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(10000);

        // 配置CDC源 - 添加数据类型处理配置
        Properties props = new Properties();
        props.put("connect.timeout.ms", 10000);
        props.put("request.timeout.ms", 15000);
        props.put("heartbeat.interval.ms", 10000);
        props.put("snapshot.mode", "initial");
        props.put("database.history.store.only.monitored.tables.ddl", "true");
        props.put("snapshot.locking.mode", "none");
        props.put("snapshot.fetch.size", 200);
        props.put("snapshot.isolation.mode", "snapshot");
        props.put("signal.data.collection", "dbo.orders_portrait_stream2");

        // 添加数据类型处理配置
        props.put("decimal.handling.mode", "double"); // 将decimal类型转换为double
        props.put("binary.handling.mode", "base64");  // 正确处理二进制数据
        props.put("column.propagate.source.type", "dbo.orders_portrait_stream2.total_amount");

        SourceFunction<String> source = SqlServerSource.<String>builder()
                .hostname("172.17.55.4")
                .port(1433)
                .database("realtime_v3")
                .tableList("dbo.orders_portrait_stream2")
                .username("sa")
                .password("Hth1028,./")
                .debeziumProperties(props)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        // 处理流水线
        DataStream<ProcessResult> results = env
                .addSource(source)
                .name("sqlserver-cdc-source")
                .flatMap(new CommentJsonParser())
                .name("comment-parser")
                .keyBy(comment -> comment.userId)
                .process(new SensitiveCommentProcessor())
                .name("sensitive-processor");

        // 配置Kafka生产者属性
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "172.17.55.4:9092"); // 根据你的Kafka配置修改
        kafkaProps.setProperty("acks", "1");
        kafkaProps.setProperty("retries", "3");
        kafkaProps.setProperty("batch.size", "16384");
        kafkaProps.setProperty("linger.ms", "1");
        kafkaProps.setProperty("buffer.memory", "33554432");

        // 创建Kafka生产者
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "comment-security-results", // 主题名称
                new KafkaStringSerializationSchema(), // 序列化模式
                kafkaProps,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE
        );

        // 输出结果到Kafka
        DataStream<String> jsonStream = results.map(ProcessResult::toJson)
                .name("result-json");

        jsonStream.addSink(kafkaProducer)
                .name("kafka-sink");

        // 同时输出到控制台用于调试
        jsonStream.print();

        System.out.println("启动SQL Server评论监控程序...");
        System.out.println("监控表: dbo.orders_portrait_stream2");
        System.out.println("字段映射: order_id, user_id, product_id, total_amount, ds, ts, comment");
        System.out.println("输出到Kafka主题: comment-security-results");

        try {
            env.execute("SQLServer Comment Security Processor");
        } catch (Exception e) {
            System.err.println("程序执行失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Kafka序列化模式
    public static class KafkaStringSerializationSchema implements KafkaSerializationSchema<String> {
        @Override
        public ProducerRecord<byte[], byte[]> serialize(String element, Long timestamp) {
            return new ProducerRecord<>(
                    "comment-security-results", // 主题
                    null, // 分区（null表示使用默认分区策略）
                    element.getBytes() // 值
            );
        }
    }

    // JSON解析器 (保持不变)
    public static class CommentJsonParser extends RichFlatMapFunction<String, CommentRecord> {
        private transient ObjectMapper objectMapper;

        @Override
        public void open(Configuration parameters) {
            objectMapper = new ObjectMapper();
        }

        @Override
        public void flatMap(String json, Collector<CommentRecord> out) throws Exception {
            try {
                JsonNode jsonNode = objectMapper.readTree(json);
                String op = jsonNode.has("op") ? jsonNode.get("op").asText() : "unknown";
                JsonNode after = jsonNode.get("after");

                if (after != null && ("c".equals(op) || "u".equals(op) || "r".equals(op))) {
                    // 根据字段映射正确解析字段
                    String orderId = after.has("order_id") ? after.get("order_id").asText() : null;
                    String userId = after.has("user_id") ? after.get("user_id").asText() : null;
                    String productId = after.has("product_id") ? after.get("product_id").asText() : null;
                    String commentContent = after.has("comment") ? after.get("comment").asText() : null;

                    // 解析金额字段
                    Double totalAmount = parseAmountField(after);

                    // 解析时间字段
                    String ds = after.has("ds") ? after.get("ds").asText() : null;
                    Long ts = after.has("ts") ? after.get("ts").asLong() : System.currentTimeMillis();

                    // 调试信息：打印所有字段
                    System.out.println("=== 解析数据记录 ===");
                    System.out.println("操作类型: " + op);
                    System.out.println("订单ID: " + orderId);
                    System.out.println("用户ID: " + userId);
                    System.out.println("产品ID: " + productId);
                    System.out.println("评论内容: " + commentContent);
                    System.out.println("金额: " + totalAmount);
                    System.out.println("日期: " + ds);
                    System.out.println("时间戳: " + ts);

                    // 打印所有字段用于调试
                    Iterator<String> fieldNames = after.fieldNames();
                    while (fieldNames.hasNext()) {
                        String fieldName = fieldNames.next();
                        JsonNode fieldValue = after.get(fieldName);
                        System.out.println("字段 '" + fieldName + "': " + fieldValue);
                    }

                    // 只有评论内容不为空时才处理
                    if (commentContent != null && !commentContent.trim().isEmpty()) {
                        CommentRecord record = new CommentRecord(
                                userId,
                                orderId,
                                productId,      // 添加productId
                                commentContent,
                                ds,
                                ts,
                                totalAmount
                        );
                        out.collect(record);

                        System.out.println("✅ 成功解析记录 - 用户: " + userId +
                                ", 订单: " + orderId +
                                ", 金额: " + totalAmount +
                                ", 评论长度: " + commentContent.length());
                        System.out.println("=== 结束解析记录 ===\n");
                    } else {
                        System.out.println("⚠️ 跳过空评论记录");
                        System.out.println("=== 结束解析记录 ===\n");
                    }
                } else if (after == null) {
                    System.out.println("⚠️ after字段为空，可能是删除操作: " + op);
                }
            } catch (Exception e) {
                System.err.println("解析JSON失败: " + json);
                e.printStackTrace();
            }
        }

        // 解析金额字段 (保持不变)
        private Double parseAmountField(JsonNode after) {
            if (after.has("total_amount")) {
                JsonNode amountNode = after.get("total_amount");
                System.out.println("原始金额字段值: '" + amountNode.asText() + "' (类型: " + amountNode.getNodeType() + ")");

                if (!amountNode.isNull()) {
                    try {
                        if (amountNode.isNumber()) {
                            // 直接是数字
                            double amount = amountNode.asDouble();
                            System.out.println("✅ 直接解析为数字金额: " + amount);
                            return amount;
                        } else if (amountNode.isTextual()) {
                            String amountStr = amountNode.asText().trim();

                            // 检查是否是正常的数字
                            if (amountStr.matches("^-?\\d+(\\.\\d+)?$")) {
                                try {
                                    double amount = Double.parseDouble(amountStr);
                                    System.out.println("✅ 字符串解析为数字金额: " + amount);
                                    return amount;
                                } catch (NumberFormatException e) {
                                    System.out.println("❌ 数字格式错误: " + amountStr);
                                }
                            }

                            // 如果是类似 "AlmQ" 的编码值或空值
                            if (amountStr.isEmpty() || amountStr.equals("null")) {
                                System.out.println("金额字段为空");
                                return null;
                            }

                            if (amountStr.length() == 4 && !amountStr.matches("\\d+")) {
                                System.out.println("⚠️ 金额字段被编码，无法直接解析: " + amountStr);
                                // 尝试从评论内容中提取金额
                                return extractAmountFromComment(after.has("comment") ? after.get("comment").asText() : null);
                            }
                        }
                    } catch (Exception e) {
                        System.err.println("金额解析异常: " + e.getMessage());
                    }
                } else {
                    System.out.println("金额字段值为null");
                }
            } else {
                System.out.println("未找到total_amount字段");
            }
            return null;
        }

        // 从评论中提取金额 (保持不变)
        private Double extractAmountFromComment(String commentContent) {
            if (commentContent == null) return null;

            System.out.println("尝试从评论提取金额，内容: " + commentContent);

            // 多种金额匹配模式
            String[] patterns = {
                    // 模式1: 数字 + 货币单位 (如: 1540块, 5400元)
                    "(\\d{1,10}[.,]?\\d{0,2})\\s*(元|块|人民币|RMB|¥)",
                    // 模式2: 数字单独出现 (如: 1540块)
                    "(\\d{1,10}[.,]?\\d{0,2})\\s*元",
                    // 模式3: 价格相关词汇 + 数字
                    "(?:价格|售价|价值|花费|支付|买了|买来|花了)\\s*(\\d{1,10}[.,]?\\d{0,2})",
                    // 模式4: 数字 + 价格相关词汇
                    "(\\d{1,10}[.,]?\\d{0,2})\\s*(?:价格|售价|价值)",
                    // 模式5: 直接匹配大额数字 (通常评论中的金额都比较大)
                    "\\b(\\d{3,5}[.,]?\\d{0,2})\\b"
            };

            for (String patternStr : patterns) {
                try {
                    Pattern pattern = Pattern.compile(patternStr);
                    Matcher matcher = pattern.matcher(commentContent);

                    if (matcher.find()) {
                        String amountStr = "";

                        // 根据分组获取金额字符串
                        if (matcher.groupCount() >= 1) {
                            amountStr = matcher.group(1);
                        } else {
                            amountStr = matcher.group();
                        }

                        // 清理金额字符串
                        amountStr = amountStr.replace(",", "").replace("，", "").replace(" ", "");

                        try {
                            double amount = Double.parseDouble(amountStr);
                            System.out.println("✅ 使用模式匹配到金额: " + amount + " (模式: " + patternStr + ")");

                            // 验证金额合理性 (通常在100-10000之间)
                            if (amount >= 100 && amount <= 100000) {
                                return amount;
                            } else {
                                System.out.println("⚠️ 金额超出合理范围: " + amount);
                            }
                        } catch (NumberFormatException e) {
                            System.out.println("❌ 金额格式错误: " + amountStr);
                        }
                    }
                } catch (Exception e) {
                    System.err.println("正则表达式匹配异常: " + e.getMessage());
                }
            }

            // 特殊处理：直接搜索评论中的数字
            System.out.println("尝试直接搜索评论中的大额数字...");
            Pattern numberPattern = Pattern.compile("\\b(\\d{3,})\\b");
            Matcher numberMatcher = numberPattern.matcher(commentContent);

            while (numberMatcher.find()) {
                String numberStr = numberMatcher.group(1);
                try {
                    double amount = Double.parseDouble(numberStr);
                    // 检查是否是合理的金额 (通常在100以上)
                    if (amount >= 100 && amount <= 100000) {
                        System.out.println("✅ 直接搜索到合理金额: " + amount);
                        return amount;
                    }
                } catch (NumberFormatException e) {
                    // 忽略格式错误
                }
            }

            System.out.println("❌ 无法从评论中提取金额");
            return null;
        }
    }

    // 敏感评论处理器 (保持不变)
    public static class SensitiveCommentProcessor extends KeyedProcessFunction<String, CommentRecord, ProcessResult> {
        private transient ValueState<Integer> sensitiveCountState;
        private transient ValueState<List<Double>> userConsumptionState; // 存储用户消费记录

        @Override
        public void open(Configuration parameters) {
            ValueStateDescriptor<Integer> descriptor = new ValueStateDescriptor<Integer>("sensitiveCount", Integer.class);
            sensitiveCountState = getRuntimeContext().getState(descriptor);

            ValueStateDescriptor<List<Double>> consumptionDescriptor =
                    new ValueStateDescriptor<List<Double>>("userConsumption", TypeInformation.of(new TypeHint<List<Double>>() {}));
            userConsumptionState = getRuntimeContext().getState(consumptionDescriptor);
        }

        @Override
        public void processElement(CommentRecord record, Context ctx, Collector<ProcessResult> out) throws Exception {
            UserInfo userInfo = UserService.getUser(record.userId);
            SecurityConfig.SensitiveResult sensitiveResult = SecurityConfig.detect(record.commentContent);

            // 更新用户消费记录并计算消费级别
            String consumptionLevel = calculateConsumptionLevel(record.userId, record.totalAmount);

            // 更新敏感次数
            if (sensitiveResult.isSensitive) {
                Integer count = sensitiveCountState.value();
                if (count == null) {
                    count = 0;
                }
                count++;
                sensitiveCountState.update(count);
                System.out.println("用户 " + record.userId + " 累计敏感次数: " + count);
            }

            // 生成结果
            ProcessResult result = new ProcessResult(
                    record.userId, record.orderId,
                    record.commentContent, sensitiveResult.processedText,
                    sensitiveResult.isSensitive, sensitiveResult.level,
                    sensitiveResult.foundWords, sensitiveResult.getBanDays(),
                    sensitiveResult.getAction(), userInfo, record.totalAmount,
                    record.commentTime, consumptionLevel  // 添加消费级别
            );

            out.collect(result);

            // 输出日志
            if (sensitiveResult.isSensitive) {
                System.out.println("🚨 敏感评论警报 - 用户: " + record.userId +
                        ", 级别: " + sensitiveResult.level +
                        ", 封禁: " + sensitiveResult.getBanDays() + "天" +
                        ", 金额: " + (record.totalAmount != null ? record.totalAmount : "null") +
                        ", 消费级别: " + consumptionLevel +  // 添加消费级别显示
                        ", 日期: " + (record.commentTime != null ? record.commentTime : "null"));
                System.out.println("   订单: " + record.orderId);
                System.out.println("   原始: " + record.commentContent);
                System.out.println("   处理: " + sensitiveResult.processedText);
                System.out.println("   检测到: " + sensitiveResult.foundWords);

                // 执行封禁和屏蔽操作
                executeBanAction(record.userId, record.orderId, sensitiveResult);
            } else {
                System.out.println("✅ 正常评论 - 用户: " + record.userId +
                        ", 金额: " + (record.totalAmount != null ? record.totalAmount : "null") +
                        ", 消费级别: " + consumptionLevel +  // 添加消费级别显示
                        ", 日期: " + (record.commentTime != null ? record.commentTime : "null"));
            }
        }

        /**
         * 计算用户消费级别
         */
        private String calculateConsumptionLevel(String userId, Double currentAmount) {
            try {
                List<Double> consumptionHistory = userConsumptionState.value();
                if (consumptionHistory == null) {
                    consumptionHistory = new ArrayList<>();
                }

                // 添加当前消费金额
                if (currentAmount != null) {
                    consumptionHistory.add(currentAmount);
                }

                // 只保留最近2天的消费记录（假设每条记录代表一天）
                if (consumptionHistory.size() > 2) {
                    consumptionHistory = consumptionHistory.subList(consumptionHistory.size() - 2, consumptionHistory.size());
                }

                // 保存更新后的消费记录
                userConsumptionState.update(consumptionHistory);

                // 计算消费级别
                if (consumptionHistory.isEmpty()) {
                    return "LOW";
                }

                // 计算单日最高消费和平均消费
                double maxDaily = consumptionHistory.stream().mapToDouble(Double::doubleValue).max().orElse(0);
                double avgDaily = consumptionHistory.stream().mapToDouble(Double::doubleValue).average().orElse(0);

                // 根据规则判断消费级别
                if (maxDaily > 10000 || (consumptionHistory.size() == 2 && avgDaily >= 4000)) {
                    return "HIGH";
                } else if (maxDaily > 8000 || (consumptionHistory.size() == 2 && avgDaily >= 3000)) {
                    return "MEDIUM";
                } else {
                    return "LOW";
                }

            } catch (Exception e) {
                System.err.println("计算消费级别失败: " + e.getMessage());
                return "LOW";
            }
        }

        private void executeBanAction(String userId, String orderId, SecurityConfig.SensitiveResult result) {
            System.out.println("执行封禁操作:");
            System.out.println("  - 用户ID: " + userId);
            System.out.println("  - 订单ID: " + orderId);
            System.out.println("  - 封禁天数: " + result.getBanDays());
            System.out.println("  - 操作类型: " + result.getAction());

            if (result.getBanDays() > 0) {
                System.out.println("  ✅ 用户已被拉黑，封禁 " + result.getBanDays() + " 天");
            }
            System.out.println("  ✅ 评论已被屏蔽");
            System.out.println("----------------------------------------");
        }
    }
}