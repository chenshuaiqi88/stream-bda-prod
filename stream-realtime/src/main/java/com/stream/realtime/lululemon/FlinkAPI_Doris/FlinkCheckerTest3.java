package com.stream.realtime.lululemon.FlinkAPI_Doris;

import com.alibaba.fastjson.JSONObject;
import com.stream.core.KafkaUtils;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 实时日志分析程序 - 需求三和四
 * 需求3：地域热度分析（按日期+省份+城市排序）
 * 需求4：用户路径分析（历史天+当天路径转换统计）
 */
public class FlinkCheckerTest3 {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Doris连接配置
    private static final String DORIS_FE_NODES = "192.168.200.31:18030";
    private static final String DORIS_DB = "bigdata_realtime_lululemon_report_v2";

    // 两个表的表名
    private static final String DORIS_TABLE_REGIONAL_HEAT = "regional_heat_stats";
    private static final String DORIS_TABLE_PATH_ANALYSIS = "user_path_analysis";

    private static final String DORIS_USER = "root";
    private static final String DORIS_PASSWORD = "";

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 增强Checkpoint配置
        env.enableCheckpointing(3000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        // 设置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));

        env.setParallelism(1);

        System.out.println("=== Flink作业配置 ===");
        System.out.println("Checkpoint间隔: 3000ms");
        System.out.println("Checkpoint超时: 60000ms");
        System.out.println("并行度: 1");
        System.out.println("====================");

        System.out.println("=== Doris连接配置 ===");
        System.out.println("FE Nodes: " + DORIS_FE_NODES);
        System.out.println("Database: " + DORIS_DB);
        System.out.println("Regional Heat Table: " + DORIS_TABLE_REGIONAL_HEAT);
        System.out.println("Path Analysis Table: " + DORIS_TABLE_PATH_ANALYSIS);
        System.out.println("User: " + DORIS_USER);
        System.out.println("====================");

        KafkaSource<String> kafkaSource = KafkaUtils.buildKafkaSource(
                "172.17.55.4:9092",
                "realtime_v3_logs",
                "regional_path_analysis_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16),
                OffsetsInitializer.earliest()
        );

        DataStreamSource<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka_log_source"
        );

// 解析JSON数据 - 包含省份城市信息
        SingleOutputStreamOperator<JSONObject> parsed = kafkaStream
                .map(new MapFunction<String, JSONObject>() {
                    private int count = 0;
                    @Override
                    public JSONObject map(String value)  {
                        count++;
                        try {
                            System.out.println("📥 Kafka消息 #" + count + ": " +
                                    (value.length() > 100 ? value.substring(0, 100) + "..." : value));

                            JSONObject json = JSONObject.parseObject(value);
                            if (json == null) {
                                System.err.println("❌ JSON解析结果为null");
                                return null;
                            }

                            Long ts = json.getLong("ts");
                            if (ts == null) {
                                ts = System.currentTimeMillis();
                            }

                            // 处理时间戳格式
                            if (ts < 1000000000000L) {
                                ts = ts * 1000;
                            }

                            // 添加日期字段
                            LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.of("Asia/Shanghai"));
                            json.put("log_date", dateTime.format(DATE_FORMATTER));

                            // 检查是否已有省份城市信息
                            boolean hasProvince = json.containsKey("province");
                            boolean hasCity = json.containsKey("city");

                            String logType = json.getString("log_type");
                            String userId = json.getString("user_id");

                            System.out.println("✅ 解析成功 #" + count + ": " + logType + " | " + userId +
                                    " | 省份: " + (hasProvince ? json.getString("province") : "无") +
                                    " | 城市: " + (hasCity ? json.getString("city") : "无"));
                            return json;

                        } catch (Exception e) {
                            System.err.println("❌ JSON解析失败 #" + count + ": " + e.getMessage());
                            return null;
                        }
                    }
                })
                .filter(Objects::nonNull);

        // 添加类型安全的过滤
        SingleOutputStreamOperator<JSONObject> filteredParsed = parsed
                .filter(new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject value)  {
                        if (value == null) {
                            System.out.println("🚫 过滤null值");
                            return false;
                        }

                        // 检查必要的字段是否存在
                        boolean hasLogType = value.containsKey("log_type");
                        boolean hasLogDate = value.containsKey("log_date");

                        if (!hasLogType || !hasLogDate) {
                            System.out.println("🚫 过滤缺少必要字段的数据: " + value.toJSONString());
                            return false;
                        }

                        return true;
                    }
                })
                .name("JSONFilter")
                .uid("json-filter");

        // ==================== 运行需求 ====================
//        System.out.println("🚀 开始处理地域热度分析...");
//        processRegionalHeat(filteredParsed);

        // 需求4：用户路径分析
        System.out.println("🚀 开始处理用户路径分析...");
        processPathAnalysis(parsed);

        System.out.println("🏁 作业配置完成，开始执行...");
        env.execute("Regional Heat & Path Analysis to Doris");
    }

// ==================== 需求3: 地域热度分析 ====================
    private static void processRegionalHeat(DataStream<JSONObject> stream) {
        try {
            System.out.println("🔍 开始处理地域热度分析 - 使用已有省份城市信息...");

            // 直接使用已有的省份城市信息
            SingleOutputStreamOperator<Tuple4<String, String, String, Long>> regionStream = stream
                    .process(new ProcessFunction<JSONObject, Tuple4<String, String, String, Long>>() {
                        @Override
                        public void processElement(JSONObject json, Context ctx, Collector<Tuple4<String, String, String, Long>> out)  {
                            try {
                                String date = json.getString("log_date");
                                if (date == null) {
                                    date = LocalDateTime.now().format(DATE_FORMATTER);
                                }

                                // 直接使用已有的省份城市信息
                                String province = json.getString("province");
                                String city = json.getString("city");

                                // 如果省份城市为空，设置为unknown
                                if (province == null || province.trim().isEmpty()) {
                                    province = "unknown";
                                }
                                if (city == null || city.trim().isEmpty()) {
                                    city = "unknown";
                                }

                                Tuple4<String, String, String, Long> result = Tuple4.of(date, province, city, 1L);
                                System.out.println("📍 地域信息: " + date + " | " + province + " | " + city);
                                out.collect(result);

                            } catch (Exception e) {
                                System.err.println("❌ 提取地域信息失败: " + e.getMessage());
                                out.collect(Tuple4.of("error_date", "error_province", "error_city", 0L));
                            }
                        }
                    })
                    .returns(TupleTypeInfo.getBasicTupleTypeInfo(String.class, String.class, String.class, Long.class));

            // 按日期+省份+城市分组统计
            SingleOutputStreamOperator<Tuple4<String, String, String, Long>> regionCount = regionStream
                    .filter(t -> !"error_date".equals(t.f0) && t.f3 > 0)
                    .keyBy(t -> t.f0 + "_" + t.f1 + "_" + t.f2)
                    .sum(3)
                    .map(new MapFunction<Tuple4<String, String, String, Long>, Tuple4<String, String, String, Long>>() {
                        @Override
                        public Tuple4<String, String, String, Long> map(Tuple4<String, String, String, Long> value)  {
                            System.out.println("📊 地域统计: " + value.f0 + " | " + value.f1 + " | " + value.f2 + " | " + value.f3);
                            return value;
                        }
                    });

            // 只处理已知地域的数据（省份和城市都不是unknown）
            SingleOutputStreamOperator<Tuple4<String, String, String, Long>> knownRegionCount = regionCount
                    .filter(t -> !"unknown".equals(t.f1) && !"unknown".equals(t.f2))
                    .map(new MapFunction<Tuple4<String, String, String, Long>, Tuple4<String, String, String, Long>>() {
                        @Override
                        public Tuple4<String, String, String, Long> map(Tuple4<String, String, String, Long> value)  {
                            System.out.println("✅ 已知地域统计: " + value.f1 + " - " + value.f2 + " : " + value.f3);
                            return value;
                        }
                    });

            // 窗口处理 - 按时间窗口输出
            SingleOutputStreamOperator<String> regionalHeat = knownRegionCount
                    .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                    .process(new ProcessAllWindowFunction<Tuple4<String, String, String, Long>, String, TimeWindow>() {
                        @Override
                        public void process(Context context, Iterable<Tuple4<String, String, String, Long>> elements, Collector<String> out) {
                            try {
                                List<Tuple4<String, String, String, Long>> list = new ArrayList<>();
                                for (Tuple4<String, String, String, Long> element : elements) {
                                    list.add(element);
                                }

                                // 按访问量降序排序，显示热门地区
                                list.sort((a, b) -> Long.compare(b.f3, a.f3));

                                System.out.println("🏆 热门地域排名 (前10):");
                                int count = 0;
                                for (Tuple4<String, String, String, Long> item : list) {
                                    String result = String.format("(%s, %s, %s, %d)", item.f0, item.f1, item.f2, item.f3);
                                    if (count < 10) {
                                        System.out.println("🥇 TOP" + (count+1) + ": " + result);
                                    }
                                    out.collect(result);
                                    count++;
                                }
                                System.out.println("📈 总共 " + count + " 个地域数据");

                            } catch (Exception e) {
                                System.err.println("❌ 窗口处理失败: " + e.getMessage());
                            }
                        }
                    });

            // 转换为JSON格式并写入Doris
            SingleOutputStreamOperator<String> regionalJsonStream = regionalHeat
                    .map(new MapFunction<String, String>() {
                        @Override
                        public String map(String value)  {
                            try {
                                String cleaned = value.replace("(", "").replace(")", "");
                                String[] parts = cleaned.split(", ");

                                if (parts.length != 4) return null;

                                JSONObject json = new JSONObject();
                                json.put("stat_date", parts[0]);
                                json.put("province", parts[1]);
                                json.put("city", parts[2]);
                                json.put("visit_count", Long.parseLong(parts[3]));

                                String jsonStr = json.toJSONString();
                                System.out.println("🎯 准备写入Doris地域热度数据: " + jsonStr);
                                return jsonStr;

                            } catch (Exception e) {
                                System.err.println("❌ 地域热度JSON转换失败: " + e.getMessage());
                                return null;
                            }
                        }
                    })
                    .filter(Objects::nonNull);

            // 添加控制台输出监控
            regionalJsonStream.addSink(new SinkFunction<String>() {
                @Override
                public void invoke(String value, Context context)  {
                    System.out.println("💾 确认数据生成: " + value);
                }
            });

            // 创建并添加Doris Sink
            try {
                DorisSink<String> dorisSink = createDorisSink(DORIS_TABLE_REGIONAL_HEAT, "stat_date,province,city,visit_count");
                regionalJsonStream.sinkTo(dorisSink)
                        .name("Doris Regional Heat Sink")
                        .uid("doris-regional-heat-sink");

                System.out.println("✅ 地域热度Doris Sink已添加到数据流");

            } catch (Exception e) {
                System.err.println("❌ 添加地域热度Doris Sink失败: " + e.getMessage());
                e.printStackTrace();
            }

        } catch (Exception e) {
            System.err.println("❌ 地域热度分析初始化失败: " + e.getMessage());
            e.printStackTrace();
        }
    }
    // ==================== 需求4: 用户路径分析 ====================
    private static void processPathAnalysis(DataStream<JSONObject> stream) {
        // 历史天 + 当天 路径分析
        SingleOutputStreamOperator<String> pathAnalysis = stream
                .keyBy(json -> json.getString("user_id"))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<JSONObject, Tuple3<String, String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String userId, Context context, Iterable<JSONObject> input, Collector<Tuple3<String, String, Integer>> out) {
                        try {
                            List<JSONObject> events = new ArrayList<>();
                            input.forEach(events::add);

                            // 按时间戳排序
                            events.sort(Comparator.comparingLong(e -> e.getLong("ts")));

                            for (int i = 0; i < events.size() - 1; i++) {
                                String from = events.get(i).getString("log_type");
                                String to = events.get(i + 1).getString("log_type");
                                Long ts = events.get(i + 1).getLong("ts");

                                if (from != null && to != null && !from.equals(to)) {
                                    String dateStr = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.of("Asia/Shanghai"))
                                            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                                    Tuple3<String, String, Integer> path = Tuple3.of(dateStr, from + "→" + to, 1);
                                    System.out.println("🛣️ 用户路径: " + userId + " | " + path.f0 + " | " + path.f1);
                                    out.collect(path);
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("❌ 用户路径分析失败: " + e.getMessage());
                        }
                    }
                })
                .keyBy(new org.apache.flink.api.java.functions.KeySelector<Tuple3<String, String, Integer>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(Tuple3<String, String, Integer> t) {
                        return Tuple2.of(t.f0, t.f1);
                    }
                })
                .sum(2)
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(15)))
                .process(new ProcessAllWindowFunction<Tuple3<String, String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Tuple3<String, String, Integer>> input, Collector<String> out) {
                        try {
                            List<Tuple3<String, String, Integer>> results = new ArrayList<>();
                            input.forEach(results::add);

                            // 按日期排序
                            results.sort(Comparator.comparing(t -> t.f0));

                            for (Tuple3<String, String, Integer> t : results) {
                                String result = String.format("%s (%s,%d)", t.f0, t.f1, t.f2);
                                System.out.println("📊 路径统计: " + result);
                                out.collect(result);
                            }
                        } catch (Exception e) {
                            System.err.println("❌ 路径统计失败: " + e.getMessage());
                        }
                    }
                });

        // 转换为JSON格式并写入Doris
        SingleOutputStreamOperator<String> pathJsonStream = pathAnalysis
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value)  {
                        try {
                            // 解析格式: 2025-11-02 (page_view→search,15)
                            String[] mainParts = value.split(" \\(");
                            String date = mainParts[0];
                            String pathPart = mainParts[1].replace(")", "");
                            String[] pathParts = pathPart.split(",");

                            String path = pathParts[0];
                            int count = Integer.parseInt(pathParts[1]);

                            JSONObject json = new JSONObject();
                            json.put("stat_date", date);
                            json.put("path_sequence", path);
                            json.put("transition_count", count);

                            String jsonStr = json.toJSONString();
                            System.out.println("🎯 准备写入Doris路径分析数据: " + jsonStr);
                            return jsonStr;
                        } catch (Exception e) {
                            System.err.println("❌ 路径分析JSON转换失败: " + e.getMessage());
                            return null;
                        }
                    }
                })
                .filter(Objects::nonNull);

        // 创建并添加Doris Sink
        try {
            DorisSink<String> dorisSink = createDorisSink(DORIS_TABLE_PATH_ANALYSIS, "stat_date,path_sequence,transition_count");
            pathJsonStream.sinkTo(dorisSink)
                    .name("Doris Path Analysis Sink")
                    .uid("doris-path-analysis-sink");

            System.out.println("✅ 路径分析Doris Sink已添加到数据流");

        } catch (Exception e) {
            System.err.println("❌ 添加路径分析Doris Sink失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // ==================== 创建Doris Sink ====================
    private static DorisSink<String> createDorisSink(String tableName, String columns) {
        try {
            System.out.println("🔄 开始创建Doris Sink for table: " + tableName);

            // Doris连接选项
            DorisOptions dorisOptions = DorisOptions.builder()
                    .setFenodes(DORIS_FE_NODES)
                    .setTableIdentifier(DORIS_DB + "." + tableName)
                    .setUsername(DORIS_USER)
                    .setPassword(DORIS_PASSWORD)
                    .build();

            System.out.println("📊 Doris连接配置: " + DORIS_FE_NODES + ", 表: " + DORIS_DB + "." + tableName);
            System.out.println("📋 使用的列映射: " + columns);

            // Stream Load配置
            Properties streamLoadProps = new Properties();
            streamLoadProps.setProperty("format", "json");
            streamLoadProps.setProperty("strip_outer_array", "false");
            streamLoadProps.setProperty("sink.batch.size", "2");
            streamLoadProps.setProperty("sink.batch.interval", "1000");
            streamLoadProps.setProperty("sink.max-retries", "3");
            streamLoadProps.setProperty("sink.enable-2pc", "false");
            streamLoadProps.setProperty("columns", columns);
            streamLoadProps.setProperty("read_json_by_line", "true");

            DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                    .setBufferSize(1024 * 1024)
                    .setBufferCount(2)
                    .setCheckInterval(1000)
                    .setMaxRetries(3)
                    .setLabelPrefix("flink_sink_" + UUID.randomUUID().toString().substring(0, 8) + "_")
                    .setStreamLoadProp(streamLoadProps)
                    .build();

            // 创建Doris Sink
            DorisSink<String> sink = DorisSink.<String>builder()
                    .setDorisOptions(dorisOptions)
                    .setDorisExecutionOptions(executionOptions)
                    .setSerializer(new SimpleStringSerializer())
                    .build();

            System.out.println("✅ Doris Sink创建成功 for table: " + tableName);
            return sink;

        } catch (Exception e) {
            System.err.println("❌ 创建Doris Sink失败 for table " + tableName + ": " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("Doris Sink初始化失败", e);
        }
    }

    // ==================== 数据模型类 ====================
    @Data
    @Builder
    public static class RegionalHeatResult {
        private String statDate;
        private String province;
        private String city;
        private Long visitCount;
    }

    @Data
    @Builder
    public static class PathAnalysisResult {
        private String statDate;
        private String pathSequence;
        private Integer transitionCount;
    }
}