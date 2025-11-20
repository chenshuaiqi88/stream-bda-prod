package com.stream.realtime.lululemon.FlinkAPI_Doris;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import com.stream.core.ConfigUtils;
import com.stream.core.KafkaUtils;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisExecutionOptions;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.UUID;

/**
 * 实时日志分析程序 - 需求4和需求5写入Doris
 */
public class FlinkCheckerTest4 {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // 在F_doris类中添加Kafka配置常量
    private static final String KAFKA_BOOTSTRAP_SERVERS = "172.17.55.4:9092";
    private static final String KAFKA_TOPIC = "realtime_v3_logs";

    // Doris连接配置
    private static final String DORIS_FE_NODES = "192.168.200.31:18030";
    private static final String DORIS_DB = "bigdata_realtime_lululemon_report_v2";
    private static final String DORIS_USER = "root";
    private static final String DORIS_PASSWORD = "";

    // 需求4和需求5的表名
    private static final String DORIS_TABLE_USER_PATH = "dws_user_path_daily";
    private static final String DORIS_TABLE_DEVICE_PLATFORM = "dws_device_platform_daily";
    private static final String DORIS_TABLE_DEVICE_BRAND = "dws_device_brand_daily";
    private static final String DORIS_TABLE_DEVICE_MODEL = "dws_device_model_daily";

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);
        env.getCheckpointConfig().setCheckpointTimeout(30000);
        env.setParallelism(1);

        System.out.println("=== Doris连接配置 ===");
        System.out.println("FE Nodes: " + DORIS_FE_NODES);
        System.out.println("Database: " + DORIS_DB);
        System.out.println("====================");

        // 修改main方法中的KafkaSource创建
        KafkaSource<String> kafkaSource = KafkaUtils.buildKafkaSource(
                KAFKA_BOOTSTRAP_SERVERS,
                KAFKA_TOPIC,
                "demand4_5_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16),
                OffsetsInitializer.earliest()
        );

        DataStreamSource<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka_log_source"
        );

        // ==================== 需求4: 路径分析 ====================
        //processPathAnalysis(kafkaStream);

        // ==================== 需求5: 用户设备统计分析 ====================
        processDeviceStatistics(kafkaStream);

        env.execute("Demand 4&5 - Path Analysis & Device Statistics to Doris");
    }

    // ==================== 需求4: 路径分析 ====================
    private static void processPathAnalysis(DataStreamSource<String> kafkaStream) {
        DataStream<PathAnalysisEvent> pathEventStream = kafkaStream
                .map(new PathEventParser())
                .filter(event -> event != null && event.getUserId() != null && !"unknown".equals(event.getUserId()));

        // 实时路径分析 - 写入Doris
        SingleOutputStreamOperator<UserPathSession> pathSessionStream = pathEventStream
                .keyBy(PathAnalysisEvent::getUserId)
                .process(new UserPathSessionAnalyzer());

        // 输出到控制台
        pathSessionStream.addSink(new UserPathSessionSink());

        // 转换为JSON格式并写入Doris
        SingleOutputStreamOperator<String> pathJsonStream = pathSessionStream
                .map(session -> {
                    try {
                        JSONObject json = new JSONObject();
                        json.put("stat_date", session.getStatDate());
                        json.put("user_id", session.getUserId());
                        json.put("session_id", session.getSessionId());
                        json.put("path_sequence", session.getPathSequence());
                        json.put("unique_pages", session.getUniquePages());
                        json.put("total_events", session.getTotalEvents());
                        json.put("total_duration", session.getTotalDuration());
                        json.put("avg_duration", session.getAvgDuration());
                        json.put("start_time", formatDateTime(session.getStartTime()));
                        json.put("end_time", formatDateTime(session.getEndTime()));
                        json.put("is_bounce", session.getIsBounce());
                        json.put("is_conversion", session.getIsConversion());
                        json.put("update_time", formatDateTime(System.currentTimeMillis()));

                        String jsonStr = json.toJSONString();
                        System.out.println("🔄 准备写入Doris路径分析数据: " + jsonStr);
                        return jsonStr;
                    } catch (Exception e) {
                        System.err.println("❌ 路径分析JSON转换失败: " + e.getMessage());
                        return null;
                    }
                })
                .filter(Objects::nonNull);

        // 创建并添加Doris Sink
        try {
            DorisSink<String> dorisSink = createDorisSink(DORIS_TABLE_USER_PATH,
                    "stat_date,user_id,session_id,path_sequence,unique_pages,total_events,total_duration,avg_duration,start_time,end_time,is_bounce,is_conversion,update_time");
            pathJsonStream.sinkTo(dorisSink)
                    .name("Doris User Path Sink")
                    .uid("doris-user-path-sink");

            System.out.println("✅ 路径分析Doris Sink已添加到数据流");

        } catch (Exception e) {
            System.err.println("❌ 添加路径分析Doris Sink失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // ==================== 需求5: 用户设备统计分析 ====================
    private static void processDeviceStatistics(DataStreamSource<String> kafkaStream) {
        DataStream<BaseLogEvent> logEventStream = kafkaStream
                .map(new BaseLogParser())
                .filter(event -> event != null && event.getLogType() != null && !"unknown".equals(event.getDeviceId()));

        // 平台级别统计
        SingleOutputStreamOperator<PlatformStatResult> platformStream = logEventStream
                .keyBy(BaseLogEvent::getDeviceId)
                .process(new DeviceDeduplicator())
                .keyBy(DeviceStatResult::getDate)
                .process(new PlatformAggregator());

        // 品牌级别统计
        SingleOutputStreamOperator<BrandStatResult> brandStream = logEventStream
                .keyBy(BaseLogEvent::getDeviceId)
                .process(new DeviceDeduplicator())
                .keyBy(DeviceStatResult::getDate)
                .process(new BrandAggregator());

        // 设备型号统计
        SingleOutputStreamOperator<ModelStatResult> modelStream = logEventStream
                .keyBy(BaseLogEvent::getDeviceId)
                .process(new DeviceDeduplicator())
                .keyBy(DeviceStatResult::getDate)
                .process(new ModelAggregator());

        // 输出到控制台
        platformStream.addSink(new PlatformStatSink());
        brandStream.addSink(new BrandStatSink());
        modelStream.addSink(new ModelStatSink());

        // 写入Doris - 平台统计
        SingleOutputStreamOperator<String> platformJsonStream = platformStream
                .flatMap((PlatformStatResult result, Collector<String> out) -> {
                    try {
                        for (PlatformCount pc : result.getPlatformCounts()) {
                            JSONObject json = new JSONObject();
                            json.put("stat_date", result.getDate());
                            json.put("platform", pc.getPlatform());
                            json.put("device_count", pc.getCount());
                            json.put("percentage", pc.getPercent());
                            json.put("update_time", formatDateTime(result.getCalculationTime()));

                            String jsonStr = json.toJSONString();
                            System.out.println("📱 准备写入Doris平台统计: " + jsonStr);
                            out.collect(jsonStr);
                        }
                    } catch (Exception e) {
                        System.err.println("❌ 平台统计JSON转换失败: " + e.getMessage());
                    }
                })
                .returns(String.class)
                .filter(Objects::nonNull);

        // 写入Doris - 品牌统计
        SingleOutputStreamOperator<String> brandJsonStream = brandStream
                .flatMap((BrandStatResult result, Collector<String> out) -> {
                    try {
                        for (BrandCount bc : result.getBrandCounts()) {
                            JSONObject json = new JSONObject();
                            json.put("stat_date", result.getDate());
                            json.put("platform", bc.getPlatform());
                            json.put("brand", bc.getBrand());
                            json.put("device_count", bc.getCount());
                            json.put("percentage", bc.getPercent());
                            json.put("update_time", formatDateTime(result.getCalculationTime()));

                            String jsonStr = json.toJSONString();
                            System.out.println("🏷️ 准备写入Doris品牌统计: " + jsonStr);
                            out.collect(jsonStr);
                        }
                    } catch (Exception e) {
                        System.err.println("❌ 品牌统计JSON转换失败: " + e.getMessage());
                    }
                })
                .returns(String.class)
                .filter(Objects::nonNull);

        // 写入Doris - 型号统计
        SingleOutputStreamOperator<String> modelJsonStream = modelStream
                .flatMap((ModelStatResult result, Collector<String> out) -> {
                    try {
                        for (ModelCount mc : result.getModelCounts()) {
                            JSONObject json = new JSONObject();
                            json.put("stat_date", result.getDate());
                            json.put("platform", mc.getPlatform());
                            json.put("brand", mc.getBrand());
                            json.put("model", mc.getModel());
                            json.put("device_count", mc.getCount());
                            json.put("percentage", mc.getPercent());
                            json.put("update_time", formatDateTime(result.getCalculationTime()));

                            String jsonStr = json.toJSONString();
                            System.out.println("📱 准备写入Doris型号统计: " + jsonStr);
                            out.collect(jsonStr);
                        }
                    } catch (Exception e) {
                        System.err.println("❌ 型号统计JSON转换失败: " + e.getMessage());
                    }
                })
                .returns(String.class)
                .filter(Objects::nonNull);

        // 创建并添加Doris Sink
        try {
            // 平台统计Sink
            DorisSink<String> platformSink = createDorisSink(DORIS_TABLE_DEVICE_PLATFORM,
                    "stat_date,platform,device_count,percentage,update_time");
            platformJsonStream.sinkTo(platformSink)
                    .name("Doris Platform Sink")
                    .uid("doris-platform-sink");

            // 品牌统计Sink
            DorisSink<String> brandSink = createDorisSink(DORIS_TABLE_DEVICE_BRAND,
                    "stat_date,platform,brand,device_count,percentage,update_time");
            brandJsonStream.sinkTo(brandSink)
                    .name("Doris Brand Sink")
                    .uid("doris-brand-sink");

            // 型号统计Sink
            DorisSink<String> modelSink = createDorisSink(DORIS_TABLE_DEVICE_MODEL,
                    "stat_date,platform,brand,model,device_count,percentage,update_time");
            modelJsonStream.sinkTo(modelSink)
                    .name("Doris Model Sink")
                    .uid("doris-model-sink");

            System.out.println("✅ 所有设备统计Doris Sink已添加到数据流");

        } catch (Exception e) {
            System.err.println("❌ 添加设备统计Doris Sink失败: " + e.getMessage());
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
            streamLoadProps.setProperty("read_json_by_line", "true");
            streamLoadProps.setProperty("strip_outer_array", "false");
            streamLoadProps.setProperty("columns", columns);

            // 使用DorisExecutionOptions的labelPrefix参数
            DorisExecutionOptions executionOptions = DorisExecutionOptions.builder()
                    .setBufferSize(1024 * 1024)
                    .setBufferCount(3)
                    .setCheckInterval(5000)
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

    // ==================== 需求4的实现类 ====================

    /**
     * 用户路径会话分析器 - 支持会话超时检测
     */
    public static class UserPathSessionAnalyzer extends KeyedProcessFunction<String, PathAnalysisEvent, UserPathSession> {

        private transient ListState<PathAnalysisEvent> userEventsState;
        private transient ValueState<Long> sessionStartTimeState;
        private static final long SESSION_TIMEOUT = 30 * 60 * 1000; // 30分钟会话超时

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<PathAnalysisEvent> eventsDescriptor =
                    new ListStateDescriptor<>("userEventsState", PathAnalysisEvent.class);
            userEventsState = getRuntimeContext().getListState(eventsDescriptor);

            ValueStateDescriptor<Long> sessionStartDescriptor =
                    new ValueStateDescriptor<>("sessionStartTime", Long.class);
            sessionStartTimeState = getRuntimeContext().getState(sessionStartDescriptor);
        }

        @Override
        public void processElement(PathAnalysisEvent event, Context ctx, Collector<UserPathSession> out) throws Exception {
            Long sessionStartTime = sessionStartTimeState.value();

            // 检查会话是否超时
            if (sessionStartTime == null || (event.getTimestamp() - sessionStartTime) > SESSION_TIMEOUT) {
                // 新会话开始，输出上一个会话的分析结果
                if (sessionStartTime != null) {
                    UserPathSession session = analyzeUserSession(event.getUserId());
                    if (session != null) {
                        out.collect(session);
                    }
                }
                // 清空状态，开始新会话
                userEventsState.clear();
                sessionStartTimeState.update(event.getTimestamp());
            }

            // 添加当前事件到会话中
            userEventsState.add(event);

            // 更新会话开始时间
            sessionStartTimeState.update(event.getTimestamp());
        }

        private UserPathSession analyzeUserSession(String userId) throws Exception {
            List<PathAnalysisEvent> events = new ArrayList<>();
            for (PathAnalysisEvent event : userEventsState.get()) {
                events.add(event);
            }

            if (events.isEmpty()) {
                return null;
            }

            // 按时间排序
            events.sort(Comparator.comparing(PathAnalysisEvent::getTimestamp));

            // 构建路径序列JSON
            JSONArray pathSequence = new JSONArray();
            Set<String> uniquePages = new HashSet<>();
            long totalDuration = 0;
            boolean hasConversion = false;

            for (int i = 0; i < events.size(); i++) {
                PathAnalysisEvent current = events.get(i);

                JSONObject step = new JSONObject();
                step.put("page", current.getPageId());
                step.put("action", current.getActionType());
                step.put("timestamp", formatDateTime(current.getTimestamp()));
                pathSequence.add(step);

                uniquePages.add(current.getPageId());

                // 检查是否转化（假设checkout页面为转化）
                if ("checkout".equals(current.getPageId()) || "payment".equals(current.getActionType())) {
                    hasConversion = true;
                }

                // 计算页面停留时间
                if (i < events.size() - 1) {
                    PathAnalysisEvent next = events.get(i + 1);
                    long duration = next.getTimestamp() - current.getTimestamp();
                    totalDuration += duration;
                }
            }

            long avgDuration = events.size() > 1 ? totalDuration / (events.size() - 1) : 0;
            boolean isBounce = events.size() <= 2; // 访问页面数<=2认为是跳出

            return UserPathSession.builder()
                    .statDate(getDateFromTimestamp(events.get(0).getTimestamp()))
                    .userId(userId)
                    .sessionId(UUID.randomUUID().toString())
                    .pathSequence(pathSequence.toJSONString())
                    .uniquePages(uniquePages.size())
                    .totalEvents(events.size())
                    .totalDuration(totalDuration)
                    .avgDuration(avgDuration)
                    .startTime(events.get(0).getTimestamp())
                    .endTime(events.get(events.size() - 1).getTimestamp())
                    .isBounce(isBounce ? 1 : 0)
                    .isConversion(hasConversion ? 1 : 0)
                    .build();
        }
    }

    public static class UserPathSessionSink implements SinkFunction<UserPathSession> {
        @Override
        public void invoke(UserPathSession session, Context context) throws Exception {
            System.out.println("\n" + createRepeatString("=", 100));
            System.out.println("【用户路径会话分析】");
            System.out.println("用户ID: " + session.getUserId());
            System.out.println("会话ID: " + session.getSessionId());
            System.out.println("日期: " + session.getStatDate());
            System.out.println("路径长度: " + session.getTotalEvents());
            System.out.println("唯一页面: " + session.getUniquePages());
            System.out.println("总时长: " + session.getTotalDuration() + "ms");
            System.out.println("平均停留: " + session.getAvgDuration() + "ms");
            System.out.println("是否跳出: " + (session.getIsBounce() == 1 ? "是" : "否"));
            System.out.println("是否转化: " + (session.getIsConversion() == 1 ? "是" : "否"));
            System.out.println(createRepeatString("=", 100));
        }
    }

    // ==================== 需求5的实现类 ====================

    /**
     * 设备去重处理器 - 确保每个设备只统计一次
     */
    public static class DeviceDeduplicator extends KeyedProcessFunction<String, BaseLogEvent, DeviceStatResult> {
        private transient ValueState<Boolean> deviceSeenState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Boolean> seenDescriptor = new ValueStateDescriptor<>("deviceSeen", Boolean.class);
            deviceSeenState = getRuntimeContext().getState(seenDescriptor);
        }

        @Override
        public void processElement(BaseLogEvent event, Context ctx, Collector<DeviceStatResult> out) throws Exception {
            Boolean seen = deviceSeenState.value();

            // 如果设备还未被统计过，则输出统计结果
            if (seen == null) {
                String platform = normalizePlatform(event.getDeviceBrand(), event.getPlatform(), event.getRawData());
                String brand = normalizeBrand(event.getDeviceBrand());
                String model = normalizeModel(event.getDeviceModel());

                if (!"unknown".equals(platform) && !"unknown".equals(brand) && !"unknown".equals(model)) {
                    DeviceStatResult result = DeviceStatResult.builder()
                            .date(getDateFromTimestamp(event.getTimestamp()))
                            .platform(platform)
                            .brand(brand)
                            .model(model)
                            .count(1L)
                            .lastUpdateTime(System.currentTimeMillis())
                            .build();

                    out.collect(result);
                    deviceSeenState.update(true);

                    System.out.println("📱 新设备统计: " + platform + " | " + brand + " | " + model);
                }
            }
        }
    }

    public static class PlatformAggregator extends KeyedProcessFunction<String, DeviceStatResult, PlatformStatResult> {
        private transient MapState<String, Long> platformCountsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>("platformCounts", String.class, Long.class);
            platformCountsState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(DeviceStatResult value, Context ctx, Collector<PlatformStatResult> out) throws Exception {
            String platform = value.getPlatform();
            Long currentCount = platformCountsState.get(platform);
            if (currentCount == null) currentCount = 0L;
            platformCountsState.put(platform, currentCount + 1L);

            List<PlatformCount> platformCounts = getAllPlatformCounts();
            long total = platformCounts.stream().mapToLong(PlatformCount::getCount).sum();

            // 计算百分比
            List<PlatformCount> platformCountsWithPercent = new ArrayList<>();
            for (PlatformCount pc : platformCounts) {
                double percent = total > 0 ? (double) pc.getCount() / total * 100 : 0;
                platformCountsWithPercent.add(PlatformCount.builder()
                        .platform(pc.getPlatform())
                        .count(pc.getCount())
                        .percent(Math.round(percent * 100.0) / 100.0)
                        .build());
            }

            out.collect(PlatformStatResult.builder()
                    .date(value.getDate())
                    .platformCounts(platformCountsWithPercent)
                    .totalDevices(total)
                    .calculationTime(System.currentTimeMillis())
                    .build());
        }

        private List<PlatformCount> getAllPlatformCounts() throws Exception {
            List<PlatformCount> platformCounts = new ArrayList<>();
            for (Map.Entry<String, Long> entry : platformCountsState.entries()) {
                platformCounts.add(PlatformCount.builder()
                        .platform(entry.getKey())
                        .count(entry.getValue())
                        .build());
            }
            platformCounts.sort((a, b) -> Long.compare(b.getCount(), a.getCount()));
            return platformCounts;
        }
    }

    public static class BrandAggregator extends KeyedProcessFunction<String, DeviceStatResult, BrandStatResult> {
        private transient MapState<String, Long> brandCountsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>("brandCounts", String.class, Long.class);
            brandCountsState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(DeviceStatResult value, Context ctx, Collector<BrandStatResult> out) throws Exception {
            String brandKey = value.getPlatform() + "_" + value.getBrand();
            Long currentCount = brandCountsState.get(brandKey);
            if (currentCount == null) currentCount = 0L;
            brandCountsState.put(brandKey, currentCount + 1L);

            List<BrandCount> brandCounts = getAllBrandCounts();

            // 按平台分组计算百分比
            Map<String, Long> platformTotals = new HashMap<>();
            for (BrandCount bc : brandCounts) {
                String platform = bc.getPlatform();
                platformTotals.put(platform, platformTotals.getOrDefault(platform, 0L) + bc.getCount());
            }

            List<BrandCount> brandCountsWithPercent = new ArrayList<>();
            for (BrandCount bc : brandCounts) {
                Long platformTotal = platformTotals.get(bc.getPlatform());
                double percent = platformTotal > 0 ? (double) bc.getCount() / platformTotal * 100 : 0;
                brandCountsWithPercent.add(BrandCount.builder()
                        .platform(bc.getPlatform())
                        .brand(bc.getBrand())
                        .count(bc.getCount())
                        .percent(Math.round(percent * 100.0) / 100.0)
                        .build());
            }

            out.collect(BrandStatResult.builder()
                    .date(value.getDate())
                    .brandCounts(brandCountsWithPercent)
                    .calculationTime(System.currentTimeMillis())
                    .build());
        }

        private List<BrandCount> getAllBrandCounts() throws Exception {
            List<BrandCount> brandCounts = new ArrayList<>();
            for (Map.Entry<String, Long> entry : brandCountsState.entries()) {
                String[] parts = entry.getKey().split("_", 2);
                if (parts.length == 2) {
                    brandCounts.add(BrandCount.builder()
                            .platform(parts[0])
                            .brand(parts[1])
                            .count(entry.getValue())
                            .build());
                }
            }
            brandCounts.sort((a, b) -> Long.compare(b.getCount(), a.getCount()));
            return brandCounts;
        }
    }

    public static class ModelAggregator extends KeyedProcessFunction<String, DeviceStatResult, ModelStatResult> {
        private transient MapState<String, Long> modelCountsState;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>("modelCounts", String.class, Long.class);
            modelCountsState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(DeviceStatResult value, Context ctx, Collector<ModelStatResult> out) throws Exception {
            String modelKey = value.getPlatform() + "_" + value.getBrand() + "_" + value.getModel();
            Long currentCount = modelCountsState.get(modelKey);
            if (currentCount == null) currentCount = 0L;
            modelCountsState.put(modelKey, currentCount + 1L);

            List<ModelCount> modelCounts = getAllModelCounts();

            // 按品牌分组计算百分比
            Map<String, Long> brandTotals = new HashMap<>();
            for (ModelCount mc : modelCounts) {
                String brandKey = mc.getPlatform() + "_" + mc.getBrand();
                brandTotals.put(brandKey, brandTotals.getOrDefault(brandKey, 0L) + mc.getCount());
            }

            List<ModelCount> modelCountsWithPercent = new ArrayList<>();
            for (ModelCount mc : modelCounts) {
                String brandKey = mc.getPlatform() + "_" + mc.getBrand();
                Long brandTotal = brandTotals.get(brandKey);
                double percent = brandTotal > 0 ? (double) mc.getCount() / brandTotal * 100 : 0;
                modelCountsWithPercent.add(ModelCount.builder()
                        .platform(mc.getPlatform())
                        .brand(mc.getBrand())
                        .model(mc.getModel())
                        .count(mc.getCount())
                        .percent(Math.round(percent * 100.0) / 100.0)
                        .build());
            }

            out.collect(ModelStatResult.builder()
                    .date(value.getDate())
                    .modelCounts(modelCountsWithPercent)
                    .calculationTime(System.currentTimeMillis())
                    .build());
        }

        private List<ModelCount> getAllModelCounts() throws Exception {
            List<ModelCount> modelCounts = new ArrayList<>();
            for (Map.Entry<String, Long> entry : modelCountsState.entries()) {
                String[] parts = entry.getKey().split("_", 3);
                if (parts.length == 3) {
                    modelCounts.add(ModelCount.builder()
                            .platform(parts[0])
                            .brand(parts[1])
                            .model(parts[2])
                            .count(entry.getValue())
                            .build());
                }
            }
            modelCounts.sort((a, b) -> Long.compare(b.getCount(), a.getCount()));
            return modelCounts;
        }
    }

    public static class PlatformStatSink implements SinkFunction<PlatformStatResult> {
        @Override
        public void invoke(PlatformStatResult result, Context context) throws Exception {
            System.out.println("\n" + createRepeatString("=", 80));
            System.out.println("【设备平台统计分析】日期: " + result.getDate());
            System.out.println("总设备数: " + String.format("%,d", result.getTotalDevices()));
            System.out.println(createRepeatString("-", 80));

            for (PlatformCount platformCount : result.getPlatformCounts()) {
                System.out.printf("%-15s %8d %6.1f%%%n",
                        platformCount.getPlatform(),
                        platformCount.getCount(),
                        platformCount.getPercent());
            }
            System.out.println(createRepeatString("=", 80));
        }
    }

    public static class BrandStatSink implements SinkFunction<BrandStatResult> {
        @Override
        public void invoke(BrandStatResult result, Context context) throws Exception {
            System.out.println("\n" + createRepeatString("=", 100));
            System.out.println("【设备品牌统计分析】日期: " + result.getDate());
            System.out.println(createRepeatString("-", 100));
            System.out.printf("%-10s %-15s %8s %10s%n", "平台", "品牌", "数量", "占比");
            System.out.println(createRepeatString("-", 100));

            for (BrandCount brandCount : result.getBrandCounts()) {
                System.out.printf("%-10s %-15s %,8d %9.1f%%%n",
                        brandCount.getPlatform(),
                        brandCount.getBrand(),
                        brandCount.getCount(),
                        brandCount.getPercent());
            }
            System.out.println(createRepeatString("=", 100));
        }
    }

    public static class ModelStatSink implements SinkFunction<ModelStatResult> {
        @Override
        public void invoke(ModelStatResult result, Context context) throws Exception {
            System.out.println("\n" + createRepeatString("=", 120));
            System.out.println("【设备型号统计分析】日期: " + result.getDate());
            System.out.println(createRepeatString("-", 120));
            System.out.printf("%-10s %-15s %-20s %8s %10s%n", "平台", "品牌", "型号", "数量", "占比");
            System.out.println(createRepeatString("-", 120));

            int count = 0;
            for (ModelCount modelCount : result.getModelCounts()) {
                if (count++ >= 20) break; // 只显示前20个
                System.out.printf("%-10s %-15s %-20s %,8d %9.1f%%%n",
                        modelCount.getPlatform(),
                        modelCount.getBrand(),
                        modelCount.getModel(),
                        modelCount.getCount(),
                        modelCount.getPercent());
            }
            System.out.println(createRepeatString("=", 120));
        }
    }

    // ==================== 解析器类 ====================

    /**
     * 基础日志解析器 - 修复字段映射
     */
    public static class BaseLogParser implements MapFunction<String, BaseLogEvent> {
        @Override
        public BaseLogEvent map(String value) throws Exception {
            try {
                JSONObject json = JSONObject.parseObject(value);

                // 修复字段映射：使用实际数据中的字段名
                String logType = json.getString("log_type");
                Long timestamp = json.getLong("ts");

                // 设备信息解析 - 修复字段映射
                String deviceBrand = null;
                String deviceModel = null;
                String platform = null;
                String deviceId = null;

                JSONObject device = json.getJSONObject("device");
                if (device != null) {
                    deviceBrand = device.getString("brand");
                    deviceModel = device.getString("device");  // 注意：字段名是 device，不是 deviceModel
                    platform = device.getString("plat");       // 注意：字段名是 plat，不是 platform
                    deviceId = device.getString("userkey");    // 使用userkey作为设备唯一标识
                }

                return BaseLogEvent.builder()
                        .logType(logType)
                        .timestamp(timestamp)
                        .deviceId(deviceId != null ? deviceId : "unknown")
                        .deviceBrand(deviceBrand != null ? deviceBrand : "unknown")
                        .deviceModel(deviceModel != null ? deviceModel : "unknown")
                        .platform(platform != null ? platform : "unknown")
                        .rawData(value)
                        .build();

            } catch (Exception e) {
                System.err.println("❌ 日志解析失败: " + value + ", 错误: " + e.getMessage());
                return null;
            }
        }
    }

    /**
     * 路径事件解析器
     */
    public static class PathEventParser implements MapFunction<String, PathAnalysisEvent> {
        @Override
        public PathAnalysisEvent map(String value) throws Exception {
            try {
                JSONObject json = JSONObject.parseObject(value);

                // 修复字段映射
                String userId = json.getString("user_id");
                String pageId = json.getString("log_type");
                String actionType = json.getString("opa");
                Long timestamp = json.getLong("ts");

                if (userId == null || "unknown".equals(userId)) {
                    return null;
                }

                return PathAnalysisEvent.builder()
                        .userId(userId)
                        .pageId(pageId != null ? pageId : "unknown")
                        .actionType(actionType != null ? actionType : "unknown")
                        .timestamp(timestamp)
                        .rawData(value)
                        .build();

            } catch (Exception e) {
                System.err.println("❌ 路径事件解析失败: " + value + ", 错误: " + e.getMessage());
                return null;
            }
        }
    }

    // ==================== 辅助方法和数据类 ====================

    private static String createRepeatString(String str, int count) {
        return String.join("", Collections.nCopies(count, str));
    }

    private static String getDateFromTimestamp(Long timestamp) {
        if (timestamp == null) return "unknown";
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault())
                .format(DATE_FORMATTER);
    }

    private static String formatDateTime(Long timestamp) {
        if (timestamp == null) return "unknown";
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault())
                .format(TIME_FORMATTER);
    }

    private static String normalizePlatform(String deviceBrand, String platform, String rawData) {
        if (platform != null && !platform.trim().isEmpty()) {
            String lowerPlatform = platform.toLowerCase();
            if (lowerPlatform.contains("android") || lowerPlatform.contains("adr")) {
                return "Android";
            } else if (lowerPlatform.contains("ios") || lowerPlatform.contains("iphone")) {
                return "iOS";
            } else if (lowerPlatform.contains("windows")) {
                return "Windows";
            } else if (lowerPlatform.contains("mac")) {
                return "macOS";
            } else if (lowerPlatform.contains("linux")) {
                return "Linux";
            }
        }

        if (deviceBrand != null && !deviceBrand.trim().isEmpty()) {
            String lowerBrand = deviceBrand.toLowerCase();
            if (lowerBrand.contains("huawei") || lowerBrand.contains("honor")) {
                return "Android";
            } else if (lowerBrand.contains("xiaomi") || lowerBrand.contains("mi ")) {
                return "Android";
            } else if (lowerBrand.contains("oppo")) {
                return "Android";
            } else if (lowerBrand.contains("vivo")) {
                return "Android";
            } else if (lowerBrand.contains("samsung")) {
                return "Android";
            } else if (lowerBrand.contains("apple") || lowerBrand.contains("iphone")) {
                return "iOS";
            }
        }

        try {
            JSONObject json = JSONObject.parseObject(rawData);
            String userAgent = json.getString("userAgent");
            if (userAgent != null) {
                String lowerUA = userAgent.toLowerCase();
                if (lowerUA.contains("android") || lowerUA.contains("adr")) {
                    return "Android";
                } else if (lowerUA.contains("iphone") || lowerUA.contains("ipad") || lowerUA.contains("ios")) {
                    return "iOS";
                } else if (lowerUA.contains("windows")) {
                    return "Windows";
                } else if (lowerUA.contains("mac")) {
                    return "macOS";
                } else if (lowerUA.contains("linux")) {
                    return "Linux";
                }
            }
        } catch (Exception e) {
            // 忽略解析异常
        }

        return "unknown";
    }

    private static String normalizeBrand(String deviceBrand) {
        if (deviceBrand == null || deviceBrand.trim().isEmpty()) {
            return "unknown";
        }

        String lowerBrand = deviceBrand.toLowerCase().trim();
        if (lowerBrand.contains("huawei") || lowerBrand.contains("honor")) {
            return "华为";
        } else if (lowerBrand.contains("xiaomi") || lowerBrand.contains("mi ")) {
            return "小米";
        } else if (lowerBrand.contains("oppo")) {
            return "OPPO";
        } else if (lowerBrand.contains("vivo")) {
            return "vivo";
        } else if (lowerBrand.contains("samsung")) {
            return "三星";
        } else if (lowerBrand.contains("apple") || lowerBrand.contains("iphone")) {
            return "苹果";
        } else if (lowerBrand.contains("oneplus")) {
            return "一加";
        } else if (lowerBrand.contains("meizu")) {
            return "魅族";
        } else if (lowerBrand.contains("nokia")) {
            return "诺基亚";
        } else if (lowerBrand.contains("sony")) {
            return "索尼";
        } else if (lowerBrand.contains("lenovo")) {
            return "联想";
        } else {
            return deviceBrand;
        }
    }

    private static String normalizeModel(String deviceModel) {
        if (deviceModel == null || deviceModel.trim().isEmpty()) {
            return "unknown";
        }
        return deviceModel.trim();
    }

    // ==================== 数据类定义 ====================

    @Data
    @Builder
    public static class BaseLogEvent {
        private String logType;
        private Long timestamp;
        private String deviceId;      // 新增：设备唯一标识
        private String deviceBrand;
        private String deviceModel;
        private String platform;
        private String rawData;
    }

    @Data
    @Builder
    public static class PathAnalysisEvent {
        private String userId;
        private String pageId;
        private String actionType;
        private Long timestamp;
        private String rawData;
    }

    @Data
    @Builder
    public static class UserPathSession {
        private String statDate;
        private String userId;
        private String sessionId;
        private String pathSequence;
        private Integer uniquePages;
        private Integer totalEvents;
        private Long totalDuration;
        private Long avgDuration;
        private Long startTime;
        private Long endTime;
        private Integer isBounce;
        private Integer isConversion;
    }

    @Data
    @Builder
    public static class DeviceStatResult {
        private String date;
        private String platform;
        private String brand;
        private String model;
        private Long count;
        private Long lastUpdateTime;
    }

    @Data
    @Builder
    public static class PlatformCount {
        private String platform;
        private Long count;
        private Double percent;
    }

    @Data
    @Builder
    public static class PlatformStatResult {
        private String date;
        private List<PlatformCount> platformCounts;
        private Long totalDevices;
        private Long calculationTime;
    }

    @Data
    @Builder
    public static class BrandCount {
        private String platform;
        private String brand;
        private Long count;
        private Double percent;
    }

    @Data
    @Builder
    public static class BrandStatResult {
        private String date;
        private List<BrandCount> brandCounts;
        private Long calculationTime;
    }

    @Data
    @Builder
    public static class ModelCount {
        private String platform;
        private String brand;
        private String model;
        private Long count;
        private Double percent;
    }

    @Data
    @Builder
    public static class ModelStatResult {
        private String date;
        private List<ModelCount> modelCounts;
        private Long calculationTime;
    }
}