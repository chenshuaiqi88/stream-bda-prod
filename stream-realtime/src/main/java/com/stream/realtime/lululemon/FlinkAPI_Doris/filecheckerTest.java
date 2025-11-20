package com.stream.realtime.lululemon.FlinkAPI_Doris;

import java.util.UUID;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;


import com.stream.core.KafkaUtils;
import lombok.Builder;
import lombok.Data;
import lombok.SneakyThrows;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
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
import org.apache.doris.flink.cfg.DorisReadOptions;


import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * 实时日志分析程序 - 需求一和二
 * 需求1：页面访问量统计
 * 需求2：搜索词TOP10词云
 */
// 修改版
public class filecheckerTest {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Doris连接配置 - 修改为你的配置
    private static final String DORIS_FE_NODES = "192.168.200.31:18030";
    private static final String DORIS_DB = "bigdata_realtime_lululemon_report_v2";

    // 两个表的表名 - 修改为你的表名
    private static final String DORIS_TABLE_PAGE_VISIT = "page_view_stats";
    private static final String DORIS_TABLE_SEARCH_TOP10 = "search_top10_daily";

    private static final String DORIS_USER = "root";
    private static final String DORIS_PASSWORD = "";

    @SneakyThrows
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 增强Checkpoint配置
        env.enableCheckpointing(3000); // 3秒一次
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
        System.out.println("Page Visit Table: " + DORIS_TABLE_PAGE_VISIT);
        System.out.println("Search Top10 Table: " + DORIS_TABLE_SEARCH_TOP10);
        System.out.println("User: " + DORIS_USER);
        System.out.println("====================");

        KafkaSource<String> kafkaSource = KafkaUtils.buildKafkaSource(
                "172.17.55.4:9092", // 直接使用你的Kafka地址
                "realtime_v3_logs",
                "integrated_analysis_" + UUID.randomUUID().toString().replace("-", "").substring(0, 16),
                OffsetsInitializer.earliest()
        );

        DataStreamSource<String> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka_log_source"
        );



        // 添加Kafka数据监控
        DataStream<String> monitoredKafkaStream = kafkaStream
                .map(new MapFunction<String, String>() {
                    private int count = 0;
                    @Override
                    public String map(String value) throws Exception {
                        count++;
                        System.out.println("📥 Kafka消息 #" + count + ": " +
                                (value.length() > 100 ? value.substring(0, 100) + "..." : value));
                        return value;
                    }
                });

        DataStream<LogEvent> logEventStream = monitoredKafkaStream
                .map(new LogParser())
                .filter(event -> {
                    boolean valid = event != null && event.getLogType() != null;
                    if (valid) {
                        System.out.println("✅ 解析成功: " + event.getLogType() + " | " + event.getUserId());
                    } else {
                        System.out.println("❌ 解析失败或事件为空");
                    }
                    return valid;
                });

        // 添加最终数据流监控
        DataStream<LogEvent> finalStream = logEventStream
                .map(new MapFunction<LogEvent, LogEvent>() {
                    private int processedCount = 0;
                    @Override
                    public LogEvent map(LogEvent value) throws Exception {
                        processedCount++;
                        System.out.println("🎯 总处理事件 #" + processedCount + ": " + value.getLogType());
                        return value;
                    }
                });

        // ==================== 运行两个需求 ====================
        // 需求1：页面访问量统计
//        System.out.println("🚀 开始处理页面访问统计...");
//        processPageVisits(logEventStream);
//        System.out.println("🏁 作业配置完成，开始执行...");

        // 需求2：搜索词TOP10词云
        System.out.println("🚀 开始处理top10统计...");
         processSearchKeywords(logEventStream);
        System.out.println("🏁 作业配置完成，开始执行...");

        env.execute("Log Analysis - Page Visits & Search Keywords to Doris");
    }

    // ==================== 需求1: 页面访问量统计 ====================
    private static void processPageVisits(DataStream<LogEvent> stream) {
        SingleOutputStreamOperator<PageVisitResult> pageVisitStream = stream
                .filter(event -> {
                    String logType = event.getLogType();
                    boolean matched = "payment".equals(logType) ||
                            "page_view".equals(logType) ||
                            "click".equals(logType) ||
                            "view".equals(logType) ||
                            "search".equals(logType) ||
                            "login".equals(logType) ||
                            "product_list".equals(logType);

                    if (matched) {
                        System.out.println("📄 匹配到页面访问日志类型: " + logType);
                    }
                    return matched;
                })
                .keyBy(event -> {
                    String date = getDateFromTimestamp(event.getTimestamp());
                    String logType = event.getLogType() != null ? event.getLogType() : "unknown";
                    String key = date + "_" + logType;
                    System.out.println("🔑 页面访问Key分组: " + key);
                    return key;
                })
                .process(new PageVisitCounter());

        // 输出到控制台
        pageVisitStream.addSink(new PageVisitSink());

        // 转换为JSON格式并写入Doris - 修改为匹配你的表结构
        SingleOutputStreamOperator<String> jsonStream = pageVisitStream
                .map(result -> {
                    try {
                        JSONObject json = new JSONObject();
                        // 匹配你的表结构：ds, page_name, pv
                        json.put("ds", result.getDate());
                        json.put("page_name", result.getPageType());
                        json.put("pv", result.getVisitCount());

                        String jsonStr = json.toJSONString();
                        System.out.println("🎯 准备写入Doris页面访问数据: " + jsonStr);
                        return jsonStr;
                    } catch (Exception e) {
                        System.err.println("❌ 页面访问JSON转换失败: " + e.getMessage());
                        return null;
                    }
                })
                .filter(Objects::nonNull);

        // 添加控制台输出作为备份
        jsonStream.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                System.out.println("💾 确认数据生成: " + value);
            }
        });


        // 添加监控Sink
        jsonStream.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                System.out.println("📤 数据发送到Doris Sink: " + value);
            }
        });

        // 创建并添加Doris Sink - 修改列映射
        try {
            DorisSink<String> dorisSink = createDorisSink(DORIS_TABLE_PAGE_VISIT, "ds,page_name,pv");
            jsonStream.sinkTo(dorisSink)
                    .name("Doris Page Visit Sink")
                    .uid("doris-page-visit-sink");

            System.out.println("✅ 页面访问Doris Sink已添加到数据流");

        } catch (Exception e) {
            System.err.println("❌ 添加页面访问Doris Sink失败: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // ==================== 需求2: 搜索词TOP10词云 ====================
    private static void processSearchKeywords(DataStream<LogEvent> stream) {
        // 修复：创建正确的数据流管道，确保类型一致性
        DataStream<SearchWordResult> searchWordStream = stream
                .map(new SearchKeywordExtractor())
                .filter(new FilterFunction<SearchWordResult>() {
                    @Override
                    public boolean filter(SearchWordResult value) throws Exception {
                        return value != null;
                    }
                });

        // 处理搜索词统计
        SingleOutputStreamOperator<DailyTopNResult> searchTopNStream = searchWordStream
                .keyBy(result -> result.getDate() + "_" + result.getKeyword())
                .process(new SearchWordCounter())
                .keyBy(SearchWordResult::getDate)
                .process(new DailyTopNCalculator(10));

        // 输出到控制台
        searchTopNStream.addSink(new SearchTopNSink());

        // 展开TOP10列表，为每个关键词创建一条记录
        SingleOutputStreamOperator<String> searchJsonStream = searchTopNStream
                .flatMap((DailyTopNResult result, Collector<String> out) -> {
                    try {
                        String date = result.getDate();
                        List<KeywordCount> topNKeywords = result.getTopNKeywords();

                        for (KeywordCount keywordCount : topNKeywords) {
                            JSONObject json = new JSONObject();
                            // 按照表结构顺序：date, rank, keyword, search_count
                            json.put("date", date);
                            json.put("rank", keywordCount.getRank());
                            json.put("keyword", keywordCount.getKeyword());
                            json.put("search_count", keywordCount.getCount());

                            String jsonStr = json.toJSONString();
                            System.out.println("🔍 准备写入Doris搜索TOP10数据: " + jsonStr);
                            out.collect(jsonStr);
                        }
                    } catch (Exception e) {
                        System.err.println("❌ 搜索TOP10数据展开失败: " + e.getMessage());
                        e.printStackTrace();
                    }
                })
                .returns(String.class)
                .filter(Objects::nonNull);

        // 创建并添加Doris Sink - 使用新的列顺序
        try {
            DorisSink<String> dorisSink = createDorisSink(DORIS_TABLE_SEARCH_TOP10, "date,rank,keyword,search_count");
            searchJsonStream.sinkTo(dorisSink)
                    .name("Doris Search Top10 Sink")
                    .uid("doris-search-top10-sink");

            System.out.println("✅ 搜索TOP10 Doris Sink已添加到数据流");

        } catch (Exception e) {
            System.err.println("❌ 添加搜索TOP10 Doris Sink失败: " + e.getMessage());
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

            // Stream Load配置 - 使用你的配置参数
            Properties streamLoadProps = new Properties();
            streamLoadProps.setProperty("format", "json");
            streamLoadProps.setProperty("strip_outer_array", "false");
            streamLoadProps.setProperty("sink.batch.size", "2");
            streamLoadProps.setProperty("sink.batch.interval", "1000");
            streamLoadProps.setProperty("sink.max-retries", "3");
            streamLoadProps.setProperty("sink.enable-2pc", "false");
            streamLoadProps.setProperty("columns", columns);
            streamLoadProps.setProperty("read_json_by_line", "true"); // 添加这个

            // 使用DorisExecutionOptions的labelPrefix参数
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

    // ==================== 需求1的实现类 ====================
    public static class PageVisitCounter extends KeyedProcessFunction<String, LogEvent, PageVisitResult> {
        private transient ValueState<Long> visitCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> countDescriptor = new ValueStateDescriptor<>("visitCount", Long.class);
            visitCountState = getRuntimeContext().getState(countDescriptor);
            System.out.println("🔄 PageVisitCounter状态初始化完成");
        }

        @Override
        public void processElement(LogEvent value, Context ctx, Collector<PageVisitResult> out) throws Exception {
            Long count = visitCountState.value();
            if (count == null) count = 0L;
            count++;
            visitCountState.update(count);

            String key = ctx.getCurrentKey();
            String[] keyParts = key.split("_", 2);
            String date = keyParts[0];
            String pageType = keyParts.length > 1 ? keyParts[1] : "unknown";

            PageVisitResult result = PageVisitResult.builder()
                    .date(date)
                    .pageType(pageType)
                    .visitCount(count)
                    .lastUpdateTime(System.currentTimeMillis())
                    .build();

            System.out.println("📈 生成页面访问结果: " + date + " | " + pageType + " | " + count);
            out.collect(result);
        }
    }

    public static class PageVisitSink implements SinkFunction<PageVisitResult> {
        @Override
        public void invoke(PageVisitResult result, Context context) throws Exception {
            String output = String.format("[页面访问量统计] 日期: %s | 页面类型: %s | 累计访问量: %d",
                    result.getDate(), result.getPageType(), result.getVisitCount());
            System.out.println("📊 " + output);
        }
    }

    // ==================== 需求2的实现类 ====================
    public static class SearchKeywordExtractor implements MapFunction<LogEvent, SearchWordResult> {
        @Override
        public SearchWordResult map(LogEvent value) throws Exception {
            try {
                // 只处理搜索类型的日志
                if (!"search".equals(value.getLogType())) {
                    return null;
                }

                JSONObject json = JSONObject.parseObject(value.getRawData());
                List<String> keywords = new ArrayList<>();

                // 处理keywords数组
                if (json.containsKey("keywords")) {
                    Object keywordsObj = json.get("keywords");
                    if (keywordsObj instanceof JSONArray) {
                        // 处理JSON数组
                        JSONArray keywordArray = (JSONArray) keywordsObj;
                        for (int i = 0; i < keywordArray.size(); i++) {
                            Object item = keywordArray.get(i);
                            if (item != null) {
                                String keyword = item.toString().trim();
                                if (!keyword.isEmpty() && !"null".equals(keyword)) {
                                    keywords.add(keyword);
                                    System.out.println("📝 提取到关键词: " + keyword);
                                }
                            }
                        }
                    } else if (keywordsObj instanceof List) {
                        // 处理Java List
                        List<?> keywordList = (List<?>) keywordsObj;
                        for (Object item : keywordList) {
                            if (item != null) {
                                String keyword = item.toString().trim();
                                if (!keyword.isEmpty() && !"null".equals(keyword)) {
                                    keywords.add(keyword);
                                    System.out.println("📝 提取到关键词: " + keyword);
                                }
                            }
                        }
                    } else if (keywordsObj instanceof String) {
                        // 如果是字符串，尝试按逗号分割
                        String keywordStr = (String) keywordsObj;
                        String[] keywordArray = keywordStr.split(",");
                        for (String keyword : keywordArray) {
                            keyword = keyword.trim();
                            if (!keyword.isEmpty() && !"null".equals(keyword)) {
                                keywords.add(keyword);
                                System.out.println("📝 提取到关键词: " + keyword);
                            }
                        }
                    }
                }

                // 如果没有找到关键词，返回null
                if (keywords.isEmpty()) {
                    System.out.println("⚠️ 未找到有效关键词");
                    return null;
                }

                // 为每个关键词创建一个SearchWordResult
                // 注意：这里我们只返回第一个关键词，后续会在counter中处理
                String firstKeyword = keywords.get(0);
                System.out.println("🔍 使用搜索关键词: " + firstKeyword + " (总关键词数: " + keywords.size() + ")");

                return SearchWordResult.builder()
                        .date(getDateFromTimestamp(value.getTimestamp()))
                        .keyword(firstKeyword)
                        .count(1L)
                        .lastUpdateTime(System.currentTimeMillis())
                        .build();

            } catch (Exception e) {
                System.err.println("❌ 提取搜索关键词失败: " + e.getMessage());
                e.printStackTrace();
                return null;
            }
        }
    }

    public static class SearchWordCounter extends KeyedProcessFunction<String, SearchWordResult, SearchWordResult> {
        private transient ValueState<Long> keywordCountState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Long> countDescriptor = new ValueStateDescriptor<>("keywordCount", Long.class);
            keywordCountState = getRuntimeContext().getState(countDescriptor);
            System.out.println("🔄 SearchWordCounter状态初始化完成");
        }

        @Override
        public void processElement(SearchWordResult value, Context ctx, Collector<SearchWordResult> out) throws Exception {
            Long currentCount = keywordCountState.value();
            if (currentCount == null) currentCount = 0L;
            currentCount += value.getCount();
            keywordCountState.update(currentCount);

            SearchWordResult result = SearchWordResult.builder()
                    .date(value.getDate())
                    .keyword(value.getKeyword())
                    .count(currentCount)
                    .lastUpdateTime(System.currentTimeMillis())
                    .build();

            System.out.println("📈 搜索词统计: " + value.getDate() + " | " + value.getKeyword() + " | " + currentCount);
            out.collect(result);
        }
    }

    public static class DailyTopNCalculator extends KeyedProcessFunction<String, SearchWordResult, DailyTopNResult> {
        private final int topN;
        private transient MapState<String, Long> keywordCountsState;

        public DailyTopNCalculator(int topN) {
            this.topN = topN;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>("keywordCounts", String.class, Long.class);
            keywordCountsState = getRuntimeContext().getMapState(descriptor);
            System.out.println("🔄 DailyTopNCalculator状态初始化完成, TopN: " + topN);
        }

        @Override
        public void processElement(SearchWordResult value, Context ctx, Collector<DailyTopNResult> out) throws Exception {
            String keyword = value.getKeyword();
            Long currentCount = keywordCountsState.get(keyword);
            if (currentCount == null) currentCount = 0L;
            keywordCountsState.put(keyword, currentCount + value.getCount());

            List<KeywordCount> topNList = calculateTopN();

            DailyTopNResult result = DailyTopNResult.builder()
                    .date(value.getDate())
                    .topNKeywords(topNList)
                    .calculationTime(System.currentTimeMillis())
                    .build();

            System.out.println("🏆 生成TOP" + topN + "结果, 日期: " + value.getDate() + ", 关键词数量: " + topNList.size());
            out.collect(result);
        }

        private List<KeywordCount> calculateTopN() throws Exception {
            List<Map.Entry<String, Long>> allKeywords = new ArrayList<>();
            for (Map.Entry<String, Long> entry : keywordCountsState.entries()) {
                allKeywords.add(entry);
            }

            // 按搜索次数降序排序
            allKeywords.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));

            List<KeywordCount> topNList = new ArrayList<>();
            int rank = 1;
            for (int i = 0; i < Math.min(topN, allKeywords.size()); i++) {
                Map.Entry<String, Long> entry = allKeywords.get(i);
                KeywordCount keywordCount = KeywordCount.builder()
                        .keyword(entry.getKey())
                        .count(entry.getValue())
                        .rank(rank++)
                        .build();
                topNList.add(keywordCount);
                System.out.println("🥇 TOP" + rank + ": " + entry.getKey() + " - " + entry.getValue() + "次");
            }
            return topNList;
        }
    }

    public static class SearchTopNSink implements SinkFunction<DailyTopNResult> {
        @Override
        public void invoke(DailyTopNResult result, Context context) throws Exception {
            System.out.println("\n" + createRepeatString("=", 80));
            System.out.println("【每日搜索词TOP10词云数据】日期: " + result.getDate());
            System.out.println(createRepeatString("-", 80));

            if (result.getTopNKeywords().isEmpty()) {
                System.out.println("暂无搜索数据");
            } else {
                for (KeywordCount keywordCount : result.getTopNKeywords()) {
                    int starCount = Math.max(1, (int) Math.log(keywordCount.getCount() + 1));
                    String visual = createRepeatString("★", starCount);
                    System.out.printf("第%2d名: %-20s %s (搜索%d次)%n",
                            keywordCount.getRank(), keywordCount.getKeyword(), visual, keywordCount.getCount());
                }
            }
            System.out.println(createRepeatString("=", 80));
        }
    }

    // ==================== 工具方法 ====================
    private static String getDateFromTimestamp(Long timestamp) {
        if (timestamp == null) {
            return getTodayDate();
        }
        try {
            // 处理时间戳格式（秒或毫秒）
            long actualTimestamp = timestamp;
            if (timestamp < 1000000000000L) { // 小于 1 万亿说明是秒级
                actualTimestamp = timestamp * 1000;
            }

            LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(actualTimestamp), ZoneId.of("Asia/Shanghai"));
            return dateTime.format(DATE_FORMATTER);
        } catch (Exception e) {
            return getTodayDate();
        }
    }

    private static String getTodayDate() {
        return LocalDateTime.now().format(DATE_FORMATTER);
    }

    private static String createRepeatString(String str, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(str);
        }
        return sb.toString();
    }

    // ==================== 数据模型类 ====================
    @Data
    @Builder
    public static class LogEvent {
        private String logId;
        private String logType;
        private String userId;
        private Long timestamp;
        private String deviceBrand;
        private String platform;
        private String deviceModel;
        private String ipAddress;
        private String rawData;
    }

    // 需求1的数据模型
    @Data
    @Builder
    public static class PageVisitResult {
        private String date;
        private String pageType;
        private Long visitCount;
        private Long lastUpdateTime;
    }

    // 需求2的数据模型
    @Data
    @Builder
    public static class SearchWordResult {
        private String date;
        private String keyword;
        private Long count;
        private Long lastUpdateTime;
    }

    @Data
    @Builder
    public static class KeywordCount {
        private String keyword;
        private Long count;
        private Integer rank;
    }

    @Data
    @Builder
    public static class DailyTopNResult {
        private String date;
        private List<KeywordCount> topNKeywords;
        private Long calculationTime;
    }

    // ==================== 日志解析器 ====================
    public static class LogParser implements MapFunction<String, LogEvent> {
        @Override
        public LogEvent map(String value) throws Exception {
            try {
                JSONObject json = JSONObject.parseObject(value);

                // 解析基本字段
                String logId = json.getString("log_id");
                String logType = json.getString("log_type");
                String userId = json.getString("user_id");
                Long timestamp = json.getLong("ts");

                // 解析设备信息
                String deviceBrand = "unknown";
                String platform = "unknown";
                String deviceModel = "unknown";

                JSONObject device = json.getJSONObject("device");
                if (device != null) {
                    deviceBrand = device.getString("brand");
                    platform = device.getString("plat");
                    deviceModel = device.getString("device");

                    if (deviceBrand == null) deviceBrand = "unknown";
                    if (platform == null) platform = "unknown";
                    if (deviceModel == null) deviceModel = "unknown";
                }

                // 解析IP地址
                String ipAddress = "unknown";
                JSONObject gis = json.getJSONObject("gis");
                if (gis != null) {
                    ipAddress = gis.getString("ip");
                    if (ipAddress == null) ipAddress = "unknown";
                }

                // 设置默认值
                if (logId == null) logId = "unknown";
                if (logType == null) logType = "unknown";
                if (userId == null) userId = "anonymous";
                if (timestamp == null) timestamp = System.currentTimeMillis();

                LogEvent event = LogEvent.builder()
                        .logId(logId)
                        .logType(logType)
                        .userId(userId)
                        .timestamp(timestamp)
                        .deviceBrand(deviceBrand)
                        .platform(platform)
                        .deviceModel(deviceModel)
                        .ipAddress(ipAddress)
                        .rawData(value)
                        .build();

                System.out.println("📝 解析日志: " + logType + " | " + userId + " | " + timestamp);
                return event;

            } catch (Exception e) {
                System.err.println("❌ 解析日志失败: " + e.getMessage());
                return null;
            }
        }
    }
}