package com.stream.realtime.lululemon.DbusLogETL;

import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.lionsoul.ip2region.xdb.Searcher;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class DbusLogETLMetricTask {

    private static final String OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC = "realtime_v3_logs";
    private static final String KAFKA_BOTSTRAP_SERVERS = "172.17.55.4:9092";
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> source = env.fromSource(
                KafkaUtils.buildKafkaSource(KAFKA_BOTSTRAP_SERVERS, OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC, new Date().toString(), OffsetsInitializer.earliest()),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts", 5L),
                "_log_kafka_source_realtime_v3_logs"
        );





        //      1. 历史天 + 当天 每个页面的总体访问量
        SingleOutputStreamOperator<JSONObject> parsed = source.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) {
                JSONObject jsonObject = JSONObject.parseObject(s);
                Long ts = jsonObject.getLong("ts");
                if (ts == null) return;
                // ✅ 判断是秒还是毫秒
                if (ts < 1000000000000L) { // 小于 1 万亿说明是秒级
                    ts = ts * 1000;
                }

                // 3️⃣ 转为日期字符串（本地时区）
                LocalDate localDate = Instant.ofEpochMilli(ts)
                        .atZone(ZoneId.of("Asia/Shanghai"))
                        .toLocalDate();

                jsonObject.put("log_date", localDate.toString());

                collector.collect(jsonObject);
            }
        });


        SingleOutputStreamOperator<String> page_views = parsed.keyBy(jsonObject -> jsonObject.getString("log_date") + "_" + jsonObject.getString("log_type"))
                .map(new MapFunction<JSONObject, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(JSONObject jsonObject) {
                        String logDate = jsonObject.getString("log_date");
                        String logType = jsonObject.getString("log_type");
                        return Tuple3.of(logDate, logType, 1L);
                    }
                })
                .keyBy(t -> t.f0 + "_" + t.f1)
                .timeWindow(Time.days(1))
                .sum(2)
                .map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value) {
                        return String.format("日期: %s, 页面: %s, PV: %d", value.f0, value.f1, value.f2);
                    }
                });




        // 2️⃣ 保留 search 日志并展开 keywords
        DataStream<Tuple3<String, String, Long>> keywordStream = parsed.flatMap(new FlatMapFunction<JSONObject, Tuple3<String, String, Long>>() {
            @Override
            public void flatMap(JSONObject json, Collector<Tuple3<String, String, Long>> out)  {
                if (!"search".equals(json.getString("log_type"))) return;
                JSONArray kws = json.getJSONArray("keywords");
                if (kws == null || kws.isEmpty()) return;

                String logDate = json.getString("log_date");
                for (Object kwObj : kws) {
                    String kw = kwObj.toString().trim();
                    if (kw.length() > 0) {
                        out.collect(Tuple3.of(logDate, kw, 1L));
                    }
                }
            }
        });

        // 3️⃣ 每天每个关键词计数
        DataStream<Tuple3<String, String, Long>> keywordCount = keywordStream
                .keyBy(t -> t.f0 + "_" + t.f1)
                .sum(2);

        // 4️⃣ 按天聚合 Top10
        SingleOutputStreamOperator<String> hot_words_top10 = keywordCount
                .keyBy(t -> t.f0)
                .process(new DbusLogETLMetricTask2.TopNProcessFunction(10))
                .keyBy(v -> 1)
                .process(new DbusLogETLMetricTask2.SortAndDedupOutput());


        SingleOutputStreamOperator<com.alibaba.fastjson.JSONObject> parsedd = source.flatMap(new RichFlatMapFunction<String, com.alibaba.fastjson.JSONObject>() {
            private transient Searcher searcher;

            @Override
            public void open(Configuration parameters) throws Exception {
                String dbPath = "D:\\idea\\daima\\zg6\\stream-bda-prod\\stream-realtime\\src\\main\\java\\com\\stream\\realtime\\lululemon\\func\\ip2region_v4.xdb";
                searcher = Searcher.newWithFileOnly(dbPath);
            }

            @Override
            public void flatMap(String s, Collector<com.alibaba.fastjson.JSONObject> collector)  {
                try {
                    com.alibaba.fastjson.JSONObject jsonObject = com.alibaba.fastjson.JSONObject.parseObject(s);
                    Long ts = jsonObject.getLong("ts");
                    if (ts == null) return;

                    if (ts < 1000000000000L) ts *= 1000;
                    LocalDate logDate = Instant.ofEpochMilli(ts)
                            .atZone(ZoneId.of("Asia/Shanghai"))
                            .toLocalDate();
                    jsonObject.put("log_date", logDate.toString());

                    com.alibaba.fastjson.JSONObject gis = jsonObject.getJSONObject("gis");
                    if (gis == null) return;
                    String ip = gis.getString("ip");
                    if (ip == null || ip.isEmpty()) return;

                    // ✅ IP转行政区
                    String region = searcher.search(ip);
                    String[] area = CityProvinceMapper.getProvinceAndCity(region);
                    String province = area[0];
                    String city = area[1];

                    jsonObject.put("province", province);
                    jsonObject.put("city", city);

                    collector.collect(jsonObject);
                } catch (Exception ignored) {}
            }

        });

        // 3 登陆区域的全国热力情况
//        DataStream<Tuple4<String, String, String, Long>> regionStream = parsedd
//                .filter(json -> "login".equals(json.getString("log_type")))
//                .map(json -> Tuple4.of(
//                        json.getString("log_date"),
//                        json.getString("province"),
//                        json.getString("city"),
//                        1L
//                ))
//                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG))
//                .keyBy(t -> t.f0 + "_" + t.f1 + "_" + t.f2)
//                .reduce((ReduceFunction<Tuple4<String, String, String, Long>>) (v1, v2) ->
//                        Tuple4.of(v1.f0, v1.f1, v1.f2, v1.f3 + v2.f3));
//
//
//        // ✅ 3. 按日期排序（基于时间窗口排序输出）
//        DataStream<String> regional_heat = regionStream
//                .keyBy(t -> "all")
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
//                .process(new ProcessWindowFunction<Tuple4<String, String, String, Long>, String, String, TimeWindow>() {
//                    @Override
//                    public void process(String key,
//                                        Context context,
//                                        Iterable<Tuple4<String, String, String, Long>> elements,
//                                        Collector<String> out) {
//
//                        List<Tuple4<String, String, String, Long>> list = new ArrayList<>();
//                        elements.forEach(list::add);
//
//                        // ✅ 按日期 + 省份 + 城市升序排序
//                        list.sort(Comparator
//                                .comparing((Tuple4<String, String, String, Long> t) -> t.f0)
//                                .thenComparing(t -> t.f1)
//                                .thenComparing(t -> t.f2));
//
//                        for (Tuple4<String, String, String, Long> item : list) {
//                            out.collect(String.format("(%s, %s, %s, %d)", item.f0, item.f1, item.f2, item.f3));
//                        }
//                    }
//                });





        // 4 历史天 + 当天 路径分析
        SingleOutputStreamOperator<String> Path_analysis = parsed
                .keyBy(json -> json.getString("user_id"))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<JSONObject, Tuple3<String, String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(String userId, Context context, Iterable<JSONObject> input, Collector<Tuple3<String, String, Integer>> out) {
                        List<JSONObject> events = new ArrayList<>();
                        input.forEach(events::add);
                        events.sort(Comparator.comparingLong(e -> e.getLong("ts")));

                        for (int i = 0; i < events.size() - 1; i++) {
                            String from = events.get(i).getString("log_type");
                            String to = events.get(i + 1).getString("log_type");
                            Long ts = events.get(i + 1).getLong("ts");

                            if (from != null && to != null && !from.equals(to)) {
                                String dateStr = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.of("Asia/Shanghai"))
                                        .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
                                out.collect(Tuple3.of(dateStr, from + "→" + to, 1));
                            }
                        }
                    }
                })
                .keyBy(new KeySelector<Tuple3<String, String, Integer>, Tuple2<String, String>>() {
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
                        List<Tuple3<String, String, Integer>> results = new ArrayList<>();
                        input.forEach(results::add);
                        results.sort(Comparator.comparing(t -> t.f0));
                        for (Tuple3<String, String, Integer> t : results) {
                            out.collect(String.format("%s (%s,%d)", t.f0, t.f1, t.f2));
                        }
                    }
                });



        // 5用户设备的统计(ios & anzhuo (子类品牌(版本)))
        SingleOutputStreamOperator<String> User_device_statistics = parsed
                .flatMap(new FlatMapFunction<JSONObject, Tuple5<String, String, String, String, String>>() {
                    @Override
                    public void flatMap(JSONObject json, Collector<Tuple5<String, String, String, String, String>> out) {
                        JSONObject device = json.getJSONObject("device");
                        if (device == null) return;

                        String userkey = device.getString("userkey");
                        String brand = device.getString("brand");
                        String plat = device.getString("plat");
                        String platv = device.getString("platv");
                        Long ts = json.getLong("ts");

                        if (userkey == null || plat == null) return;

                        String dt = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.of("Asia/Shanghai"))
                                .format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));

                        out.collect(Tuple5.of(dt, plat, brand, platv, userkey));
                    }
                })
                .keyBy(new KeySelector<Tuple5<String, String, String, String, String>, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> getKey(Tuple5<String, String, String, String, String> t) {
                        return Tuple4.of(t.f0, t.f1, t.f2, t.f3);
                    }
                })
                .process(new KeyedProcessFunction<Tuple4<String, String, String, String>, Tuple5<String, String, String, String, String>, Tuple5<String, String, String, String, Integer>>() {

                    private transient MapState<String, Boolean> userSet;

                    @Override
                    public void open(Configuration parameters) {
                        MapStateDescriptor<String, Boolean> desc = new MapStateDescriptor<>("userSet", String.class, Boolean.class);
                        userSet = getRuntimeContext().getMapState(desc);
                    }

                    @Override
                    public void processElement(Tuple5<String, String, String, String, String> value, Context ctx, Collector<Tuple5<String, String, String, String, Integer>> out) throws Exception {
                        if (!userSet.contains(value.f4)) {
                            userSet.put(value.f4, true);
                            int size = 0;
                            for (String ignored : userSet.keys()) size++;
                            out.collect(Tuple5.of(value.f0, value.f1, value.f2, value.f3, size));
                        }
                    }
                })
                .map(t -> String.format("%s | %s | %s | %s | %d", t.f0, t.f1, t.f2, t.f3, t.f4));




//        User_device_statistics.print("用户设备的统计-->");
//
//        Path_analysis.print("路径分析-->");
//
//        regional_heat.print("每天地区热力情况-->");
//
//        hot_words_top10.print("每天热词top10-->");
//
//        page_views.print("每个页面的总体访问量--> ");





        env.execute("DbusLogETLMetricTask");
    }
}
