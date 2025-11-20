package com.stream.realtime.lululemon.DbusLogETL;

import com.alibaba.fastjson2.JSONObject;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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

public class DbusLogETLMetricTask4 {

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




// ✅ 2. 解析 JSON + 时间标准化
        SingleOutputStreamOperator<JSONObject> parsed = source.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    Long ts = jsonObject.getLong("ts");
                    if (ts == null) return;

                    // 判断是否是秒级时间戳
                    if (ts < 1000000000000L) {
                        ts = ts * 1000;
                    }
                    jsonObject.put("ts", ts);

                    // 添加格式化时间字段，便于调试
                    String timeStr = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.of("Asia/Shanghai"))
                            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                    jsonObject.put("log_time", timeStr);

                    collector.collect(jsonObject);
                } catch (Exception ignored) {
                }
            }
        });

// 聚合数据，按用户路径进行统计
        parsed
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
                })
                .print();








        env.execute("User Path Analysis");



    }
    }