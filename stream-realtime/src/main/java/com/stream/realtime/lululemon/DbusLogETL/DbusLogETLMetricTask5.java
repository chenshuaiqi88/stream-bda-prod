package com.stream.realtime.lululemon.DbusLogETL;

import com.alibaba.fastjson2.JSONObject;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;

import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import java.util.Date;


public class DbusLogETLMetricTask5 {

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




       //  5. 历史天 + 当天 用户设备的统计(ios & az (子类品牌(版本)))
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

        parsed
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
                .map(t -> String.format("%s | %s | %s | %s | %d", t.f0, t.f1, t.f2, t.f3, t.f4))
                .print();




        env.execute("User Path Analysis");



    }
    }