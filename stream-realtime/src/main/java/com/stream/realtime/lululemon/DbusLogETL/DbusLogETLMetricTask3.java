package com.stream.realtime.lululemon.DbusLogETL;

import com.alibaba.fastjson2.JSONObject;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.lionsoul.ip2region.xdb.Searcher;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DbusLogETLMetricTask3 {

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


        // 3. 历史天 + 当天 登陆区域的全国热力情况(每个地区的访问值)
        DataStream<Tuple3<String, String, Long>> regionStream = source
                .flatMap(new RichFlatMapFunction<String, JSONObject>() {
                    private transient Searcher searcher;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        String dbPath = "D:\\idea\\daima\\zg6\\stream-bda-prod\\stream-realtime\\src\\main\\java\\com\\stream\\realtime\\lululemon\\func\\ip2region_v4.xdb";
                        searcher = Searcher.newWithFileOnly(dbPath);
                    }

                    @Override
                    public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                        JSONObject jsonObject = JSONObject.parseObject(s);
                        Long ts = jsonObject.getLong("ts");
                        if (ts == null) return;

                        if (ts < 1000000000000L) ts *= 1000;
                        LocalDate localDate = Instant.ofEpochMilli(ts)
                                .atZone(ZoneId.of("Asia/Shanghai"))
                                .toLocalDate();
                        jsonObject.put("log_date", localDate.toString());

                        JSONObject gis = jsonObject.getJSONObject("gis");
                        if (gis == null) return;
                        String ip = gis.getString("ip");
                        if (ip == null || ip.isEmpty()) return;

                        String region = searcher.search(ip);
                        String province = "未知";
                        String city = "未知";

                        if (region != null && !region.isEmpty()) {
                            String[] parts = region.split("\\|");
                            String addr = null;
                            for (String p : parts) {
                                if (p.contains("省") || p.contains("自治区") || p.contains("市")) {
                                    addr = p;
                                    break;
                                }
                            }

                            if (addr != null) {
                                if (addr.contains("省")) {
                                    province = addr.substring(0, addr.indexOf("省") + 1);
                                    if (addr.length() > addr.indexOf("省") + 1) {
                                        city = addr.substring(addr.indexOf("省") + 1);
                                    } else {
                                        city = province; // 如果没有市，就直接用省
                                    }
                                } else if (addr.contains("自治区")) {
                                    province = addr.substring(0, addr.indexOf("自治区") + 3);
                                    if (addr.length() > addr.indexOf("自治区") + 3) {
                                        city = addr.substring(addr.indexOf("自治区") + 3);
                                    } else {
                                        city = province;
                                    }
                                } else if (addr.contains("市")) {
                                    province = addr;
                                    city = province; // 特殊情况：如果是直辖市，城市等于省份
                                }
                            }
                        }

                        String fullRegion = city.equals(province) ? province : province + city; // 确保不重复拼接


                        jsonObject.put("province", province);
                        jsonObject.put("city", city);
                        jsonObject.put("full_region", fullRegion);

                        collector.collect(jsonObject);
                    }
                })
                .filter(json -> "login".equals(json.getString("log_type")))
                .map(json -> Tuple3.of(
                        json.getString("log_date"),
                        json.getString("full_region"),
                        1L
                ))
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
                .keyBy(t -> t.f0 + "_" + t.f1)
                .reduce(new ReduceFunction<Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> reduce(Tuple3<String, String, Long> v1, Tuple3<String, String, Long> v2) {
                        return Tuple3.of(v1.f0, v1.f1, v1.f2 + v2.f2);
                    }
                });

        regionStream.print();












        env.execute("Login Region Heatmap");



    }
    }