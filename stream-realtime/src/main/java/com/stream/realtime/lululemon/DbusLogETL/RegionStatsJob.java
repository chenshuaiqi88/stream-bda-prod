package com.stream.realtime.lululemon.DbusLogETL;


import com.alibaba.fastjson.JSONObject;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.lionsoul.ip2region.xdb.Searcher;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.List;

public class RegionStatsJob {
    private static final String OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC = "realtime_v3_logs";
    private static final String KAFKA_BOTSTRAP_SERVERS = "172.17.55.4:9092";

    public static void main(String[] args) throws Exception {



        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.fromSource(
                KafkaUtils.buildKafkaSource(KAFKA_BOTSTRAP_SERVERS, OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC, new Date().toString(), OffsetsInitializer.earliest()),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts", 5L),
                "_log_kafka_source_realtime_v3_logs"
        );

        SingleOutputStreamOperator<JSONObject> parsed = source.flatMap(new RichFlatMapFunction<String, JSONObject>() {
            private transient Searcher searcher;

            @Override
            public void open(Configuration parameters) throws Exception {
                String dbPath = "D:\\idea\\daima\\zg6\\stream-bda-prod\\stream-realtime\\src\\main\\java\\com\\stream\\realtime\\lululemon\\func\\ip2region_v4.xdb";
                searcher = Searcher.newWithFileOnly(dbPath);
            }

            @Override
            public void flatMap(String s, Collector<JSONObject> collector)  {
                try {
                    JSONObject jsonObject = JSONObject.parseObject(s);
                    Long ts = jsonObject.getLong("ts");
                    if (ts == null) return;

                    if (ts < 1000000000000L) ts *= 1000;
                    LocalDate logDate = Instant.ofEpochMilli(ts)
                            .atZone(ZoneId.of("Asia/Shanghai"))
                            .toLocalDate();
                    jsonObject.put("log_date", logDate.toString());

                    JSONObject gis = jsonObject.getJSONObject("gis");
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

        // ✅ 筛选登录日志并聚合
        DataStream<Tuple4<String, String, String, Long>> regionStream = parsed
                .filter(json -> "login".equals(json.getString("log_type")))
                .map(json -> Tuple4.of(
                        json.getString("log_date"),
                        json.getString("province"),
                        json.getString("city"),
                        1L
                ))
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG))
                .keyBy(t -> t.f0 + "_" + t.f1 + "_" + t.f2)
                .reduce((ReduceFunction<Tuple4<String, String, String, Long>>) (v1, v2) ->
                        Tuple4.of(v1.f0, v1.f1, v1.f2, v1.f3 + v2.f3));

        // ✅ 3. 按日期排序（基于时间窗口排序输出）
        DataStream<String> sortedStream = regionStream
                .keyBy(t -> "all")
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple4<String, String, String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<Tuple4<String, String, String, Long>> elements,
                                        Collector<String> out) {

                        List<Tuple4<String, String, String, Long>> list = new ArrayList<>();
                        elements.forEach(list::add);

                        // ✅ 按日期 + 省份 + 城市升序排序
                        list.sort(Comparator
                                .comparing((Tuple4<String, String, String, Long> t) -> t.f0)
                                .thenComparing(t -> t.f1)
                                .thenComparing(t -> t.f2));

                        for (Tuple4<String, String, String, Long> item : list) {
                            out.collect(String.format("(%s, %s, %s, %d)", item.f0, item.f1, item.f2, item.f3));
                        }
                    }
                });

        sortedStream.print();



        env.execute("Region Province-City Statistics Job");
    }
}
