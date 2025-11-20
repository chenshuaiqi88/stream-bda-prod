package com.stream.realtime.lululemon.FlinkAPI_Doris;


import com.alibaba.fastjson2.JSONObject;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.Properties;

public class PageViewToDorisJob {
    private static final String OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC = "realtime_v3_logs";
    private static final String KAFKA_BOTSTRAP_SERVERS = "172.17.55.4:9092";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);


        DataStreamSource<String> source = env.fromSource(
                KafkaUtils.buildKafkaSource(KAFKA_BOTSTRAP_SERVERS, OMS_ORDER_INFO_REALTIME_ORIGIN_TOPIC, new Date().toString(), OffsetsInitializer.earliest()),
                WaterMarkUtils.publicAssignWatermarkStrategy("ts", 5L),
                "_log_kafka_source_realtime_v3_logs"
        );

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


        // 数据处理逻辑
        SingleOutputStreamOperator<Tuple3<String, String, Long>> processedStream = parsed
                .keyBy(jsonObject -> jsonObject.getString("log_date") + "_" + jsonObject.getString("log_type"))
                .map(new MapFunction<JSONObject, Tuple3<String, String, Long>>() {
                    @Override
                    public Tuple3<String, String, Long> map(JSONObject jsonObject) {
                        String logDate = jsonObject.getString("log_date");
                        String logType = jsonObject.getString("log_type");
                        return Tuple3.of(logDate, logType, 1L);
                    }
                })
                .keyBy(t -> t.f0 + "_" + t.f1)
                .timeWindow(Time.seconds(10))
                .sum(2);


        SingleOutputStreamOperator<String> jsonStream = processedStream
                .map(new MapFunction<Tuple3<String, String, Long>, String>() {
                    @Override
                    public String map(Tuple3<String, String, Long> value) {
                        JSONObject json = new JSONObject();
                        json.put("ds", value.f0);
                        json.put("page_name", value.f1);
                        json.put("pv", value.f2);
                        String jsonStr = json.toString();
                        System.out.println("----------->>>> " + jsonStr);
                        return jsonStr;
                    }
                });

        // 配置 Doris Sink
        DorisSink<String> dorisSink = buildDorisSink();

        // 将数据写入 Doris

        jsonStream.sinkTo(dorisSink)
                .name("doris-page-view-sink")
                .uid("doris-page-view-sink");


        env.execute("Page View Stats to Doris");
    }


    private static DorisSink<String> buildDorisSink() {
        // Doris 连接配置
        DorisOptions dorisOptions = DorisOptions.builder()
                .setFenodes("192.168.200.31:18030")
                .setTableIdentifier("bigdata_realtime_lululemon_report_v2.page_view_stats")
                .setUsername("root")
                .setPassword("")
                .build();

        // 配置执行参数
        Properties properties = new Properties();
        properties.setProperty("format", "json");
        properties.setProperty("strip_outer_array", "true");
        properties.setProperty("sink.batch.size", "3");
        properties.setProperty("sink.batch.interval", "3000");
        properties.setProperty("sink.max-retries", "3");
        properties.setProperty("sink.enable-2pc", "false");

        // Doris 执行配置
        DorisExecutionOptions executionOptions = DorisExecutionOptions
                .builder()
                .setStreamLoadProp(properties)
                .build();

        // 序列化器 - 直接使用 JSON 字符串
        SimpleStringSerializer serializer = new SimpleStringSerializer();

        System.out.println("🚀 Doris Sink 配置完成，使用 18030 端口");


        return DorisSink.<String>builder()
                .setDorisOptions(dorisOptions)
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisExecutionOptions(executionOptions)
                .setSerializer(serializer)
                .build();


    }



}