package com.stream.realtime.lululemon.DbusLogETL;

import com.alibaba.fastjson2.JSONObject;
import com.stream.core.KafkaUtils;
import com.stream.core.WaterMarkUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.FlatMapFunction;
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


import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;



public class DbusLogETLMetricTask6 {

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




        SingleOutputStreamOperator<JSONObject> parsed = source.flatMap((FlatMapFunction<String, JSONObject>) (s, collector) -> {
            try {
                JSONObject jsonObject = JSONObject.parseObject(s);
                Long ts = jsonObject.getLong("ts");
                if (ts == null) return;

                // 秒级时间戳转毫秒
                if (ts < 1000000000000L) ts = ts * 1000;
                jsonObject.put("ts", ts);

                // 格式化时间
                LocalDateTime dateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(ts), ZoneId.of("Asia/Shanghai"));
                String timeStr = dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                String dateStr = dateTime.toLocalDate().toString();
                int hour = dateTime.getHour();

                jsonObject.put("log_time", timeStr);
                jsonObject.put("log_date", dateStr);
                jsonObject.put("hour", hour);

                collector.collect(jsonObject);
            } catch (Exception ignored) {}
        });

        DataStream<String> userPortrait = parsed
                .keyBy(json -> json.getString("user_id"))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .process(new ProcessWindowFunction<JSONObject, String, String, TimeWindow>() {
                    @Override
                    public void process(String userId,
                                        Context context,
                                        Iterable<JSONObject> elements,
                                        Collector<String> out) {

                        Set<String> loginDates = new HashSet<>();
                        boolean hasPayment = false;
                        boolean hasSearch = false;
                        boolean hasBrowse = false;
                        int minLoginHour = 24, maxLoginHour = -1;

                        for (JSONObject json : elements) {
                            String type = json.getString("log_type");
                            String date = json.getString("log_date");
                            int hour = json.getIntValue("hour");

                            if ("login".equals(type)) {
                                loginDates.add(date);
                                minLoginHour = Math.min(minLoginHour, hour);
                                maxLoginHour = Math.max(maxLoginHour, hour);
                            } else if ("payment".equals(type)) {
                                hasPayment = true;
                            } else if ("search".equals(type)) {
                                hasSearch = true;
                            } else {
                                hasBrowse = true;
                            }
                        }

                        String loginTimeRange = (minLoginHour <= maxLoginHour)
                                ? String.format("%d点-%d点", minLoginHour, maxLoginHour)
                                : "无登录";

                        String timePeriod = getTimePeriod(minLoginHour, maxLoginHour);

                        // ✅ 改成 JSON 输出
                        JSONObject result = new JSONObject();
                        result.put("user_id", userId);
                        result.put("login_days", loginDates.size());
                        result.put("login_dates", String.join(",", loginDates));
                        result.put("has_payment", hasPayment);
                        result.put("has_search", hasSearch);
                        result.put("has_browse", hasBrowse);
                        result.put("login_time_range", loginTimeRange);
                        result.put("time_period", timePeriod);
                        result.put("window_end", new Date(context.window().getEnd()).toString());

                        out.collect(result.toJSONString());
                    }

                    private String getTimePeriod(int startHour, int endHour) {
                        if (startHour == 24 || endHour == -1) return "未知";
                        if (endHour <= 5) return "凌晨(1-5)";
                        if (endHour <= 12) return "早上(6-12)";
                        if (endHour <= 14) return "中午(12-14)";
                        if (endHour <= 18) return "下午(14-18)";
                        return "晚上(18-24)";
                    }
                });


                userPortrait.print();


        // ✅ 2. 按 user_id 聚合用户画像
//        DataStream<String> userPortrait = parsed
//                .keyBy(json -> json.getString("user_id"))
//                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
//                .process(new ProcessWindowFunction<JSONObject, String, String, TimeWindow>() {
//                    @Override
//                    public void process(String userId,
//                                        Context context,
//                                        Iterable<JSONObject> elements,
//                                        Collector<String> out) {
//
//                        Set<String> loginDates = new HashSet<>();
//                        boolean hasPayment = false;
//                        boolean hasSearch = false;
//                        boolean hasBrowse = false;
//                        int minLoginHour = 24, maxLoginHour = -1;
//
//                        for (JSONObject json : elements) {
//                            String type = json.getString("log_type");
//                            String date = json.getString("log_date");
//                            int hour = json.getIntValue("hour");
//
//                            if ("login".equals(type)) {
//                                loginDates.add(date);
//                                minLoginHour = Math.min(minLoginHour, hour);
//                                maxLoginHour = Math.max(maxLoginHour, hour);
//                            } else if ("payment".equals(type)) {
//                                hasPayment = true;
//                            } else if ("search".equals(type)) {
//                                hasSearch = true;
//                            } else {
//                                hasBrowse = true;
//                            }
//                        }
//
//                        String loginTimeRange = (minLoginHour <= maxLoginHour)
//                                ? String.format("%d点-%d点", minLoginHour, maxLoginHour)
//                                : "无登录";
//
//                        // ✅ 新增时段判断逻辑
//                        String timePeriod = getTimePeriod(minLoginHour, maxLoginHour);
//
//                        // ✅ 拼接最终结果字符串
//                        String result = String.format(
//                                "用户ID: %s | 登录天数: %d | 登录日期: %s | 购买: %s | 搜索: %s | 浏览: %s | 登录时间段: %s | 时段: %s",
//                                userId,
//                                loginDates.size(),
//                                String.join(",", loginDates),
//                                hasPayment ? "是" : "否",
//                                hasSearch ? "是" : "否",
//                                hasBrowse ? "是" : "否",
//                                loginTimeRange,
//                                timePeriod
//                        );
//
//                        out.collect(result);
//                    }
//
//                    // ✅ 放在 ProcessWindowFunction 内部定义即可（私有方法）
//                    private String getTimePeriod(int startHour, int endHour) {
//                        if (startHour == 24 || endHour == -1) return "未知";
//
//                        if (endHour <= 12 && startHour >= 6) {
//                            return "早上(6-12)";
//                        } else if (startHour >= 12 && endHour <= 14) {
//                            return "中午(12-14)";
//                        } else if (startHour >= 14 && endHour <= 18) {
//                            return "下午(14-18)";
//                        } else if (startHour >= 18 && endHour <= 24) {
//                            return "晚上(18-24)";
//                        }else if (startHour >= 1 && endHour < 6) {
//                            return "凌晨(1-5)";
//                        } else {
//                            // 若跨越多个时段，根据结束时间大致判断主要时段
//                            if (endHour <= 5) return "凌晨(1-5)";
//                            if (endHour <= 12) return "早上(6-12)";
//                            if (endHour <= 14) return "中午(12-14)";
//                            if (endHour <= 18) return "下午(14-18)";
//                            return "晚上(18-24)";
//                        }
//                    }
//                });
//
//
//        // ✅ 输出画像
//        userPortrait.print();




        env.execute("User Path Analysis");



    }
}