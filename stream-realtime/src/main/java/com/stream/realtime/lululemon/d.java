package com.stream.realtime.lululemon;

import com.stream.core.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class d {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取 comment-security-results 主题
        KafkaSource<String> commentSource = KafkaUtils.buildKafkaSource(
                "172.17.55.4:9092",
                "comment-security-results",
                "flink-comment-reader",
                OffsetsInitializer.earliest()
        );

        // 读取 realtime_v3_logs 主题
        KafkaSource<String> logsSource = KafkaUtils.buildKafkaSource(
                "172.17.55.4:9092",
                "realtime_v3_logs",
                "flink-logs-reader",
                OffsetsInitializer.earliest()
        );

        // 处理 comment-security-results 数据
        DataStream<String> commentStream = env.fromSource(
                commentSource,
                WatermarkStrategy.noWatermarks(),
                "comment-security-source"
        );

        // 处理 realtime_v3_logs 数据
        DataStream<String> logsStream = env.fromSource(
                logsSource,
                WatermarkStrategy.noWatermarks(),
                "realtime-logs-source"
        );

        // 输出原始数据用于调试
//        commentStream
//                .map(record -> {
//                    System.out.println("🔵 [comment-security-results] " + record);
//                    return record;
//                })
//                .name("comment-processor");

        logsStream
                .map(record -> {
                    System.out.println("🟢 [realtime_v3_logs] " + record);
                    return record;
                })
                .name("logs-processor");







        env.execute();
    }
}
