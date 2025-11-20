package com.stream.realtime.lululemon.Two_stream_join.Final_version;

import com.stream.core.KafkaUtils;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * 数据源管理类
 */
public class DataSourceManager {
    
    public static KafkaSource<String> createCommentSource() {
        return KafkaUtils.buildKafkaSource(
            "172.17.55.4:9092",
            "comment-security-results",
            "flink-comment-reader",
            OffsetsInitializer.earliest()
        );
    }
    
    public static KafkaSource<String> createLogsSource() {
        return KafkaUtils.buildKafkaSource(
            "172.17.55.4:9092",
            "realtime_v3_logs",
            "flink-logs-reader",
            OffsetsInitializer.earliest()
        );
    }
    
    public static KafkaSource<String> createUserInfoSource() {
        return KafkaUtils.buildKafkaSource(
            "172.17.55.4:9092",
            "user_info_enhanced",
            "flink-userinfo-reader",
            OffsetsInitializer.earliest()
        );
    }
    
    public static void debugRawData(DataStream<String> commentStream, 
                                   DataStream<String> logsStream, 
                                   DataStream<String> userInfoStream) {
        commentStream
            .map(record -> {
                System.out.println("📥 [RAW-COMMENT] " + record);
                return record;
            })
            .name("comment-processor");

        logsStream
            .map(record -> {
                System.out.println("🟢 [realtime_v3_logs] " + record);
                return record;
            })
            .name("logs-processor");

        userInfoStream
            .map(record -> {
                System.out.println("🟣 [user_info_enhanced] " + record);
                return record;
            })
            .name("user-info-processor");
    }
}