package com.stream.realtime.lululemon.Two_stream_join.Final_version;

import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 三流 CoProcessFunction Join 处理器 - 主类
 */
public class KFK_log_comment {

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 配置序列化
        env.getConfig().enableForceAvro();
        env.getConfig().enableForceKryo();

        // 注册自定义类型
        env.getConfig().registerTypeWithKryoSerializer(
                DataModel.UserBehaviorHistory.class,
                com.esotericsoftware.kryo.serializers.JavaSerializer.class
        );
        env.getConfig().registerTypeWithKryoSerializer(
                DataModel.SearchRecord.class,
                com.esotericsoftware.kryo.serializers.JavaSerializer.class
        );

        // 配置重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, // 重启尝试次数
                org.apache.flink.api.common.time.Time.seconds(10) // 重启间隔
        ));

        // 1. 创建数据源
        KafkaSource<String> commentSource = DataSourceManager.createCommentSource();
        KafkaSource<String> logsSource = DataSourceManager.createLogsSource();
        KafkaSource<String> userInfoSource = DataSourceManager.createUserInfoSource();

        // 2. 创建数据流
        DataStream<String> commentStream = env.fromSource(commentSource, WatermarkStrategy.noWatermarks(), "comment-security-source");
        DataStream<String> logsStream = env.fromSource(logsSource, WatermarkStrategy.noWatermarks(), "realtime-logs-source");
        DataStream<String> userInfoStream = env.fromSource(userInfoSource, WatermarkStrategy.noWatermarks(), "user-info-source");

        // 3. 调试输出原始数据
        DataSourceManager.debugRawData(commentStream, logsStream, userInfoStream);

        // 4. 解析数据流
        DataStream<DataModel.CommentData> parsedCommentStream = DataProcessor.parseCommentStream(commentStream);
        DataStream<DataModel.LogData> parsedLogStream = DataProcessor.parseLogStream(logsStream);
        DataStream<DataModel.UserInfoData> parsedUserInfoStream = DataProcessor.parseUserInfoStream(userInfoStream);

        // 5. 流连接处理
        SingleOutputStreamOperator<DataModel.CommentLogJoinResult> commentLogJoinedStream = 
            StreamUtils.joinCommentWithLog(parsedCommentStream, parsedLogStream);

        SingleOutputStreamOperator<DataModel.FinalJoinResult> finalJoinedStream =
                StreamUtils.joinWithUserInfo(commentLogJoinedStream, parsedUserInfoStream);


        // 6. 输出最终结果
        StreamUtils.printFinalResults(finalJoinedStream);

        env.execute("KFK-log");



    }
}