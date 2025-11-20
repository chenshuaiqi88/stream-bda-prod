package com.stream.realtime.lululemon.Data_source;


import com.stream.core.EnvironmentSettingUtils;
import com.stream.core.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;
import java.util.Properties;

/**
 * @Package com.stream.FlinkSink2DorisTest
 * @Author zhou.han
 * @Date 2024/12/16 13:44
 * @description: Test
 */
public class FlinkSink2DorisTest {
    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettingUtils.defaultParameter(env);

        DataStreamSource<String> dataStreamSource = env.fromSource(
                KafkaUtils.buildKafkaSource("172.17.55.4:9092", "zh_test", new Date().toString(), OffsetsInitializer.latest()),
                WatermarkStrategy.noWatermarks(),
                "test-kafka"
        );

        dataStreamSource.print();

        dataStreamSource.sinkTo(sink2DorisFunc("biggta_realtime_report_v3.report_gmv_topn")).setParallelism(1);

        env.execute();
    }


    public static DorisSink<String> sink2DorisFunc(String tableName){
        Properties props = new Properties();
        props.setProperty("format", "json");
        props.setProperty("read_json_by_line", "true");

        return DorisSink.<String>builder()
                .setDorisReadOptions(DorisReadOptions.builder().build())
                .setDorisOptions(
                        DorisOptions.builder()
                                .setFenodes("192.168.200.31:9030")
                                .setTableIdentifier(tableName)
                                .setUsername("root")
                                .setPassword("")
                                .build()
                )
                .setDorisExecutionOptions(DorisExecutionOptions.builder() // 执行参数
                        .setLabelPrefix("doris_label_"+new Date().getTime())  // stream-load 导入的时候的 label 前缀
                        .disable2PC() // 开启两阶段提交后,labelPrefix 需要全局唯一,为了测试方便禁用两阶段提交
                        .setDeletable(false)
                        .setBufferCount(4) // 用于缓存stream load数据的缓冲条数: 默认 3
                        .setBufferSize(1024*1024) //用于缓存stream load数据的缓冲区大小: 默认 1M
                        .setMaxRetries(3)
                        .setStreamLoadProp(props) // 设置 stream load 的数据格式 默认是 csv,根据需要改成 json
                        .build())
                .setSerializer(new SimpleStringSerializer())
                .build();
    }
}
