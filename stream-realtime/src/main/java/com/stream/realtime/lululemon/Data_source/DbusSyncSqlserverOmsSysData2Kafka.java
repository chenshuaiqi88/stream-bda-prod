package com.stream.realtime.lululemon.Data_source;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.stream.core.ConfigUtils;
import com.stream.core.EnvironmentSettingUtils;
import com.stream.core.KafkaUtils;
import com.stream.realtime.lululemon.func.MapMergeJsonData;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Properties;

public class DbusSyncSqlserverOmsSysData2Kafka {

    private static final String OMS_OREDR_INFO_REALTIME_ORIGIN_TOPIC = "realtime_v3_order_info";
    private static final String KAFKA_BOTSTRAP_SERVERS = "172.17.55.4:9092";

    @SneakyThrows
    public static void main(String[] args) {

        // 判断是否存在 没有就创建，有就删除
        boolean KafkaTopicDelFlag = KafkaUtils.kafkaTopicExists(KAFKA_BOTSTRAP_SERVERS,OMS_OREDR_INFO_REALTIME_ORIGIN_TOPIC);
        // 三分区 一副本
        KafkaUtils.createKafkaTopic(KAFKA_BOTSTRAP_SERVERS,OMS_OREDR_INFO_REALTIME_ORIGIN_TOPIC,3,(short)1,KafkaTopicDelFlag );

        System.setProperty("HADOOP_USER_NAME","root");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);


        Properties debeziumProperties = new Properties();
        debeziumProperties.put("snapshot.mode", "initial");
        debeziumProperties.put("database.history.store.only.monitored.tables.ddl", "true");
        debeziumProperties.put("snapshot.locking.mode", "none");
        debeziumProperties.put("snapshot.fetch.size", 200);
        DebeziumSourceFunction<String> sqlServerSource = SqlServerSource.<String>builder()
                .hostname("172.17.55.4")
                .port(1433)
                .username("sa")
                .password("Hth1028,./")
                .database("realtime_v3")
                .tableList("dbo.oms_order_dtl")
                .startupOptions(StartupOptions.latest())
                .debeziumProperties(debeziumProperties)
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = env.addSource(sqlServerSource, "_transaction_log_source1");
        SingleOutputStreamOperator<JSONObject> convertStr2JsonDs = dataStreamSource.map(JSON::parseObject)
                .uid("convertStr2JsonDs")
                .name("convertStr2JsonDs");


        SingleOutputStreamOperator<JSONObject> processData = convertStr2JsonDs.map(new MapMergeJsonData());

//        processData.print();



        // 临时方案：硬编码Kafka地址
        String bootstrapServers = "172.17.55.4:9092"; // 根据你的实际Kafka地址修改

// 或者 String bootstrapServers = "cdh01:9092,cdh02:9092,cdh03:9092";

        processData.map(JSONObject::toString)
                .sinkTo(KafkaUtils.buildKafkaSink(bootstrapServers, OMS_OREDR_INFO_REALTIME_ORIGIN_TOPIC))
                .uid("kafkaSinkOperator")
                .name("kafkaSinkOperator");





//        processData
//                .map(JSONObject::toString)
//                .sinkTo(
//                KafkaUtils.buildKafkaSinkOrigin(KAFKA_BOTSTRAP_SERVERS,OMS_OREDR_INFO_REALTIME_ORIGIN_TOPIC)
//        );








        env.execute("DbusSyncSqlserverOmsSysData2Kafka");
    }



}
