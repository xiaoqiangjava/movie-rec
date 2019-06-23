package com.xq.rec.kafka.stream;

import org.apache.kafka.clients.consumer.internals.NoAvailableBrokersException;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

/**
 * 使用kafka stream将订阅的日志信息进行数据预处理，得到用户评分数据
 * @author xiaoqiang
 * @date 2019/6/23 23:43
 */
public class Application
{
    private static String formTopic = "logs";

    private static String toTopic = "recommend";

    private static String brokers = "learn:9092";

    private static String zookeepers = "learn:2181";

    public static void main(String[] args)
    {
        // 定义kafka stream配置
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);

        StreamsConfig config = new StreamsConfig(props);

        // 创建一个拓扑构建器
        TopologyBuilder builder = new TopologyBuilder();
        builder.addSource("source", formTopic)
                .addProcessor("processor", () -> new LogProcessor(), "source")
                .addSink("sink", toTopic, "processor");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();
        System.out.println("Kafka stream started>>>>>>>>>>");
    }
}
