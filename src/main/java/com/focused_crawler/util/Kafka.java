package com.focused_crawler.utils;

import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.Broker;
import org.apache.storm.kafka.trident.GlobalPartitionInformation;
import org.apache.storm.kafka.StaticHosts;
import org.apache.storm.kafka.bolt.KafkaBolt;
import org.apache.storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import org.apache.storm.kafka.bolt.selector.DefaultTopicSelector;
import org.apache.storm.spout.SchemeAsMultiScheme;

import java.util.UUID;
import java.util.Properties;

public class Kafka {
  private String host = "kafka:9092";
  private String topic = "test";

  public Kafka() {
  }

  public Kafka(String host, String topic) {
    this.host = host;
    this.topic = topic;
  }

  public Properties getProperties() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "kafka:9092");
    props.put("acks", "1");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return props;
  }

  public KafkaBolt getBolt() {
    Properties props = getProperties();
    KafkaBolt bolt = new KafkaBolt()
            .withProducerProperties(props)
            .withTopicSelector(new DefaultTopicSelector(topic))
            .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
    return bolt;
  }

  public KafkaSpout getSpout() {
    Broker brokerForPartition0 = new Broker(host, 9092);
    GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation(topic);
    partitionInfo.addPartition(0, brokerForPartition0);
    StaticHosts hosts = new StaticHosts(partitionInfo);
    SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());
    spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
    KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
    return kafkaSpout;
  }
}
