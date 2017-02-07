package com.focused_crawler.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.tuple.Tuple;


public class KafkaParserBolt extends BaseBasicBolt {
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    String[] values = tuple.getString(0).split("\\|");
    Integer oid = Integer.parseInt(values[0]);
    Integer sid = Integer.parseInt(values[1]);
    String  url = values[2];
    Double relevance = Double.parseDouble(values[3]);
    collector.emit(new Values(oid, sid, url, relevance));
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
    ofd.declare(new Fields("oid", "sid", "url", "relevance"));
  }
}
