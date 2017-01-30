package com.focused_crawler.spout;

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.spout.Scheme;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;

public class SeedSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  Integer count = 0;

  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
  }

  @Override
  public void nextTuple() {
    if (count++ < 10) {
     _collector.emit("requestStream", new Values("poll"), count);
     Utils.sleep(10);
   }
  }

  @Override
  public void ack(Object id) {
    count++;
    _collector.emit("requestStream", new Values("poll"), count);
    System.out.println("Ack recieved to SeedSpout" + id);
  }

  @Override
  public void fail(Object id) {
    System.out.println("Fail recieved to SeedSpout" + id);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("requestStream", new Fields("request"));
  }
}
