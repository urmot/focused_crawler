package com.focused_crawler.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.topology.IRichBolt;

import java.util.Map;
import java.util.HashMap;
import java.io.IOException;

public class CrawlerBolt extends ShellBolt implements IRichBolt {
  public CrawlerBolt() {
    super("node", "simpleCrawler.js");
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("documentStream", new Fields("oid", "sid", "url", "html"));
    declarer.declareStream("updateStream", new Fields("oid", "sid", "url", "relevance"));
    declarer.declareStream("requestStream", new Fields("request"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return null;
  }
}
