package com.focused_crawler.bolt;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class LinkParserBolt extends BaseBasicBolt {

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Integer src = tuple.getInteger(0);
    String url = tuple.getString(1);
    Integer oid = url.hashCode();
    try {
      String server = new URI(url).getHost();
      if (server != null) {
        Integer sid = server.hashCode();
        String[] link = {oid.toString(), sid.toString(), url};
        collector.emit("linkStream", new Values(String.join("|", link), src));
      }
    } catch (URISyntaxException ignore) {
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("linkStream", new Fields("link", "src"));
  }

  public void storeLink(Tuple tuple) {
  }
}
