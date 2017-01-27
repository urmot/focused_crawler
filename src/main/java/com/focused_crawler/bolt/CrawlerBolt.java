package com.focused_crawler.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;
import java.io.IOException;

public class CrawlerBolt extends BaseRichBolt {
  private static final Logger LOG = LoggerFactory.getLogger(CrawlerBolt.class);
  OutputCollector _collector;
  String userAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_1) AppleWebKit/602.2.14 (KHTML, like Gecko) Version/10.0.1 Safari/602.2.14";

  @Override
  public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
    _collector = collector;
    _collector.emit("requestStream", new Values("poll"));
  }

  @Override
  public void execute(Tuple tuple) {
    _collector.emit("requestStream", new Values("poll"));
    try {
      Document document = Jsoup.connect(tuple.getString(2)).userAgent(userAgent).get();
      _collector.emit("fileStream", new Values(tuple.getInteger(1), tuple.getString(2), document.outerHtml()));
      Elements links = document.select("a[href]");
      _collector.emit("documentStream", new Values(tuple.getInteger(0), tuple.getInteger(1), document.text(), tuple.getString(2)));
      for (Element e : links) {
        _collector.emit("linkStream", new Values(tuple.getInteger(0), e.attr("href")));
      }
    } catch (IOException exp) {
      LOG.debug("http/get failed: " + tuple.getString(2));
      LOG.debug(exp.getMessage());
      _collector.emit("updateStream", tuple.getValues());
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("documentStream", new Fields("oid", "sid", "document", "url"));
    declarer.declareStream("updateStream", new Fields("oid", "sid", "url", "relevance"));
    declarer.declareStream("linkStream", new Fields("oid", "link"));
    declarer.declareStream("requestStream", new Fields("request"));
    declarer.declareStream("fileStream", new Fields("sid", "url", "html"));
  }
}
