package com.focused_crawler.bolt;

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Document;

import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;

public class DocumentParserBolt extends BaseBasicBolt {

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Integer oid = (int) (long) tuple.getLong(0);
    Integer sid = (int) (long) tuple.getLong(1);
    String url = tuple.getString(2);
    String html = tuple.getString(3);

    Document document = Jsoup.parse(html);
    Elements links = document.select("a[href]");
    for (Element e : links)
      collector.emit("linkStream", new Values(oid, e.attr("href")));

    List<String> words = new ArrayList<String>(Arrays.asList(document.text().split("\\W")));
    if (words.size() > 100) {
      collector.emit("docSizeStream", new Values(oid, sid, words.size(), url));
      words.stream().forEach(word -> {
        collector.emit("wordStream", new Values(oid, word));
      });
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("linkStream", new Fields("src", "link"));
    declarer.declareStream("wordStream", new Fields("oid", "word"));
    declarer.declareStream("docSizeStream", new Fields("oid", "sid", "size", "url"));
  }
}
