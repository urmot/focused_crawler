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
import org.apache.storm.utils.Utils;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public class DocumentParserBolt extends BaseBasicBolt {
  public Set stopwords = new HashSet<String>();

  @Override
  public void prepare(Map stormConf, TopologyContext context) {
    Config theconf = new Config();
    theconf.putAll(Utils.readStormConfig());
    ClientBlobStore clientBlobStore = Utils.getClientBlobStore(theconf);
    try {
      InputStreamWithMeta blobInputStream = clientBlobStore.getBlob("stopwords");
      BufferedReader r = new BufferedReader(new InputStreamReader(blobInputStream));
      r.lines().forEach((stopword) -> stopwords.add(stopword));
      stopwords.add("");
      r.close();
    } catch (IOException | AuthorizationException | KeyNotFoundException exp) {
      throw new RuntimeException(exp);
    }
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    Integer oid = (int) (long) tuple.getLong(0);
    Integer sid = (int) (long) tuple.getLong(1);
    String url = tuple.getString(2);
    String html = tuple.getString(3);
    Document document = Jsoup.parse(html);
    List<String> words = new ArrayList<String>(Arrays.asList(document.text().split("[^a-zA-Z]")));

    words.removeAll(stopwords);
    if (words.size() > 70)  {
      collector.emit("docSizeStream", new Values(oid, sid, words.size(), url));
      Elements links = document.select("a[href]");
      for (Element e : links) collector.emit("linkStream", new Values(oid, e.attr("href")));
      Map<String, Integer> wordCount = new HashMap<String, Integer>();
      words.forEach((word) -> {
        Integer count = wordCount.getOrDefault(word, 0);
        wordCount.put(word, ++count);
      });
      wordCount.forEach((word, count) -> {
        collector.emit("wordStream", new Values(oid, word, count));
      });
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("linkStream", new Fields("src", "link"));
    declarer.declareStream("wordStream", new Fields("oid", "word", "count"));
    declarer.declareStream("docSizeStream", new Fields("oid", "sid", "size", "url"));
  }
}
