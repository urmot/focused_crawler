package com.focused_crawler.bolt;

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.utils.Utils;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.blobstore.ClientBlobStore;
import org.apache.storm.blobstore.InputStreamWithMeta;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.KeyNotFoundException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.PriorityBlockingQueue;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

public class PriorityControllerBolt extends BaseBasicBolt {
  PriorityBlockingQueue<List> queue;
  Set crawled = new HashSet<String>();
  Map<Integer, Integer> serverloads = new HashMap<Integer, Integer>();

  @Override
  public void prepare(Map conf, TopologyContext context) {
    queue = new PriorityBlockingQueue(100, new ValuesComparator());
    Config theconf = new Config();
    theconf.putAll(Utils.readStormConfig());
    ClientBlobStore clientBlobStore = Utils.getClientBlobStore(theconf);
    try {
      InputStreamWithMeta blobInputStream = clientBlobStore.getBlob("seed");
      BufferedReader r = new BufferedReader(new InputStreamReader(blobInputStream));
      r.lines().forEach(url -> {
        try {
          Integer sid = new URI(url).getHost().hashCode();
          queue.add(new Values(url.hashCode(), sid, url, 0d));
        } catch (URISyntaxException exp) { }
      });
      r.close();
    } catch (IOException | AuthorizationException | KeyNotFoundException exp) {
      throw new RuntimeException(exp);
    }
  }

  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    switch (tuple.getSourceStreamId()) {
    case "requestStream":
      List link = queue.poll();
      if (link == null) {
        collector.emit("linkStream", new Values(null, null, null, null));
      } else {
        collector.emit("linkStream", link);
        Integer sid = (Integer) link.get(1);
        Integer serverload = serverloads.getOrDefault(sid, 0);
        serverloads.put(sid, serverload++);
      }
      break;
    case "linkStream":
      Integer oid = tuple.getInteger(0);
      if (!crawled.contains(oid)) {
        crawled.add(oid);
        queue.add(tuple.getValues());
      }
      break;
    }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("linkStream", new Fields("oid", "sid", "url", "relevance"));
  }

  public class ValuesComparator implements Comparator {
  	@Override
  	public int compare (Object arg0, Object arg1) {
  		List x = (List) arg0;
  		List y = (List) arg1;

  		if ((Double) x.get(3) > (Double) y.get(3)) {
  			return 1;
  		} else if ((Double) x.get(3) < (Double) y.get(3)) {
  			return -1;
  		} else {
        Integer xSid = (Integer) x.get(1);
        Integer ySid = (Integer) y.get(1);
        if (serverloads.getOrDefault(xSid, 0) < serverloads.getOrDefault(ySid, 0))
          return 1;
        else
          return -1;
  		}
  	}
  }
}
