package com.focused_crawler.bolt;

import org.apache.storm.Config;
import org.apache.storm.state.KeyValueState;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseStatefulBolt;
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

public class StatefulPriorityControllerBolt extends BaseStatefulBolt<KeyValueState<String, Object>> {
  private OutputCollector collector;
  KeyValueState<String, Object> kvState;
  PriorityBlockingQueue<List> queue;
  List<Integer> taskIds;
  Map<Integer, List<Integer>> tasks;
  Map<Integer, Integer> taskCount;
  Map<Integer, Long> lastvisited;
  Map<Integer, Integer> serverloads;
  List<List> tmpQueue = new ArrayList<List>();
  Integer index = 0;

  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    taskIds = context.getComponentTasks("crawler");
    queue = new PriorityBlockingQueue<List>(1000, new ValuesComparator());
    serverloads = new HashMap<Integer, Integer>();
    this.collector = collector;
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
  public void execute(Tuple tuple) {
    switch (tuple.getSourceComponent()) {
    case "requestSpout":
      before(tuple);
      List link = getNextLink(tuple);
      if (link != null) {
        List task = tasks.remove(tuple.getInteger(0));
        Integer taskId;
        if (task == null) {
          taskId = taskIds.get(index);
          index++;
          if (index.equals(taskIds.size())) index = 0;
        } else {
          taskId = (Integer) task.get(0);
        }
        collector.emitDirect(taskId, "linkStream", tuple, link);
        after((Integer) link.get(1), tuple.getInteger(1), taskId);
        kvState.put("queue", queue);
        collector.ack(tuple);
      } else {
        collector.fail(tuple);
      }
      break;
    case "filter":
      queue.add(tuple.getValues());
      kvState.put("queue", queue);
      collector.ack(tuple);
      break;
    }
  }


  public List getNextLink(Tuple tuple) {
    List<List> tmp = new ArrayList<List>();
    List link = new ArrayList();
    while(queue.size() > 0) {
      link = queue.poll();
      Integer sid = (Integer) link.get(1);
      Long endTime = lastvisited.get(sid);
      if (endTime == null) break;
      if (endTime.equals(0l) || System.currentTimeMillis() - endTime < 30000)
       tmp.add(link);
      else
        break;
    }
    if (tmp.size() > 0) tmp.forEach(l -> queue.add(l));
    if (queue.size() == 0) return null;
    else return link;
  }

  public void before(Tuple tuple) {
    Integer msId = tuple.getInteger(0);
    List task = tasks.get(msId);
    if (tasks == null) {
      Integer taskId = (Integer) task.get(0);
      Integer sid = (Integer) task.get(1);
      Integer count = taskCount.get(taskId);
      taskCount.put(taskId, --count);
      lastvisited.put(sid, System.currentTimeMillis());

      kvState.put("taskCount", taskCount);
      kvState.put("lastvisited", lastvisited);
    }
  }

  public void after(Integer sid, Integer msId, Integer taskId) {
    tasks.put(msId, Arrays.asList(taskId, sid));
    Integer count = taskCount.getOrDefault(taskId, 0);
    taskCount.put(taskId, ++count);
    lastvisited.put(sid, 0l);
    Integer serverload = serverloads.getOrDefault(sid, 0);
    serverloads.put(sid, ++serverload);

    kvState.put("tasks", tasks);
    kvState.put("taskCount", taskCount);
    kvState.put("lastvisited", lastvisited);
    kvState.put("serverloads", serverloads);
  }

  @Override
  public void initState(KeyValueState<String, Object> state) {
    kvState = state;
    kvState.put("queue", queue);
    queue = (PriorityBlockingQueue<List>) kvState.get("queue", new PriorityBlockingQueue<List>(100, new ValuesComparator()));
    tasks = (Map<Integer, List<Integer>>) kvState.get("tasks", new HashMap<Integer, List<Integer>>());
    taskCount = (Map<Integer, Integer>) kvState.get("taskCount", new HashMap<Integer, Integer>());
    lastvisited = (Map<Integer, Long>) kvState.get("lastvisited", new HashMap<Integer, Long>());
    serverloads = (Map<Integer, Integer>) kvState.get("serverloads", new HashMap<Integer, Integer>());
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declareStream("linkStream", new Fields("oid", "sid", "url", "relevance"));
  }

  private class ValuesComparator implements Comparator {
  	@Override
  	public int compare (Object arg0, Object arg1) {
  		List x = (List) arg0;
  		List y = (List) arg1;

  		if ((Double) x.get(3) > (Double) y.get(3)) {
  			return -1;
  		} else if ((Double) x.get(3) < (Double) y.get(3)) {
  			return 1;
  		} else {
        Integer xSid = (Integer) x.get(1);
        Integer ySid = (Integer) y.get(1);
        if (serverloads.getOrDefault(xSid, 0) < serverloads.getOrDefault(ySid, 0))
          return -1;
        else
          return 1;
  		}
  	}
  }
}
