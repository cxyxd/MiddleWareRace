package com.alibaba.middleware.race.jstorm;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class StopSpout extends BaseRichSpout  {

	private static final long serialVersionUID = 4404640091892228020L;
	protected static final Logger   logger = LoggerFactory.getLogger(StopSpout.class);  
	private transient SpoutOutputCollector collector;
	private transient Boolean isFirst=true;

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		isFirst=true;
	}



	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("stop",new Fields("stop"));
	}

	@Override
	public void nextTuple() {
		try {
				Thread.sleep(20 * 1000); // 睡眠10秒钟
				logger.error("我发出了一个延迟stop");
				collector.emit("stop", new Values("stop"));
		
		} catch (InterruptedException e) {
			logger.error("abcd"+e.getMessage());
		}
	}
	

}
