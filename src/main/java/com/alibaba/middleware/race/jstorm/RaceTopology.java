package com.alibaba.middleware.race.jstorm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.security.jca.GetInstance;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.StaticTairOperatorImpl;
import com.alibaba.middleware.race.bolt.DistributeAllBolt;
import com.alibaba.middleware.race.bolt.JudgeTBorTMBolt;
import com.alibaba.middleware.race.bolt.PutOrderBolt;
import com.alibaba.middleware.race.bolt.RateBolt;
import com.alibaba.middleware.race.bolt.SplitComputeTBSum;
import com.alibaba.middleware.race.bolt.SplitComputeTMSum;

/**
  SELECT count(*) from pay;
  
  delete from pay;
  
  
  SELECT SUM(payAmount),platform,createTime from pay group by
 platform,createTime ORDER BY platform,createTime;



jstorm jar preliminary.demo-1.0-SNAPSHOT.jar  com.alibaba.middleware.race.jstorm.RaceTopology 41500fyqmw


jstorm kill 41500fyqmw


1 修改pom
2 看raceconfig
3 看stopspout
4 consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup+"a");
5 看启动类
7 logback.xml

1468188300 

http://ali-middleware-race.oss-cn-shanghai.aliyuncs.com/41500fyqmw.tar.xz


      
      
  全部并发是可以的  他妈的 以后不准再改了    
      
 */
public class RaceTopology {
	
	private static Logger logger = LoggerFactory.getLogger(RaceTopology.class);
	
	private static TopologyBuilder createBuilder() {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("consumer", new ConsumerSpot(), parallelism_hint).setNumTasks(numTask);
		builder.setSpout("continue_stop", new StopSpout(), 1).setNumTasks(1);

		builder.setBolt("distribute", new DistributeAllBolt(), parallelism_hint).setNumTasks(numTask)
		.shuffleGrouping("consumer");
		
		
		builder.setBolt("putorder", new PutOrderBolt(), parallelism_hint).setNumTasks(numTask)
				.shuffleGrouping("distribute", RaceConfig.MqTaobaoTradeTopic)
				.shuffleGrouping("distribute", RaceConfig.MqTmallTradeTopic);

		builder.setBolt("judgeTBorTB", new JudgeTBorTMBolt(), numWorker).setNumTasks(numWorker)
				.shuffleGrouping("distribute", RaceConfig.MqPayTopic);

		builder.setBolt("bolt_TM", new SplitComputeTMSum(), parallelism_hint).setNumTasks(numTask)
				.allGrouping("continue_stop", "stop")
				.shuffleGrouping("judgeTBorTB", RaceConfig.MqTmallTradeTopic);

		builder.setBolt("bolt_TB", new SplitComputeTBSum(), parallelism_hint).setNumTasks(numTask)
				.allGrouping("continue_stop", "stop")
				.shuffleGrouping("judgeTBorTB", RaceConfig.MqTaobaoTradeTopic);

		builder.setBolt("rate3", new RateBolt(), numWorker).setNumTasks(numWorker)
				.fieldsGrouping("distribute", "rate", new Fields("createTime"))
				.allGrouping("continue_stop", "stop");
		
		return builder; 
	}
	
	

	// private static Logger logger =
	// LoggerFactory.getLogger(RaceTopology.class);

	private static int numTask=1;
	private static int parallelism_hint=1;
	private static int numWorker=1;
	public static void main(String[] args) throws Exception {
	//	tair
	//	StaticTairOperatorImpl.getin   .put(1, 1);
	//	System.out.println(StaticTairOperatorImpl.get(1));
		StaticTairOperatorImpl.getInstance().put(1, 1);
		Config conf = new Config();
		conf.setNumWorkers(numWorker);
		conf.setNumAckers(0);

		 TopologyBuilder builder = createBuilder();


		
		if (args!=null&&args.length>0) {
			try {
				StormSubmitter.submitTopology(RaceConfig.JstormTopologyName,
						conf, builder.createTopology());
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(RaceConfig.JstormTopologyName, conf,
					builder.createTopology());
		}

	}
	
	



}