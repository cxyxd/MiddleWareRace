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



jstorm jar preliminary.demo-1.0-SNAPSHOT.jar  com.alibaba.middleware.race.jstorm.RaceTopology
jstorm kill 41500fyqmw


1 修改pom
2 看raceconfig
3 看stopspout
4 consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup+"a");
5 看启动类
7 logback.xml

1468188300 

http://ali-middleware-race.oss-cn-shanghai.aliyuncs.com/41500fyqmw.tar.xz


      选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
      因为我们后台对选手的git进行下载打包
      拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology； 所以这个主类路径一定要正确
      
      
  全部并发是可以的  他妈的 以后不准再改了    
      
 */
public class RaceTopology {
	
	private static Logger logger = LoggerFactory.getLogger(RaceTopology.class);
	
	private static TopologyBuilder createBuilder() {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout3", new EmitPaymentSpot(), 2).setNumTasks(2);
		builder.setSpout("continue_stop", new StopSpout(), 1).setNumTasks(1);

		builder.setBolt("putorder", new PutOrderBolt(), 2).setNumTasks(2)
				.shuffleGrouping("spout3", RaceConfig.MqTaobaoTradeTopic)
				.shuffleGrouping("spout3", RaceConfig.MqTmallTradeTopic);

		builder.setBolt("judgeTBorTB", new JudgeTBorTMBolt(), 3).setNumTasks(3)
				.shuffleGrouping("spout3", RaceConfig.MqPayTopic);

		builder.setBolt("bolt_TM", new SplitComputeTMSum(), 2).setNumTasks(2)
				.allGrouping("continue_stop", "stop")
				.shuffleGrouping("judgeTBorTB", RaceConfig.MqTmallTradeTopic);

		builder.setBolt("bolt_TB", new SplitComputeTBSum(), 2).setNumTasks(2)
				.allGrouping("continue_stop", "stop")
				.shuffleGrouping("judgeTBorTB", RaceConfig.MqTaobaoTradeTopic);

		builder.setBolt("rate3", new RateBolt(), 3).setNumTasks(3)
				.fieldsGrouping("spout3", "rate", new Fields("createTime"))
				.allGrouping("continue_stop", "stop");
		
		return builder; 
	}
	
	

	// private static Logger logger =
	// LoggerFactory.getLogger(RaceTopology.class);

	private static int numTask=2;
	private static int parallelism_hint=2;
	private static int numWorker=3;
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