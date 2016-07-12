package com.alibaba.middleware.race.jstorm;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

public class ConsumerSpot extends BaseRichSpout 
    implements MessageListenerConcurrently{

	/**
	 * 
	 */
	private static final long serialVersionUID = -3085994102089532269L;

	private static Logger log = LoggerFactory.getLogger(ConsumerSpot.class);
	private transient SpoutOutputCollector collector;

	private transient DefaultMQPushConsumer consumer;
	
	protected transient LinkedBlockingDeque<MessageExt> all;

 
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {


		all = new LinkedBlockingDeque<MessageExt>();
		count2=new AtomicInteger(0);

		count4=new AtomicInteger(0);
		first=true;
		
		
		log.error("init DefaultMQPushConsumer");
		initConsumer();
		
		this.collector = collector;
	}
		
	public void initConsumer(){
		consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup+"1012");

		if (RaceConfig.TairGroup.equals("group_1")) {
			consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
			consumer.setNamesrvAddr("10.150.0.94:9876");
		}
		consumer.setConsumeMessageBatchMaxSize(32);
		try {
			consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
			consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
			consumer.subscribe(RaceConfig.MqPayTopic, "*");
		} catch (MQClientException e) {
			e.printStackTrace();
		}
		consumer.setInstanceName("consume-" + Thread.currentThread()+new Date());
		consumer.registerMessageListener(this);
		try {
			consumer.start();
		} catch (MQClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private transient Boolean first=true;

	@Override
	public void nextTuple() {
		
		try {
			int size=all.size();
			List<MessageExt> list=new ArrayList<MessageExt>();
			if (size>500) {
				for (int i = 0; i < 500; i++) {
					MessageExt msg=all.poll();
					if (msg!=null) 
						list.add(msg);
				}
			}else {
				for (int i = 0; i < size; i++) {
					MessageExt msg=all.poll();
					if (msg!=null) 
						list.add(msg);
				}
			}
			if (list.size()>0) 
				collector.emit(new Values(list));
			


			if (first) {
				log.error("nexttuple 启动了");
				first = false;
			}

		} catch (Exception e) {
			log.error("abcd"+e);
		}

	}
	

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("msgs"));
	}


	
	
	private transient  AtomicInteger count2;
	private transient  AtomicInteger count4;
	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,ConsumeConcurrentlyContext context) {
		if (msgs!=null) {
			all.addAll(msgs);
			return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
		}
		return ConsumeConcurrentlyStatus.RECONSUME_LATER;
	}
	
}