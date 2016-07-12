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

public class EmitPaymentSpot extends BaseRichSpout 
    implements MessageListenerConcurrently{

	/**
	 * 
	 */
	private static final long serialVersionUID = -3085994102089532269L;

	private static Logger log = LoggerFactory.getLogger(EmitPaymentSpot.class);
	private SpoutOutputCollector collector;

	private transient DefaultMQPushConsumer consumer;
	
	protected transient LinkedBlockingDeque<MessageExt> all;
	protected transient LinkedBlockingDeque<MessageExt> orders_tb_Deque;
	protected transient LinkedBlockingDeque<MessageExt> orders_tm_Deque;
	protected transient LinkedBlockingDeque<MessageExt> psysDeque;
 
	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {

		orders_tb_Deque = new LinkedBlockingDeque<MessageExt>();
		orders_tm_Deque = new LinkedBlockingDeque<MessageExt>();
		psysDeque = new LinkedBlockingDeque<MessageExt>();
		all = new LinkedBlockingDeque<MessageExt>();
		count2=new AtomicInteger(0);
		count3=new AtomicInteger(0);
		count4=new AtomicInteger(0);
		first=true;
		
		
		log.error("init DefaultMQPushConsumer");
		initConsumer();
		
		this.collector = collector;
	}
		
	public void initConsumer(){
		consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup+"1003");

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
		distributeAll();
		
	

			if (first) {
				log.error("nexttuple 启动了");
				first = false;
			}

			// 给putorderbolt发送信息 类型是List<byte[]>
			emitForPutOrderBolt();

			// 发送给rate的pay的字段
			// 发送给judge tm or tb的是整个pojo
			if (psysDeque.size() > 200) {
				do200();
			} else {
				doless200();
			}
		} catch (Exception e) {
			log.error("abcd"+e);
		}

	}

	private void distributeAll() {
		for (int i = 0; i <500; i++) {
			MessageExt msg=all.poll();
			if (msg!=null&&msg.getBody().length>2) {
				if (msg!=null&&msg.getTopic().equals(RaceConfig.MqPayTopic)) {
					psysDeque.offer(msg);
				}else if (msg!=null&&msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)) {
					orders_tb_Deque.offer(msg);
				}else if(msg!=null&&msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)) {
					orders_tm_Deque.offer(msg);
				}
			}
			
		}
		
	}

	/**
	 * 给putorderbolt发送信息  类型是List<byte[]>
	 */
	private void emitForPutOrderBolt() {
		List<byte[]> tb_body=new ArrayList<byte[]>();
		if (orders_tb_Deque.size()>100) {
			for (int i = 0; i < 100; i++) {
				MessageExt msg=orders_tb_Deque.poll();
				if (msg!=null) 
					tb_body.add(msg.getBody());				
			}
		}else {
			//为什么吧size取出来 自己想
			int size=orders_tb_Deque.size();
			for (int i = 0; i <size ; i++) {
				MessageExt msg=orders_tb_Deque.poll();
				if (msg!=null) 
					tb_body.add(msg.getBody());
			}
		}
		if (tb_body.size()>0) 
			collector.emit(RaceConfig.MqTaobaoTradeTopic, new Values(tb_body));
		
		
		
		
		
		List<byte[]> tm_body=new ArrayList<byte[]>();
		if (orders_tm_Deque.size()>100) {
			for (int i = 0; i < 100; i++) {
				MessageExt msg=orders_tm_Deque.poll();
				if (msg!=null) 
					tm_body.add(msg.getBody());				
			}
		}else {
			int size=orders_tm_Deque.size();
			for (int i = 0; i < size; i++) {
				MessageExt msg=orders_tm_Deque.poll();
				if (msg!=null) 
					tm_body.add(msg.getBody());
			}
		}
		if (tm_body.size()>0) 
			collector.emit(RaceConfig.MqTmallTradeTopic, new Values(tm_body));
		
	}

	private transient  AtomicInteger count3;
	private void doless200() {
		List<PaymentMessage> paymentMessageList=new ArrayList<PaymentMessage>();
		int size=psysDeque.size();
		for (int i = 0; i <size ; i++) {
			PaymentMessage paymentMessage = RaceUtils.readKryoObject(
					PaymentMessage.class, psysDeque.poll().getBody());
			
			//发给rate 没办法这得一个一个发
			long createTime=(paymentMessage.getCreateTime()/1000/60)*60;
			collector.emit("rate",new Values(paymentMessage.getPayPlatform(),paymentMessage.getPayAmount(),createTime));
			paymentMessageList.add(paymentMessage);
		}
		//judge tm or tb
		if (paymentMessageList.size()>0) {
			collector.emit(RaceConfig.MqPayTopic,new Values(paymentMessageList));
			count3.addAndGet(paymentMessageList.size());
			if (count3.get()%20000<50) 
				log.error("给judge发送了 "+count3.get());
		}
		
		
		
	}

	private void do200() {
		List<PaymentMessage> paymentMessageList=new ArrayList<PaymentMessage>();
		for (int i = 0; i < 200; i++) {
			PaymentMessage paymentMessage = RaceUtils.readKryoObject(
					PaymentMessage.class, psysDeque.poll().getBody());
			
			//发给rate 没办法这得一个一个发
			long createTime=(paymentMessage.getCreateTime()/1000/60)*60;
			collector.emit("rate",new Values(paymentMessage.getPayPlatform(),paymentMessage.getPayAmount(),createTime));
			paymentMessageList.add(paymentMessage);
		}
		//judge tm or tb
		if (paymentMessageList.size()>0) {
			collector.emit(RaceConfig.MqPayTopic,new Values(paymentMessageList));
			count3.addAndGet(paymentMessageList.size());
			if (count3.get()%20000<100) 
				log.error("给judge发送了 "+paymentMessageList.size());
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		//这两个是给putOrder的
		declarer.declareStream(RaceConfig.MqTmallTradeTopic,new Fields("msg"));
		declarer.declareStream(RaceConfig.MqTaobaoTradeTopic,new Fields("msg"));
		
		//这个是给 judge的
		declarer.declareStream(RaceConfig.MqPayTopic,new Fields("pays"));
		
		//这个给rate
		declarer.declareStream("rate",new Fields("payPlatform","payAmount","createTime"));
	}


	
	
	private transient  AtomicInteger count2;
	private transient  AtomicInteger count4;
	@Override
	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,ConsumeConcurrentlyContext context) {
	
		
		if (msgs!=null) {
	//		count2.addAndGet(msgs.size());
	//		if (count2.get()%200000<30) 
	//			log.error("consumer 收到:"+count2.get()+"  "+Thread.currentThread());
		
			all.addAll(msgs);
			return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
		}
		return ConsumeConcurrentlyStatus.RECONSUME_LATER;
		
		/*
		for (MessageExt msg : msgs) {
			byte[] body = msg.getBody();
			if (body.length >2) {
				all.offer(msg);
//				String topic=msg.getTopic();
				
//				if (topic.equals(RaceConfig.MqPayTopic)) {
//					count4.incrementAndGet();
//					psysDeque.offer(msg);
//				}else if (topic.equals(RaceConfig.MqTaobaoTradeTopic)) {
//					orders_tb_Deque.offer(msg);
//				} else if  (topic.equals(RaceConfig.MqTmallTradeTopic)) {
//					orders_tm_Deque.offer(msg);
//				} 
				
			}else {
			//	count2.decrementAndGet();
			}
		}
//		if (count2.get()>(Producer.count-100)*3) {
//			log.error("consumer 收到:"+count2.get()+"  "+Thread.currentThread());
//		}
//		if (count4.get()>(Producer.count-100)*2) {
//			log.error("订单  收到:"+count4.get()+"  "+Thread.currentThread());
//		}
		

		return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
		*/
	}
	
	
}