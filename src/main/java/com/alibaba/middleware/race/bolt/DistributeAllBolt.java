package com.alibaba.middleware.race.bolt;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.common.message.MessageExt;


public class DistributeAllBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6338550887902994018L;
	private static Logger log = LoggerFactory.getLogger(DistributeAllBolt.class);
	private transient OutputCollector collector=null;
	protected transient LinkedBlockingDeque<MessageExt> orders_tb_Deque;
	protected transient LinkedBlockingDeque<MessageExt> orders_tm_Deque;
	protected transient LinkedBlockingDeque<MessageExt> psysDeque;
	
	private transient  AtomicInteger count3;
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector=collector;
		orders_tb_Deque = new LinkedBlockingDeque<MessageExt>();
		orders_tm_Deque = new LinkedBlockingDeque<MessageExt>();
		psysDeque = new LinkedBlockingDeque<MessageExt>();
		
		count3=new AtomicInteger(0);
	}

	@Override
	public void execute(Tuple input) {
		distributeAll(input);
		emit();
	}
	private void emit() {
		emitTb();
		emitTm();
		
		// 发送给rate的pay的字段
		// 发送给judge tm or tb的是整个pojo
	//	log.error("paysdeque size: "+psysDeque.size());
		if (psysDeque.size() > 200) {
			do200();
		} else {
			doless200();
		}
		
	} 
	
	private void emitTm() {
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

	private void emitTb() {
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
		
	}

	@SuppressWarnings("unchecked")
	private void distributeAll(Tuple input) {
		List<MessageExt> all=(List<MessageExt>) input.getValueByField("msgs");
		
	//	log.error("distributeAll "+size);
		
		for (MessageExt msg:all) {
			String topic;
			if (msg!=null&&msg.getBody().length>2) {
				topic=msg.getTopic();
				if (topic.equals(RaceConfig.MqPayTopic)) {
					psysDeque.offer(msg);
				}else if (topic.equals(RaceConfig.MqTaobaoTradeTopic)) {
					orders_tb_Deque.offer(msg);
				}else if(topic.equals(RaceConfig.MqTmallTradeTopic)) {
					orders_tm_Deque.offer(msg);
				}
			}
			
		}
		
	}
	
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
				log.error(paymentMessageList.size()+" 总计给judge发送了 "+count3.get());
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
				log.error(paymentMessageList.size()+" 总计给judge发送了 "+count3.get());
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

}
