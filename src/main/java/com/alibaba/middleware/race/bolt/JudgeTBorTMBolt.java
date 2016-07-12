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
import com.alibaba.middleware.race.model.MyStoreOrder;
import com.alibaba.middleware.race.model.PaymentMessage;


public class JudgeTBorTMBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 5771174544851852079L;
	private transient MyStoreOrder orders;
	private OutputCollector collector;
	protected static final Logger   logger = LoggerFactory.getLogger(RateBolt.class);  

	protected transient LinkedBlockingDeque<PaymentMessage> failedPayment;
	private AtomicInteger count;

	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		orders=MyStoreOrder.getInstance();
		this.collector=collector;
		count=new AtomicInteger(0);
		failedPayment=new LinkedBlockingDeque<PaymentMessage>();

		
		/**
		 * 为啥要加这个函数?
		 * 因为有可能 前一个bolt发送的消息里 还有在order里找不到信息的payment
		 * 设计一个定时器 如果再5s之内 这个next方法没有被访问(我就任务 这是前面的bolt最后一次给我发消息了)
		 *  那我就再调用一一次execute
		 * 
		 */
		
	}

	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		try {

			List<PaymentMessage> pays = (List<PaymentMessage>) input.getValueByField("pays");


			List<PaymentMessage> tbList = new ArrayList<PaymentMessage>();
			List<PaymentMessage> tmList = new ArrayList<PaymentMessage>();
			PaymentMessage failedMessage = null;


			// 为什么要取出来 自己想
			int fails = failedPayment.size();
			for (int i = 0; i < fails; i++) {
				failedMessage = failedPayment.poll();
				if (failedMessage != null)
					pays.add(failedMessage);
			}

			for (PaymentMessage pay : pays) {

				String from = orders.get(pay.getOrderId());
				if (from == null) {
					failedPayment.offer(pay);
					continue;
				}

				if (from.equals("tb")) {
					tbList.add(pay);
				} else if (from.equals("tm")) {
					tmList.add(pay);
				}

			}

			if (tbList.size() > 0) {
				count.addAndGet(tbList.size());
				if (count.get() % 10000 < 10)
					logger.error(count+"前面是总计 我是judge 给tb发送 " + tbList.size());

				collector.emit(RaceConfig.MqTaobaoTradeTopic,
						new Values(tbList));

			}
			if (tmList.size() > 0) {
				collector
						.emit(RaceConfig.MqTmallTradeTopic, new Values(tmList));
			}

		} catch (Exception e) {
			logger.error("abcd"+e.getMessage());
		}
		
	}




	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
			
			//发送给splitcomputeTmsum and splitcomputeTbsum
			declarer.declareStream(RaceConfig.MqTmallTradeTopic,new Fields("list"));
			declarer.declareStream(RaceConfig.MqTaobaoTradeTopic,new Fields("list"));
		
	}
}