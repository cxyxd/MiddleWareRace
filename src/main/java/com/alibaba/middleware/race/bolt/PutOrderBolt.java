package com.alibaba.middleware.race.bolt;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.MyStoreOrder;
import com.alibaba.middleware.race.model.OrderMessage;

public class PutOrderBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6864266184551371654L;

	private transient MyStoreOrder orders;
	
	protected static final Logger   logger = LoggerFactory.getLogger(PutOrderBolt.class);  
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		orders=MyStoreOrder.getInstance();
		count=new AtomicInteger(0);
	}
	private AtomicInteger count;
	
	@SuppressWarnings("unchecked")
	@Override
	public void execute(Tuple input) {
		try {

			List<byte[]> bodys = (List<byte[]>) input.getValueByField("msg");

			String id = input.getSourceStreamId();
			count.addAndGet(bodys.size());
			if (count.get()%10000<20) {
				logger.error("我得到 订单"+count);
			}
			
			// logger.error("id: "+id);
			if (id.equals(RaceConfig.MqTaobaoTradeTopic)) {
				for (byte[] body : bodys) {
					OrderMessage orderMessage = RaceUtils.readKryoObject(
							OrderMessage.class, body);
					Long orderId = orderMessage.getOrderId();
					orders.put(orderId, "tb");
				}

			} else if (id.equals(RaceConfig.MqTmallTradeTopic)) {
				for (byte[] body : bodys) {
					OrderMessage orderMessage = RaceUtils.readKryoObject(
							OrderMessage.class, body);
					Long orderId = orderMessage.getOrderId();
					orders.put(orderId, "tm");
				}
			}
		} catch (Exception e) {
			logger.error("abcd"+e.getMessage());
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}
}