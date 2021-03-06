package com.alibaba.middleware.race.bolt;

import java.text.DecimalFormat;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.alibaba.middleware.race.DBUtil;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.StaticTairOperatorImpl;
import com.alibaba.middleware.race.model.MyStoreTB;
import com.alibaba.middleware.race.model.PaymentMessage;



public class SplitComputeTBSum implements IRichBolt {
	
	private static final long serialVersionUID = 1495546200761129239L;
	OutputCollector collector;
	protected static final Logger logger = LoggerFactory.getLogger(SplitComputeTBSum.class);


	private transient MyStoreTB taobaoSum;
	private transient  AtomicInteger count;
	private transient StaticTairOperatorImpl tair;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		taobaoSum =MyStoreTB.getInstance();
		count=new AtomicInteger(0);
		tair=StaticTairOperatorImpl.getInstance();
	}

	
	public void doStop(Tuple tuple) {
		logger.error("写入tair  "+taobaoSum.size()+"   TB");

		 DecimalFormat df = new DecimalFormat("#.00");  
		 int i=0;
		for(Entry<String, Double> entry: taobaoSum.entrySet() ){
			String key=entry.getKey();
			Double value=entry.getValue();
			Object objFromTair=tair.get(key);
			double valueFromTair=-1;
			if (objFromTair!=null) {
				valueFromTair=Double.parseDouble(""+objFromTair);
			}
			if (valueFromTair==-1&&value-valueFromTair>1) 
				tair.put(entry.getKey(), Double.parseDouble(df.format(entry.getValue())));
//			i++;
//			if (i<10) {
//				logger.error("tair 具体数值"+entry.getKey()+"---"+tair.get(entry.getKey()).toString());
//			}
		}
	}
	
	@Override
	public void execute(Tuple tuple) {
		try {

			String id = tuple.getSourceStreamId();
			if (id.equals("stop")) {
				logger.error("i get stop");
				doStop(tuple);
			} else {
				compute(tuple);
				if (RaceConfig.TairGroup.equals("group_1")
						&& RaceConfig.writeSQL) 
					DBUtil.wirteMySQL(tuple);
				
			}
		} catch (Exception e) {
			logger.error("abcd"+e.getMessage());
		}

	}

	@SuppressWarnings("unchecked")
	private void compute(Tuple tuple) {
		String id = tuple.getSourceStreamId();
		List<PaymentMessage> list=(List<PaymentMessage>) tuple.getValueByField("list");
		count.addAndGet(list.size());
		logger.error("这次收到 :"+list.size());
		if (count.get()>0&&count.get()%10000<10) {
			logger.error("我已经处理了: TB "+count.get()+ " "+Thread.currentThread());
		}
		for (PaymentMessage pay:list) {
			long createTime=(pay.getCreateTime()/1000/60)*60;
			
			double payAmount = pay.getPayAmount();
			double sum = 0;
			
			if (id.equals(RaceConfig.MqTaobaoTradeTopic)) {
				if (taobaoSum.containsKey(RaceConfig.prex_taobao + createTime)) {
					sum = taobaoSum.get(RaceConfig.prex_taobao + createTime)+ payAmount;
					taobaoSum.put(RaceConfig.prex_taobao + createTime, sum);
			//		logger.error("加上"+createTime+" "+payAmount+"  tb 等于 "+sum);
				} else {
					taobaoSum.put(RaceConfig.prex_taobao + createTime, payAmount);
			//		logger.error("最初 "+createTime+" "+payAmount+"  tb  "+taobaoSum.size());
				}
			}
		}
		
			

		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}


	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	

		
	
}