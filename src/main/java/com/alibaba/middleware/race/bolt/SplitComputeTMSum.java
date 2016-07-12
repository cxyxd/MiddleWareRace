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
import com.alibaba.middleware.race.model.MyStoreTM;
import com.alibaba.middleware.race.model.PaymentMessage;



public class SplitComputeTMSum implements IRichBolt {
	
	/**
	 *  
	 */
	private static final long serialVersionUID = 1495546200761129239L;
	private transient OutputCollector collector;
	protected static final Logger logger = LoggerFactory.getLogger(SplitComputeTMSum.class);


	private transient MyStoreTM tmSum;
	private transient AtomicInteger count;
	private transient StaticTairOperatorImpl tair;
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		tmSum =MyStoreTM.getInstance();
		count=new AtomicInteger(0);
		tair=StaticTairOperatorImpl.getInstance();
	}
	

	public void doStop(Tuple tuple) {
	//	logger.error("写入tair"+tmSum.size()+"  TM");
		 DecimalFormat df = new DecimalFormat("#.00");  
		for(Entry<String, Double> entry: tmSum.entrySet() ){
			String key=entry.getKey();
			Double value=entry.getValue();
			Object objFromTair=tair.get(key);
			double valueFromTair=-1;
			if (objFromTair!=null) {
				valueFromTair=Double.parseDouble(""+objFromTair);
			}
			if (valueFromTair==-1&&value-valueFromTair>1) 
				tair.put(entry.getKey(), Double.parseDouble(df.format(entry.getValue())));
		}
	}
	
	@Override
	public void execute(Tuple tuple) {

		try {

			String id = tuple.getSourceStreamId();
			if (id.equals("stop")) {
				// logger.error("i get stop");
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
	//	logger.error("这次收到 :"+list.size());
		count.addAndGet(list.size());
//		if (count.get()>0&&count.get()%10000<10) {
//			logger.error("我已经处理了: TM "+count.get()+ " "+Thread.currentThread());
//		}
		
		for (PaymentMessage pay:list) {
			long createTime=(pay.getCreateTime()/1000/60)*60;
			
			double payAmount = pay.getPayAmount();
			double sum = 0;
			
			if (id.equals(RaceConfig.MqTmallTradeTopic)) {
				if (tmSum.containsKey(RaceConfig.prex_tmall + createTime)) {
					sum = tmSum.get(RaceConfig.prex_tmall + createTime)+ payAmount;
					tmSum.put(RaceConfig.prex_tmall + createTime, sum);
			//		logger.error("加上"+createTime+" "+payAmount+"  tb 等于 "+sum);
				} else {
					tmSum.put(RaceConfig.prex_tmall + createTime, payAmount);
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