package com.alibaba.middleware.race.bolt;

import java.text.DecimalFormat;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.StaticTairOperatorImpl;

public class RateBolt extends BaseRichBolt {
	
	/**
	 *  
	 */
	private static final long serialVersionUID = -2071204313760739600L;
	private transient Map<Long, Double> wirelessSum;
	private transient Map<Long, Double> pcSum;
	
	private transient Map<Long, Double> wirelessAllSum;
	private transient Map<Long, Double> pcAllSum;
	private transient Long beginTime=2867083461L;
	private transient Long endTime=1L;
	private transient StaticTairOperatorImpl tair;
	
	protected static final Logger   logger = LoggerFactory.getLogger(RateBolt.class);  

	@SuppressWarnings("rawtypes")
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		wirelessSum = new ConcurrentHashMap<Long, Double>();
		pcSum = new ConcurrentHashMap<Long, Double>();

		wirelessAllSum = new ConcurrentHashMap<Long, Double>();
		pcAllSum = new ConcurrentHashMap<Long, Double>();
		
		beginTime=2867083461L;
		endTime=1L;
		tair=StaticTairOperatorImpl.getInstance();
	}

	public void execute(Tuple input) {
		try {

			String streamId = input.getSourceStreamId();
			if (streamId.equals("rate")) {
				doData(input);
			} else if (streamId.equals("stop")) {
				doStop();
			}
		} catch (Exception e) {
			logger.error("abcd"+e.getMessage());
		}

	}



	private void doStop() {

		double pc=pcSum.get(beginTime)==null?0:pcSum.get(beginTime);
		double wireless=wirelessSum.get(beginTime)==null?0:wirelessSum.get(beginTime);
		
		pcAllSum.put(beginTime, pc);
		wirelessAllSum.put(beginTime, wireless);
		
	//	logger.error("compute ratio: "+beginTime+" "+(wireless/pc));
		
		
		
	//	logger.error(beginTime+"  begin time");
		 DecimalFormat df;
		for (long i = beginTime+60; i < endTime; i+=60) {
			 pc=pcSum.get(i)==null?1:pcSum.get(i);
			 wireless=wirelessSum.get(i)==null?0:wirelessSum.get(i);
			double lastPcSum=pcAllSum.get(i-60)==null?0:pcAllSum.get(i-60);
			double lastWirelessSum=wirelessAllSum.get(i-60)==null?0:wirelessAllSum.get(i-60);
			pcAllSum.put(i, pc+lastPcSum);
			wirelessAllSum.put(i, wireless+lastWirelessSum);
			
			
			df = new DecimalFormat("#.00");
			String result=df.format(wirelessAllSum.get(i)/pcAllSum.get(i));
			double result_d=Double.parseDouble(result);
			
			Object fromTair=tair.get(RaceConfig.prex_ratio+i);
			
			if (fromTair!=null) {
				double fromTair_d=Double.parseDouble(""+fromTair);
				double tem=fromTair_d-result_d;
				if (Math.abs(tem)>0.01) {
					tair.put(RaceConfig.prex_ratio+i, result_d);
				}
			}else {
				tair.put(RaceConfig.prex_ratio+i, result_d);
			}
			
			
		} 

	}


	private void doData(Tuple input) {
		
		
		short payPlatform=input.getShortByField("payPlatform");
		double payAmount=input.getDoubleByField("payAmount");
		Long createTime=input.getLongByField("createTime");

		
		if (createTime<beginTime) {
			beginTime=createTime;
	//		logger.error(payAmount+" sp   "+beginTime);
		}
		if (createTime>endTime) {
			endTime=createTime;
		}
		
		//payPlatform==0  pc
		if (payPlatform==0) {
			if (pcSum.containsKey(createTime)) {
				pcSum.put(createTime, pcSum.get(createTime)+payAmount);
			}else {
				pcSum.put(createTime, payAmount);
			}
		}else if (payPlatform==1) { //   无线
			if (wirelessSum.containsKey(createTime)) {
				wirelessSum.put(createTime, wirelessSum.get(createTime)+payAmount);
			}else {
				wirelessSum.put(createTime, payAmount);
			}
		}
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

}
