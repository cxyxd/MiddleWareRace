package com.alibaba.middleware.race.Tair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;

/**
 * @author WangJianmin
 * @date 2014-7-9
 * @description Java-client test application for tair.
 *
 */
public class TairClientTest {

	protected static final Logger   logger = LoggerFactory.getLogger(TairClientTest.class);  
	
	public static void main(String[] args) {
		StaticTairOperatorImpl tair=StaticTairOperatorImpl.getInstance();
		tair.put(1, 12);
		System.out.println(tair.get(1));
	//	StaticTairOperatorImpl.init();
	//	StaticTairOperatorImpl.get(null);
		int begintime=1467875340-6000;  //10000
		int endtime  =begintime+15000;

		for (int i = begintime; i <= endtime; i+=60) {
			Object o=tair.get(RaceConfig.prex_taobao+i);
			if (o!=null) {
				System.out.println(RaceConfig.prex_taobao+i+": " +o);
			}
		}
		
		System.out.println("--------------");
		
		for (int i = begintime; i <= endtime; i+=60) {
			Object o=tair.get(RaceConfig.prex_tmall+i);
			if (o!=null) {
				System.out.println(RaceConfig.prex_tmall+" "+i+" : " +o);
			}
		}
		
		for (int i = begintime; i <= endtime; i+=60) {
			Object o=tair.get(RaceConfig.prex_ratio+i);
			if (o!=null) {
				System.out.println(RaceConfig.prex_ratio+" "+i+" : " +o);
			}
		}
		
		System.out.println("--------------");
		for (int i = begintime; i <= endtime; i+=60) {
			StaticTairOperatorImpl.delete(RaceConfig.prex_taobao+i);
			StaticTairOperatorImpl.delete(RaceConfig.prex_tmall+i);
			StaticTairOperatorImpl.delete(RaceConfig.prex_ratio+i);
		}
		
		
	}

}