package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

    /**
	 * 
	 */ 
	private static final long serialVersionUID = 7160717176287541054L;
	//这些是写tair key的前缀
    public static String prex_tmall = "platformTmall_41500fyqmw_"; 
    public static String prex_taobao = "platformTaobao_41500fyqmw_"; 
    public static String prex_ratio = "ratio_41500fyqmw_";
  

    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布        
    public static String JstormTopologyName = "41500fyqmw";
    public static String MetaConsumerGroup = "41500fyqmw";
   
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    
    
   
    public static String TairGroup = "group_1";
    public static String TairConfigServer = "10.150.0.94:5198"; 
    public static String TairSalveConfigServer = "10.150.0.94:5198";
    public static Integer TairNamespace = 5;
    

    
    public static Boolean writeSQL=false;
    
}