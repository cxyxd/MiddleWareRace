package com.alibaba.middleware.race.Tair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;


/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/
 * group 、namespace我们都会在正式提交代码前告知选手
 */
public class StaticTairOperatorImpl  implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -2823974917795312916L;
	private static DefaultTairManager tairManager=null;
	private static StaticTairOperatorImpl tair=null;
	protected static final Logger   logger = LoggerFactory.getLogger(StaticTairOperatorImpl.class);  
	
	private StaticTairOperatorImpl(){
		logger.error("我在进行初始化");
    	List<String> confServers = new ArrayList<String>();
		confServers.add(RaceConfig.TairConfigServer); 
		confServers.add(RaceConfig.TairConfigServer); 

		tairManager = new DefaultTairManager();
		
		tairManager.setConfigServerList(confServers);

		// 设置组名
		tairManager.setGroupName(RaceConfig.TairGroup);
		// 初始化客户端
		tairManager.init();
	}
	
	
	public static StaticTairOperatorImpl getInstance()  {
		if (tair==null) 
			tair=new StaticTairOperatorImpl();
		return tair;
    }

    public  boolean put(Serializable key, Serializable value) {
    	//System.out.println("tair put "+key+"  "+value+" "+result);
        return put(RaceConfig.TairNamespace, key,value);
    }
    public   boolean put(int nameSpace,Serializable key, Serializable value) {

    	ResultCode result = tairManager.put(nameSpace, key,value,0,0);
    //	System.out.println("tair put namespace: "+nameSpace+" key:"+key+" value: "+value+"" +
    //			" result: "+result.isSuccess()+" with time: "+System.currentTimeMillis());
        return result.isSuccess();
    }
    


    
    public  boolean contain(Serializable key){
    	Object o=get(key);
    	if (o==null) {
			return false;
		}else {
			return true;
		}
    }
    
    public  Object get(int nameSpace,Serializable key){

    	Result<DataEntry> result=tairManager.get(nameSpace, key);
    	if (result.isSuccess()) {
    		
			DataEntry entry = result.getValue();
			
			if (entry != null) {
				return entry.getValue();
			} else {
		//		System.out.println("tair get namespace: "+nameSpace+" key:"+key+"  is null"+" with time: "+System.currentTimeMillis());
				return null;
			}
		} else {
			// 异常处理
			System.out.println("ssv "+result.getRc().getMessage());
			return  null;
		}
    }

    public   Object get(Serializable key) {
    	return get(RaceConfig.TairNamespace, key);
    }
    
    public static ResultCode delete(Serializable key){

    	return tairManager.delete(RaceConfig.TairNamespace, key);
    }

    public static boolean remove(Serializable key) {
        return false;
    }

    public static void close(){
    }

    //天猫的分钟交易额写入tair
    public static void main(String [] args) throws Exception {

        
    }
}