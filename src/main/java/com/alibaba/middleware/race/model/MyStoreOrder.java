package com.alibaba.middleware.race.model;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MyStoreOrder implements Serializable {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -6810259403678593071L;
	private transient Map<Long, String> orders=new ConcurrentHashMap<Long, String>();
	private final static MyStoreOrder  myStore=new MyStoreOrder();  
	private MyStoreOrder(){
		
	}
	
	public static  MyStoreOrder getInstance(){
		return myStore;
	}
	
	public String put(Long key, String value){
		 return orders.put(key, value);
	}
	
	public String get(Long key){
		return orders.get(key);
	}
}
