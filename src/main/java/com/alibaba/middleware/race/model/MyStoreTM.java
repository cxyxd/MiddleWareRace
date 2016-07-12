package com.alibaba.middleware.race.model;

import java.io.Serializable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class MyStoreTM implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = -3917754830264195791L;
	private transient  Map<String, Double> orders=new ConcurrentHashMap<String, Double>();
	private final static MyStoreTM  myStore=new MyStoreTM();  
	private MyStoreTM(){
		
	}
	
	public static  MyStoreTM getInstance(){
		return myStore;
	}
	
	public Double put(String key, Double value){
		 return orders.put(key, value);
	}
	
	public Double get(Object key){
		return orders.get(key);
	}
	
	public int size(){
		return orders.size();
	}
	
	public Set<Entry<String, Double>>  entrySet(){
		return orders.entrySet();
	}
	
	
	public boolean containsKey(String key){
		return orders.containsKey(key);
	}
}
