package com.alibaba.middleware.race;

import com.alibaba.middleware.race.model.OrderMessage;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


public class RaceUtils {
    /** 
     * 由于我们是将消息进行Kryo序列化后，堆积到RocketMq，所有选手需要从metaQ获取消息，
     * 反序列出消息模型，只要消息模型的定义类似于OrderMessage和PaymentMessage即可
     * @param object
     * @return   
     */
    public static byte[] writeKryoObject(Object object) { 
        Output output = new Output(1024); 
        Kryo kryo = new Kryo();
        kryo.writeObject(output, object);
        output.flush(); 
        output.close();
        byte [] ret = output.toBytes();
        output.clear(); 
        return ret;
    }

    public static <T> T readKryoObject(Class<T> tClass, byte[] bytes) {
        Kryo kryo = new Kryo();
        Input input = new Input(bytes);
        input.close();
        T ret = kryo.readObject(input, tClass);
        return ret;
    }
    public static void main(String[] args) {
    	OrderMessage order=OrderMessage.createTbaoMessage();
    	order.setCreateTime(System.currentTimeMillis());
    	byte[] body= writeKryoObject(order);
    	long t1=System.currentTimeMillis();
		for (int i = 0; i < 100000; i++) {
			readKryoObject(OrderMessage.class, body);
		}
		long t2=System.currentTimeMillis();
		System.out.println(t2-t1);
	}

}