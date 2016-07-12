package com.alibaba.middleware.race.rocketmq;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.EmitPaymentSpot;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;


/**
 * Consumer，订阅消息
 */

/**
 * RocketMq消费组信息我们都会再正式提交代码前告知选手
 */
public class OrderConsumer { 
	
	private static transient  AtomicInteger count2;
	protected static transient LinkedBlockingDeque<MessageExt> orders_tb_Deque;
	protected static transient LinkedBlockingDeque<MessageExt> orders_tm_Deque;
	protected static  transient LinkedBlockingDeque<MessageExt> psysDeque;
	private static Logger log = LoggerFactory.getLogger(EmitPaymentSpot.class);

	
    public static void main(String[] args) throws InterruptedException, MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);

        /**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
     //   consumer.setMessageModel(MessageModel.BROADCASTING);
     //   consumer.setConsumeMessageBatchMaxSize(32);
        //在本地搭建好broker后,记得指定nameServer的地址
        consumer.setNamesrvAddr("10.150.0.94:9876");

        consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
    	consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
		consumer.subscribe(RaceConfig.MqPayTopic, "*");
		
		count2=new AtomicInteger(0);
		orders_tb_Deque = new LinkedBlockingDeque<MessageExt>();
		orders_tm_Deque = new LinkedBlockingDeque<MessageExt>();
		psysDeque = new LinkedBlockingDeque<MessageExt>();
		
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
        		count2.addAndGet(msgs.size());
        		if (count2.get()%50000<25) {
        			log.error("consumer 收到:"+count2.get());
        		}
        		
        		for (MessageExt msg : msgs) {
        			byte[] body = msg.getBody();
        			if (body.length >2) {
        				String topic=msg.getTopic();
        				
        				if (topic.equals(RaceConfig.MqPayTopic)) {
        					psysDeque.offer(msg);
        				}else if (topic.equals(RaceConfig.MqTaobaoTradeTopic)) {
        					orders_tb_Deque.offer(msg);
        				} else if  (topic.equals(RaceConfig.MqTmallTradeTopic)) {
        					orders_tm_Deque.offer(msg);
        				} 
        			}


        		}
        		return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        	}
            
         
			
			public String convert(long mill) {
				Date date = new Date(mill);
				String strs = "";
				try {
					SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
					strs = sdf.format(date);
				} catch (Exception e) {
					e.printStackTrace();
				}
				return strs;
			}

		
        });

        consumer.start();

        System.out.println("Consumer Started.");
    }

    
    
    
    
}