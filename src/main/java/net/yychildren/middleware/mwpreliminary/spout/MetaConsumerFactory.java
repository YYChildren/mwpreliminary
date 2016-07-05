package net.yychildren.middleware.mwpreliminary.spout;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.MQHelper;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;

public class MetaConsumerFactory {
	
	private static final Logger	LOG   = Logger.getLogger(MetaConsumerFactory.class);
	
    
    // private static final long   serialVersionUID = 4641537253577312163L;
    
    public static Map<String, DefaultMQPushConsumer> consumers = 
    		new HashMap<String, DefaultMQPushConsumer>();
    
    public static synchronized DefaultMQPushConsumer mkInstance(MetaClientConfig config, 
			MessageListenerConcurrently listener)  throws Exception{
    	
    	String topic = config.getTopic();
    	String groupId = RaceConfig.MetaConsumerGroup; 
        // config.getConsumerGroup();
    	String subExpress = "*";
    	// config.getSubExpress();
    	int queueSize = config.getQueueSize();
    	int sendBatchSize = config.getSendBatchSize();
		int pullBatchSize = config.getPullBatchSize();
		long pullInterval = config.getPullInterval();
		int minPullThreadNum = config.getPullThreadNum();
		int maxPullThreadNum = config.getPullThreadNum();
		Date date = config.getStartTimeStamp();
    	
    	String key = topic + "@" + groupId;
    	
    	DefaultMQPushConsumer consumer = consumers.get(key);
    	if (consumer != null) {
    		
    		LOG.info("Consumer of " + key + " has been created, don't recreate it ");
    		
    		//Attention, this place return null to info duplicated consumer
    		return null;
    	}
    	
        
        StringBuilder sb = new StringBuilder();
        sb.append("Begin to init meta client \n");
        sb.append(",configuration:").append(config);
        
        LOG.info(sb.toString());
        
        consumer = new DefaultMQPushConsumer(config.getConsumerGroup());
        
        String nameServer = config.getNameServer();
        if ( nameServer != null) {
			String namekey = "rocketmq.namesrv.domain";

			String value = System.getProperty(namekey);
			// this is for alipay
			if (value == null) {

				System.setProperty(namekey, nameServer);
			} else if (value.equals(nameServer) == false) {
				throw new Exception(
						"Different nameserver address in the same worker "
								+ value + ":" + nameServer);

			}
		}
        
        String instanceName = groupId +"@" +	JStormUtils.process_pid();
		consumer.setInstanceName(instanceName);
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);
		consumer.subscribe(topic, subExpress);
		consumer.registerMessageListener(listener);
		
		consumer.setPullThresholdForQueue(queueSize);
		consumer.setConsumeMessageBatchMaxSize(sendBatchSize);
		consumer.setPullBatchSize(pullBatchSize);
		consumer.setPullInterval(pullInterval);
		consumer.setConsumeThreadMin(minPullThreadNum);
		consumer.setConsumeThreadMax(maxPullThreadNum);
		
		if ( date != null) {
			LOG.info("Begin to reset meta offset to " + date);
			try {
				MQHelper.resetOffsetByTimestamp(MessageModel.CLUSTERING,
					instanceName, groupId, topic, date.getTime());
				LOG.info("Successfully reset meta offset to " + date);
			}catch(Exception e) {
				LOG.error("Failed to reset meta offset to " + date);
			}

		}else {
			LOG.info("Don't reset meta offset  ");
		}

		consumer.start();
		
		consumers.put(key, consumer);
		LOG.info("Successfully create " + key + " consumer");
		
		
		return consumer;
		
    }
    
}
