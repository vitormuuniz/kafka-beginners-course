package com.github.simplestaph.kafka.tutorial1.consumer;

import com.github.simplestaph.kafka.utils.KafkaUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

public class ConsumerDemoGroups {
	public static void main(String[] args) {
		var logger = LoggerFactory.getLogger(ConsumerDemoGroups.class);

		var groupId = "my-first-application6";
		var topic = "twitter_tweets";

		var properties = KafkaUtils.getProperties();
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) { //create consumer
			//subscribe consumer to our topic(s)
			consumer.subscribe(List.of(topic));

			//poll for new data
			while (true) {
				var consumerRecords = consumer.poll(Duration.ofMillis(100));
				for (var consumerRecord : consumerRecords) {
					logger.info("Key: {}, Value: {} \n", consumerRecord.key(), consumerRecord.value());
					logger.info("Partition: {}, Offset: {}", consumerRecord.partition(), consumerRecord.offset());
				}
			}
		}
	}
}
