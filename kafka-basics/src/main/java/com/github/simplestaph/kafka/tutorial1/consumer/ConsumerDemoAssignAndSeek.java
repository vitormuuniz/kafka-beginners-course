package com.github.simplestaph.kafka.tutorial1.consumer;

import com.github.simplestaph.kafka.utils.KafkaUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;

public class ConsumerDemoAssignAndSeek {
	public static void main(String[] args) {
		var logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class);

		var topic = "first_topic";

		var properties = KafkaUtils.getProperties();

		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) { //create consumer
			//assign and seek are mostly used to replay data or fetch a specific message

			//assign
			var partitionToReadFrom = new TopicPartition(topic, 0);
			var offsetToReadFrom = 15L;
			consumer.assign(List.of(partitionToReadFrom));

			//seek
			consumer.seek(partitionToReadFrom, offsetToReadFrom);

			var numberOfMessagesToRead = 5;
			var keepOnReading = true;
			var numberOfMessagesSoFar = 0;

			//poll for new data
			while (keepOnReading) {
				var consumerRecords = consumer.poll(Duration.ofMillis(100));
				for (var consumerRecord : consumerRecords) {
					numberOfMessagesSoFar++;
					logger.info("Key: {}, Value: {} \n", consumerRecord.key(), consumerRecord.value());
					logger.info("Partition: {}, Offset: {}", consumerRecord.partition(), consumerRecord.offset());
					if (numberOfMessagesSoFar >= numberOfMessagesToRead) {
						keepOnReading = false;
						break;
					}
				}
			}
			logger.info("Exiting the application");
		}
	}
}
