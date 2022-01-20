package com.github.simplestaph.kafka.tutorial1.producer;

import com.github.simplestaph.kafka.utils.KafkaUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
	public static void main(String[] args) throws ExecutionException, InterruptedException {
		Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

		var properties = KafkaUtils.getProperties();

		//create the producer
		try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
			for (int i = 0; i < 10; i++) {
				var topic = "first_topic";
				var value = "Hello World " + i;
				var key = "id_" + i;

				logger.info("Key: {}", key);

				//create a producer record
				var producerRecord = new ProducerRecord<>(topic, key, value);

				//send data (is asynchronous)
				producer.send(producerRecord, (recordMetadata, e) -> {
					//executes every time a record is succesfully sent or an exception is thrown
					if (e == null) { // the record was succesfully sent
						logger.info(
								"Received new metadata: \n " +
								"Topic: {} \n Partition: {} \n Offset: {} \n Timestamp: {} \n",
								recordMetadata.topic(), recordMetadata.partition(),
								recordMetadata.offset(), recordMetadata.timestamp());
					} else {
						logger.error("Error while producing: ", e);
					}
				}).get(); //block send() to make it synchronous (don't do that in production)
			}

			//flush data
			producer.flush();
		}
	}

}
