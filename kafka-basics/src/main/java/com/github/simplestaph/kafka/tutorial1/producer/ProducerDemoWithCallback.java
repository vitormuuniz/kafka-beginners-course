package com.github.simplestaph.kafka.tutorial1.producer;

import com.github.simplestaph.kafka.utils.KafkaUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoWithCallback {
	public static void main(String[] args) {
		Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

		var properties = KafkaUtils.getProperties();

		//create the producer
		try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

			//create a producer record
			ProducerRecord<String, String> producerRecord = new ProducerRecord<>("first_topic", "Hello World");

			//send data (is asynchronous)
			//executes every time a record is succesfully sent or an exception is thrown
			producer.send(producerRecord, (recordMetadata, e) -> {
				if (e == null) { // the record was succesfully sent
					logger.info(
							"Received new metadata: \n " +
							"Topic: {} \n Partition: {} \n Offset: {} \n Timestamp: {} \n",
							recordMetadata.topic(), recordMetadata.partition(),
							recordMetadata.offset(), recordMetadata.timestamp());
				} else {
					logger.error("Error while producing: ", e);
				}
			});

			//flush data
			producer.flush();
		}
	}

}
