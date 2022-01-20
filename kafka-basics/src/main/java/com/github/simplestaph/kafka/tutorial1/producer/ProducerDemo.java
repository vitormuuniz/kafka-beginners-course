package com.github.simplestaph.kafka.tutorial1.producer;

import com.github.simplestaph.kafka.utils.KafkaUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerDemo {
	public static void main(String[] args) {
		var properties = KafkaUtils.getProperties();

		//create the producer
		try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {
			var topic = "first_topic";
			var value = "Hello World";

			//create a producer record
			ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, value);

			//send data (is asynchronous)
			producer.send(producerRecord);

			//flush data
			producer.flush();
		}
	}

}
