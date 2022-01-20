package com.github.simplestaph.kafka.utils;

import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaUtils {
	private static final String BOOTSTRAP_SERVER = "localhost:9092";

	private KafkaUtils() {}

	public static Properties getProperties() {
		//create consumer properties
		var properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		return properties;
	}

	public static String extractTwitterId(String tweetJson) {
		return JsonParser.parseString(tweetJson).getAsJsonObject().get("id_str").getAsString();
	}

	public static Integer extractUserFollowersInTwitter(String tweetJson) {
		try {
			return JsonParser.parseString(tweetJson)
					.getAsJsonObject()
					.get("user")
					.getAsJsonObject()
					.get("followers_count")
					.getAsInt();
		} catch (NullPointerException e) {
			return 0;
		}
	}
}
