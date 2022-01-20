package com.github.simplestaph.kafka.tutorial4;

import com.github.simplestaph.kafka.utils.KafkaUtils;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {
	public static void main(String[] args) {
		//create properties
		var properties = new Properties();
		properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
		properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
		properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

		//create a topology
		var streamsBuilder = new StreamsBuilder();

		//input topic
		KStream<String, String> inputTopic = streamsBuilder.stream("twitter_tweets");
		var filteredStream = inputTopic.filter((key, jsonTweet) ->
			//filter for tweets which has a user of over 10000 followers
			KafkaUtils.extractUserFollowersInTwitter(jsonTweet) > 10000
		);
		filteredStream.to("important_tweets");

		//build the topology
		var kafkaStreams = new KafkaStreams(streamsBuilder.build(), properties);

		//start our stream application
		kafkaStreams.start();


	}
}
