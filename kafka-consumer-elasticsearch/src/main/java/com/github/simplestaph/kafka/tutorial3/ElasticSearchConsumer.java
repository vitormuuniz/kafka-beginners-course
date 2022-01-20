package com.github.simplestaph.kafka.tutorial3;

import com.github.simplestaph.kafka.utils.KafkaUtils;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ElasticSearchConsumer {

	public static RestHighLevelClient createClient() throws IOException {
		Properties prop = new Properties();
		prop.load(ElasticSearchConsumer.class.getClassLoader().getResourceAsStream("application.properties"));

		//replace with your own credentials
		String hostname = prop.getProperty("elasticsearch.credentials.hostname");
		String username = prop.getProperty("elasticsearch.credentials.username");
		String password = prop.getProperty("elasticsearch.credentials.password");

		//don't do if you run a local ES
		final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
		credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

		RestClientBuilder restClientBuilder = RestClient.builder(
				new HttpHost(hostname, 443, "https")).setHttpClientConfigCallback(httpClientBuilder ->
						httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));

		return new RestHighLevelClient(restClientBuilder);
	}

	public static KafkaConsumer<String, String> createConsumer(String topic) {
		var groupId = "kafka-demo-elasticsearch";

		var properties = KafkaUtils.getProperties();
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //disable auto commit of offsets
		properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); //limit received records amount

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
		consumer.subscribe(List.of(topic));
		return consumer;
	}

	public static void main(String[] args) throws IOException {
		var logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

		var client = createClient();

		var consumer = createConsumer("twitter_tweets");

		//poll for new data
		while (true) {
			var consumerRecords = consumer.poll(Duration.ofMillis(100));

			var recordCount = consumerRecords.count();

			var bulkRequest = new BulkRequest();

			for (var consumerRecord : consumerRecords) {
				// twitter feed specific id
				try {
					String id = KafkaUtils.extractTwitterId(consumerRecord.value());
					String json = consumerRecord.value();

					var indexRequest = new IndexRequest("twitter").source(json, XContentType.JSON);
					indexRequest.id(id);

					bulkRequest.add(indexRequest);
				} catch (NullPointerException e) {
					logger.warn("Skipping bad data: {}", consumerRecord.value());
				}
			}
			if (recordCount > 0) {
				logger.info("Received {} records", recordCount);
				client.bulk(bulkRequest, RequestOptions.DEFAULT);
				logger.info("Committing offsets...");
				consumer.commitSync();
				logger.info("Offsers have been committed");
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

		//close the client gracefully
		//client.close();
	}
}
