package com.github.simplestaph.kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

	private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

	public static void main(String[] args) throws InterruptedException, IOException {
		new TwitterProducer().run();
	}

	public void run() throws InterruptedException, IOException {
		logger.info("Setup");

		//set up your blocking queues: Be sure to size these properly based on expected TPS of your stream
		var msgQueue = new LinkedBlockingQueue<String>(100000);

		//create a twitter client
		var twitterClient = createTwitterClient(msgQueue);

		//attempt to establish a connection
		twitterClient.connect();

		//create a kafka producer
		var producer = createKafkaProducer();

		//loop to send tweets to kafka
		//on a different thread, or multiple different threads....
		while (!twitterClient.isDone()) {
			String msg;
			try {
				msg = msgQueue.poll(5, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				e.printStackTrace();
				twitterClient.stop();
				throw e;
			}
			if (msg != null) {
				logger.info(msg);
				producer.send(new ProducerRecord<>("twitter_tweets", null, msg), (recordMetadata, e) -> {
					if (e != null)
						logger.error("Something bad happend", e);
				});
			}
		}

		//add a shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			logger.info("Stopping application...");
			logger.info("Shutting down client from Twitter...");
			twitterClient.stop();
			logger.info("Closing producer from Kafka...");
			producer.close();
			logger.info("Done!");
		}));

		logger.info("End of application");
	}

	public Client createTwitterClient(BlockingQueue<String> msgQueue) throws IOException {
		Properties prop = new Properties();
		prop.load(TwitterProducer.class.getClassLoader().getResourceAsStream("application.properties"));

		var consumerKey = prop.getProperty("api.twitter.consumerKey");
		var consumerSecret = prop.getProperty("api.twitter.consumerSecret");
		var token = prop.getProperty("api.twitter.token");
		var secret = prop.getProperty("api.twitter.secret");

		// These secrets should be read from a config file
		var hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);

		// Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
		var hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		var hosebirdEndpoint = new StatusesFilterEndpoint();
		var terms = Lists.newArrayList("bitcoin", "usa", "politics", "sport", "soccer");

		hosebirdEndpoint.trackTerms(terms);

		return new ClientBuilder().name("Hosebird-Client-01") // optional: mainly for the logs
				.hosts(hosebirdHosts)
				.authentication(hosebirdAuth)
				.endpoint(hosebirdEndpoint)
				.processor(new StringDelimitedProcessor(msgQueue)) // optional: use this if you want to process client events
				.build();
	}

	public KafkaProducer<String, String> createKafkaProducer() {
		final String bootstrapServers = "localhost:9092";

		//create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		//create safe Producer
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
		properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, String.valueOf(Integer.MAX_VALUE));
		properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

		// high throughput producer (at the expense of a bit of latency and CPU usage)
		properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
		properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
		properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, String.valueOf(32*1024)); //32kb batch size


		//create the producer
		return new KafkaProducer<>(properties);
	}
}
