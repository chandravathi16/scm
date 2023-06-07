package com.ck.scmproject.config;

import org.springframework.stereotype.Component;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;

import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.time.Duration;
import java.util.Arrays;

import java.util.Properties;

@Component
public class Consumer {

	// initially MongoDB collection is empty
	private static MongoCollection<Document> collection = null;

	public void consumeData() throws JSONException {

		// try connection with MongoDB database; if an exception occurs, catch the exception
		try {
			
			MongoClient mongoClient = MongoClients.create(System.getenv("MONGODB_URI"));
			MongoDatabase databaseName = mongoClient.getDatabase(System.getenv("mongodbName"));
			
			String collectionName = System.getenv("mongoCollectionName");
			collection = databaseName.getCollection(collectionName);
			
			System.out.println("MongoDB connection established!!");
			mongoClient.close();
		} 
		catch (Exception exception) {
			throw new IllegalArgumentException("Failed to connect to MongoDB: " + exception.getMessage());
		}

		Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

		String bootstrapServer = System.getenv("bootstrapServer");
		String groupId = System.getenv("group_id");
		String topic = System.getenv("topic");

		// Set consumer properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

		// Subscribe to the topic
		consumer.subscribe(Arrays.asList(topic));

		try {
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<String, String> record : records) {
					logger.info("Key: " + record.key() + ", Value:" + record.value());
					logger.info("Partition:" + record.partition() + ",Offset:" + record.offset());
					Document doc = Document.parse(record.value());
					collection.insertOne(doc);
				}
			}
		} catch (Exception exception) {
			throw new IllegalArgumentException("Error while consuming data from Kafka: " + exception.getMessage());
		} finally {
			consumer.close();
		}
	}
}
