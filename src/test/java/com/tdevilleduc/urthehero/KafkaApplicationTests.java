package com.tdevilleduc.urthehero;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.time.Duration;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class KafkaApplicationTests {

	@Test
	void contextLoads() {
	}

	@Test
	public void testUsage() throws Exception {
		try (KafkaContainer kafka = new KafkaContainer()) {
			kafka.start();
			testKafkaFunctionality(kafka.getBootstrapServers());
		}
	}

	protected void testKafkaFunctionality(String bootstrapServers) throws Exception {
		try (
				KafkaProducer<String, String> producer = new KafkaProducer<>(
						ImmutableMap.of(
								ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
								ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
						),
						new StringSerializer(),
						new StringSerializer()
				);

				KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
						ImmutableMap.of(
								ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
								ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID(),
								ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
						),
						new StringDeserializer(),
						new StringDeserializer()
				);
		) {
			String topicName = "messages";
			consumer.subscribe(Arrays.asList(topicName));

			producer.send(new ProducerRecord<>(topicName, "testcontainers", "rulezzz")).get();

			Unreliables.retryUntilTrue(10, TimeUnit.SECONDS, () -> {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

				if (records.isEmpty()) {
					return false;
				}

				Assertions.assertThat(records)
						.hasSize(1)
						.extracting(ConsumerRecord::topic, ConsumerRecord::key, ConsumerRecord::value)
						.containsExactly(Assertions.tuple(topicName, "testcontainers", "rulezzz"));

				return true;
			});

			consumer.unsubscribe();
		}
	}
}
