package com.tdevilleduc.urthehero.kafka;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.containers.KafkaContainer;

public class KafkaProducerTest {
    @Rule
    public KafkaContainer kafka = new KafkaContainer();

    @Autowired
    private KafkaProducer kafkaController;

    @Before
    public void setUp() {
        kafka.start();
    }

    @After
    public void tearDownClass() {
        kafka.stop();
    }

    public void testPost_thenSuccess() {
        this.kafkaController.sendMessage("thomas");
    }
}
