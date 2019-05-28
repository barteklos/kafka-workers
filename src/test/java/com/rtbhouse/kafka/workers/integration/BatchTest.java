package com.rtbhouse.kafka.workers.integration;

import static org.assertj.core.api.Assertions.assertThat;
import com.rtbhouse.kafka.workers.api.KafkaWorkers;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.observer.RecordStatusObserver;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.api.task.WorkerTask;
import com.rtbhouse.kafka.workers.api.task.WorkerTaskFactory;
import com.rtbhouse.kafka.workers.integration.utils.KafkaServerRule;
import com.rtbhouse.kafka.workers.integration.utils.RequiresKafkaServer;
import com.rtbhouse.kafka.workers.integration.utils.TestProperties;
import com.rtbhouse.kafka.workers.integration.utils.ZookeeperUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

@RequiresKafkaServer
public class BatchTest {

    private static final Logger logger = LoggerFactory.getLogger(BatchTest.class);

    private static final String TOPIC = "topic";
    private static final int RECORDS_COUNT = 10000;

    private static final Properties SERVER_PROPERTIES = TestProperties.serverProperties();

    private static final Properties WORKERS_PROPERTIES = TestProperties.workersProperties(
            StringDeserializer.class, StringDeserializer.class, TOPIC);
    static {
        WORKERS_PROPERTIES.put(WorkersConfig.CONSUMER_COMMIT_INTERVAL_MS, 100L);
    }

    private static final Properties PRODUCER_PROPERTIES = TestProperties.producerProperties(
            StringSerializer.class, StringSerializer.class);

    @Rule
    public KafkaServerRule kafkaServerRule = new KafkaServerRule(SERVER_PROPERTIES);

    private KafkaProducer<String, String> producer;
    private AdminClient adminClient;

    @Before
    public void before() throws Exception {
        ZookeeperUtils.createTopics(kafkaServerRule.getZookeeperConnectString(), 1, 1, TOPIC);
        producer = new KafkaProducer<>(PRODUCER_PROPERTIES);
        adminClient = AdminClient.create(TestProperties.adminClientProperties());
    }

    @After
    public void after() throws IOException {
        producer.close();
        adminClient.close();
    }

    @Test
    public void shouldCombineObservers() throws Exception {

        // given
        for (int i = 0; i < RECORDS_COUNT; i++) {
            producer.send(new ProducerRecord<>(TOPIC, 0, null, "key_" + i, "value_" + i));
        }

        CountDownLatch latch = new CountDownLatch(5);

        KafkaWorkers<String, String> kafkaWorkers = new KafkaWorkers<>(
                new WorkersConfig(WORKERS_PROPERTIES),
                new TestTaskFactory());

        // when
        kafkaWorkers.start();

        // then
        String groupId = WORKERS_PROPERTIES.getProperty("consumer.kafka.group.id");
        TopicPartition partition = new TopicPartition(TOPIC, 0);

        for (int i = 0; i < 100; i++) {
            logger.info("committed offset: {}", getOffset(groupId, partition));
            Thread.sleep(100L);
        }

    }

    private Long getOffset(String group, TopicPartition partition) throws ExecutionException, InterruptedException {
        OffsetAndMetadata metadata = adminClient.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get().get(partition);
        return metadata != null ? metadata.offset() : null;
    }

    private static class TestBatchTask implements WorkerTask<String, String> {

        private int batchSize = 0;
        private RecordStatusObserver observer;

        @Override
        public void process(WorkerRecord<String, String> record, RecordStatusObserver observer) {

            if (this.observer == null) {
                this.observer = observer;
            } else {
                this.observer.combine(observer);
            }
            batchSize++;

            if (batchSize == 100) {
                this.observer.onSuccess();
                this.observer = null;
                batchSize = 0;
            }

        }

    }

    private static class TestTaskFactory implements WorkerTaskFactory<String, String> {

        @Override
        public TestBatchTask createTask(WorkersConfig config) {
            return new TestBatchTask();
        }

    }

}
