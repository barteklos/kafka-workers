package com.rtbhouse.kafka.workers.integration;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import com.rtbhouse.kafka.workers.api.KafkaWorkers;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.observer.BatchStatusObserver;
import com.rtbhouse.kafka.workers.api.observer.RecordStatusObserver;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
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

@RequiresKafkaServer
public class BatchTest {

    private static final Logger logger = LoggerFactory.getLogger(BatchTest.class);

    private static final String TOPIC = "topic";
    private static final int RECORDS_COUNT = 100;
    private static final int RECORDS_PER_BATCH_COUNT = 10;

    private static final Properties SERVER_PROPERTIES = TestProperties.serverProperties();

    private static final Properties WORKERS_PROPERTIES = TestProperties.workersProperties(
            StringDeserializer.class, StringDeserializer.class, TOPIC);
    static {
        WORKERS_PROPERTIES.put(WorkersConfig.CONSUMER_COMMIT_INTERVAL_MS, 100L);
    }

    private static final Properties PRODUCER_PROPERTIES = TestProperties.producerProperties(
            StringSerializer.class, StringSerializer.class);

    private static final String GROUP_ID = WORKERS_PROPERTIES.getProperty("consumer.kafka.group.id");
    private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

    @Rule
    public KafkaServerRule kafkaServerRule = new KafkaServerRule(SERVER_PROPERTIES);

    private KafkaProducer<String, String> producer;

    @Before
    public void before() throws Exception {
        ZookeeperUtils.createTopics(kafkaServerRule.getZookeeperConnectString(), 1, 1, TOPIC);
        producer = new KafkaProducer<>(PRODUCER_PROPERTIES);
    }

    @After
    public void after() throws IOException {
        producer.close();
    }

    @Test
    public void shouldBatchObservers() throws Exception {

        // given
        for (int i = 0; i < RECORDS_COUNT; i++) {
            producer.send(new ProducerRecord<>(TOPIC, 0, null, "key_" + i, "value_" + i));
        }

        CountDownLatch latch = new CountDownLatch(1);

        KafkaWorkers<String, String> kafkaWorkers = new KafkaWorkers<>(
                new WorkersConfig(WORKERS_PROPERTIES),
                new TestTaskFactory(latch));

        // when
        kafkaWorkers.start();

        // then
        assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
    }

    private static class TestBatchTask implements WorkerTask<String, String> {

        private AdminClient adminClient;

        private CountDownLatch latch;

        private BatchStatusObserver batchObserver;

        private int recordsCount = 0;

        public TestBatchTask(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void init(WorkerSubpartition subpartition, WorkersConfig config) {
            adminClient = AdminClient.create(TestProperties.adminClientProperties());
        }

        @Override
        public void process(WorkerRecord<String, String> record, RecordStatusObserver observer) {
            recordsCount++;

            if (batchObserver == null) {
                batchObserver = observer.toBatch();
            } else {
                batchObserver.append(observer);
            }

            if (recordsCount % RECORDS_PER_BATCH_COUNT == 0) {
                batchObserver.onSuccess();
                batchObserver = null;
            }
        }

        @Override
        public void punctuate(long punctuateTime) {
            Long offset = getOffset(GROUP_ID, PARTITION);
            if (offset != null && offset >= RECORDS_COUNT) {
                latch.countDown();
            }
        }

        @Override
        public void close() {
            adminClient.close();
        }

        private Long getOffset(String group, TopicPartition partition) {
            OffsetAndMetadata metadata = null;
            try {
                metadata = adminClient.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get().get(partition);
            } catch (InterruptedException | ExecutionException e) {
            }
            return metadata != null ? metadata.offset() : null;
        }

    }

    private static class TestTaskFactory implements WorkerTaskFactory<String, String> {

        private CountDownLatch latch;

        public TestTaskFactory(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public TestBatchTask createTask(WorkersConfig config) {
            return new TestBatchTask(latch);
        }

    }

}
