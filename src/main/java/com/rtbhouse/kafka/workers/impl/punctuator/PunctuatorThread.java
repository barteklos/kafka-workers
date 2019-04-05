package com.rtbhouse.kafka.workers.impl.punctuator;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.rtbhouse.kafka.workers.impl.queues.RecordsQueue;
import com.rtbhouse.kafka.workers.impl.task.TaskManager;
import com.rtbhouse.kafka.workers.impl.task.WorkerThread;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.WorkersException;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.AbstractWorkersThread;
import com.rtbhouse.kafka.workers.impl.KafkaWorkersImpl;
import com.rtbhouse.kafka.workers.impl.Partitioned;
import com.rtbhouse.kafka.workers.impl.consumer.ConsumerRebalanceListenerImpl;
import com.rtbhouse.kafka.workers.impl.consumer.OffsetCommitCallbackImpl;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;
import com.rtbhouse.kafka.workers.impl.partitioner.SubpartitionSupplier;
import com.rtbhouse.kafka.workers.impl.queues.QueuesManager;

public class PunctuatorThread<K, V> extends AbstractWorkersThread {

    private static final Logger logger = LoggerFactory.getLogger(PunctuatorThread.class);

    private final List<WorkerThread<K, V>> threads;

    public PunctuatorThread(
            WorkersConfig config,
            WorkersMetrics metrics,
            KafkaWorkersImpl<K, V> workers,
            List<WorkerThread<K, V>> threads) {
        super("punctuator-thread", config, metrics, workers);
        this.threads = threads;
    }

    @Override
    public void init() {
    }

    @Override
    public void process() throws InterruptedException {
        for (WorkerThread<K, V> thread : threads) {
            if (thread.shouldPunctuateNow()) {
                thread.notifyThread();
            }
        }
        Thread.sleep(config.getLong(WorkersConfig.PUNCTUATOR_INTERVAL_MS));
    }

    @Override
    public void close() {
    }

}
