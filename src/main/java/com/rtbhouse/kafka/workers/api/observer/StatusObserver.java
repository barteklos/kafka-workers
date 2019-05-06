package com.rtbhouse.kafka.workers.api.observer;

import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * Common interface for {@link RecordStatusObserver} and {@link BatchStatusObserver}.
 */
public interface StatusObserver {

    /**
     * Should be called when {@link WorkerRecord} or batch of records was processed successfully. Information will be used to commit
     * related offset or offsets by internal {@link KafkaConsumer}.
     */
    void onSuccess();

    /**
     * Should be called when {@link WorkerRecord} or batch of record could not be processed because of any failure. In that case whole
     * processing will be resumed from last committed offset.
     *
     * @param exception
     *            exception which caused a failure
     */
    void onFailure(Exception exception);

    WorkerSubpartition subpartition();

}
