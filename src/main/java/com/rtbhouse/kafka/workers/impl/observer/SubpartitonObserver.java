package com.rtbhouse.kafka.workers.impl.observer;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.task.RecordProcessingGuarantee;
import com.rtbhouse.kafka.workers.impl.KafkaWorkersImpl;
import com.rtbhouse.kafka.workers.impl.errors.ProcessingFailureException;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.offsets.OffsetsState;
import com.rtbhouse.kafka.workers.impl.task.WorkerThread;

import java.util.Set;

public class SubpartitonObserver {

    private final WorkerSubpartition subpartition;

    private final WorkersMetrics metrics;
    private final RecordProcessingGuarantee recordProcessingGuarantee;
    private final OffsetsState offsetsState;
    private final KafkaWorkersImpl<?, ?> kafkaWorkers;

    public SubpartitonObserver(
            WorkerSubpartition subpartition,
            WorkersMetrics metrics,
            WorkersConfig config,
            OffsetsState offsetsState,
            KafkaWorkersImpl<?, ?> kafkaWorkers) {
        this.subpartition = subpartition;
        this.metrics = metrics;
        this.recordProcessingGuarantee = config.getRecordProcessingGuarantee();
        this.offsetsState = offsetsState;
        this.kafkaWorkers = kafkaWorkers;
    }

    public void onSuccess(Set<Long> offsets) {
        markProcessed(offsets);
    }

    public void onFailure(Set<Long> offsets, Exception exception) {
        if (RecordProcessingGuarantee.AT_LEAST_ONCE.equals(recordProcessingGuarantee)) {
            kafkaWorkers.shutdown(new ProcessingFailureException(
                    "record processing failed, subpartition: " + subpartition + " , offsets: " + offsets, exception));
        } else {
            markProcessed(offsets);
        }
    }

    public WorkerSubpartition subpartition() {
        return subpartition;
    }

    private void markProcessed(Set<Long> offsets) {
        offsets.forEach(offset -> {
            offsetsState.updateProcessed(subpartition.topicPartition(), offset);
            metrics.recordSensor(WorkersMetrics.PROCESSED_OFFSET_METRIC, subpartition, offset);
        });
    }

}
