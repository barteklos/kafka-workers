package com.rtbhouse.kafka.workers.impl.task;

import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.observer.RecordStatusObserver;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.api.task.WorkerTask;
import com.rtbhouse.kafka.workers.impl.metrics.WorkersMetrics;
import com.rtbhouse.kafka.workers.impl.observer.SubpartitonObserver;

public class WorkerTaskImpl<K, V> implements WorkerTask<K, V> {

    // user-defined task to process
    private final WorkerTask<K, V> task;

    // subpartition which is associated with given task in one-to-one relation
    private WorkerSubpartition subpartition;

    private final WorkersMetrics metrics;
    // private final SubpartitonObserver observer;

    private WorkerThread<K, V> thread;

    public WorkerTaskImpl(WorkerTask<K, V> task, WorkerSubpartition subpartition, WorkersMetrics metrics) {
        this.task = task;
        this.subpartition = subpartition;
        this.metrics = metrics;
        this.observer = new SubpartitonObserver(subpartition);
    }

    @Override
    public void init(WorkerSubpartition subpartition, WorkersConfig config) {
        metrics.addWorkerThreadSubpartitionMetrics(subpartition);
        task.init(subpartition, config);
    }

    @Override
    public boolean accept(WorkerRecord<K, V> record) {
        metrics.recordSensor(WorkersMetrics.ACCEPTING_OFFSET_METRIC, subpartition, record.offset());
        boolean accepted = task.accept(record);
        if (accepted) {
            metrics.recordSensor(WorkersMetrics.ACCEPTED_OFFSET_METRIC, subpartition, record.offset());
        }
        return accepted;
    }

    @Override
    public void process(WorkerRecord<K, V> record, RecordStatusObserver observer) {
        metrics.recordSensor(WorkersMetrics.PROCESSING_OFFSET_METRIC, subpartition, record.offset());
        try {
            task.process(record, observer);
        } catch (Exception e) {
            observer.onFailure(e);
        }
    }

    @Override
    public void punctuate(long punctuateTime) {
        task.punctuate(punctuateTime);
    }

    @Override
    public void close() {
        task.close();
        metrics.removeWorkerThreadSubpartitionMetrics(subpartition);
    }

    public WorkerSubpartition subpartition() {
        return subpartition;
    }

    public void setThread(WorkerThread<K, V> thread) {
        this.thread = thread;
    }

    public void notifyTask() {
        if (thread != null) {
            thread.notifyThread();
        }
    }

}
