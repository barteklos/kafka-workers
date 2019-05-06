package com.rtbhouse.kafka.workers.api.observer;

import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.api.task.WorkerTask;

/**
 * Every {@link WorkerRecord} is associated with its {@link RecordStatusObserver} which purpose is to report
 * record final status. It means that eventually one of methods: {@link #onSuccess()} or {@link #onFailure(Exception)}
 * has to be called for every {@link WorkerRecord} previously passed to {@link WorkerTask#process(WorkerRecord, RecordStatusObserver)}.
 */
public interface RecordStatusObserver extends StatusObserver {

    BatchStatusObserver toBatch();

    long offset();

}
