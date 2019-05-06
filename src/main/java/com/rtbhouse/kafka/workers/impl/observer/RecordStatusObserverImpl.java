package com.rtbhouse.kafka.workers.impl.observer;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.rtbhouse.kafka.workers.api.observer.BatchStatusObserver;
import com.rtbhouse.kafka.workers.api.observer.RecordStatusObserver;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;

public class RecordStatusObserverImpl implements RecordStatusObserver {

    private final SubpartitionObserver subpartitionObserver;

    private final long offset;

    public RecordStatusObserverImpl(SubpartitionObserver subpartitionObserver, long offset) {
        this.subpartitionObserver = subpartitionObserver;
        this.offset = offset;
    }

    @Override
    public void onSuccess() {
        subpartitionObserver.onSuccess(offsets());
    }

    @Override
    public void onFailure(Exception exception) {
        subpartitionObserver.onFailure(offsets(), exception);
    }

    @Override
    public BatchStatusObserver toBatch() {
        BatchStatusObserver batchObserver = new BatchStatusObserverImpl(subpartitionObserver);
        batchObserver.append(this);
        return batchObserver;
    }

    @Override
    public WorkerSubpartition subpartition() {
        return subpartitionObserver.subpartition();
    }

    @Override
    public long offset() {
        return offset;
    }

    private Set<Long> offsets() {
        return Stream.of(offset()).collect(Collectors.toSet());
    }

}
