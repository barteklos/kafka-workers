package com.rtbhouse.kafka.workers.impl.observer;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.rtbhouse.kafka.workers.api.observer.BatchStatusObserver;
import com.rtbhouse.kafka.workers.api.observer.RecordStatusObserver;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.impl.errors.FailedCombineObserversException;

public class BatchStatusObserverImpl implements BatchStatusObserver {

    private final SubpartitionObserver subpartitionObserver;

    private long[] offsets;
    private int size;

    public BatchStatusObserverImpl(SubpartitionObserver subpartitionObserver) {
        this.offsets = new long[0];
        this.size = 0;
        this.subpartitionObserver = subpartitionObserver;
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
    public WorkerSubpartition subpartition() {
        return subpartitionObserver.subpartition();
    }

    @Override
    public void append(RecordStatusObserver observer) {
        appendOffsets(observer.subpartition(), Stream.of(observer.offset()).collect(Collectors.toSet()));
    }

    @Override
    public void combine(BatchStatusObserver observer) {
        appendOffsets(observer.subpartition(), observer.offsets());
    }

    public Set<Long> offsets() {
        return Arrays.stream(offsets, 0, size).boxed().collect(Collectors.toSet());
    }

    private void appendOffsets(WorkerSubpartition otherSubpartition, Set<Long> otherOffsets) {
        if (!subpartition().equals(otherSubpartition)) {
            throw new FailedCombineObserversException(
                    "could not combine observers for different subpartitions: " + subpartition() + ", " + otherSubpartition);
        }
        if (offsets.length < size + otherOffsets.size()) {
            long[] newOffsets = new long[2 * Math.max(offsets.length, otherOffsets.size())];
            System.arraycopy(offsets, 0, newOffsets, 0, size);
            offsets = newOffsets;
        }
        otherOffsets.forEach(offset -> offsets[size++] = offset);
    }

}