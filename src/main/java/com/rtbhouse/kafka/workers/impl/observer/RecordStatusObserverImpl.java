package com.rtbhouse.kafka.workers.impl.observer;

import com.rtbhouse.kafka.workers.api.observer.RecordStatusObserver;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.errors.FailedCombineObserversException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RecordStatusObserverImpl<K, V> implements RecordStatusObserver {

    private final SubpartitonObserver subpartitonObserver;
    protected final Map<Integer, Set<Integer>> compressedOffsets = new HashMap<>();

    public RecordStatusObserverImpl(WorkerRecord<?, ?> record, SubpartitonObserver subpartitonObserver) {
        this.subpartitonObserver = subpartitonObserver;
        int compressedKey = compressedKey(record.offset());
        int compressedValue = compressedValue(record.offset());
        this.compressedOffsets.put(compressedKey, new HashSet<>(Arrays.asList(compressedValue)));
    }

    @Override
    public void onSuccess() {
        subpartitonObserver.onSuccess(offsets());
    }

    @Override
    public void onFailure(Exception exception) {
        subpartitonObserver.onFailure(offsets(), exception);
    }

    @Override
    public void combine(RecordStatusObserver observer) {
        RecordStatusObserverImpl<K, V> observerImpl = (RecordStatusObserverImpl<K, V>) observer;
        if (!subpartition().equals(observerImpl.subpartition())) {
            throw new FailedCombineObserversException(
                    "could not combine observers for different subpartitions: " + subpartition() + ", " + observerImpl.subpartition());
        }
        for (Map.Entry<Integer, Set<Integer>> entry : observerImpl.compressedOffsets.entrySet()) {
            Integer key = entry.getKey();
            Set<Integer> values = entry.getValue();
            if (!compressedOffsets.containsKey(key)) {
                compressedOffsets.put(key, new HashSet<>());
            }
            compressedOffsets.get(key).addAll(values);
        }

    }

    private WorkerSubpartition subpartition() {
        return subpartitonObserver.subpartition();
    }

    protected Set<Long> offsets() {
        return compressedOffsets.entrySet().stream()
                .flatMap(entry -> offsets(entry.getKey(), entry.getValue()))
                .collect(Collectors.toSet());
    }

    private Stream<Long> offsets(int key, Set<Integer> values) {
        return values.stream().map(value -> fromCompressed(key, value));
    }

    private int compressedKey(long offset) {
        return (int)(offset >> 32);
    }

    private int compressedValue(long offset) {
        return (int)(offset);
    }

    private long fromCompressed(int key, int value) {
        return (((long)key) << 32) | (value & 0xffffffffL);
    }

}
