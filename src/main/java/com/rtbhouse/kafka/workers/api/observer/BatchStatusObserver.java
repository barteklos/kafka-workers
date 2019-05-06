package com.rtbhouse.kafka.workers.api.observer;

import java.util.Set;

public interface BatchStatusObserver extends StatusObserver {

    void append(RecordStatusObserver observer);

    void combine(BatchStatusObserver observer);

    Set<Long> offsets();

}
