package com.rtbhouse.kafka.workers.impl.observer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.rtbhouse.kafka.workers.api.observer.BatchStatusObserver;
import com.rtbhouse.kafka.workers.api.partitioner.WorkerSubpartition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BatchStatusObserverTest {

    @Mock
    private SubpartitionObserver subpartitionObserver;

    @Test
    public void shouldAppendRecordObservers() {

        when(subpartitionObserver.subpartition()).thenReturn(new WorkerSubpartition("topic", 0, 0));

        BatchStatusObserver batchStatusObserver = null;
        Set<Long> offsets = new HashSet<>(Arrays.asList(
                0L,
                32766L,
                32767L,
                32768L,
                1073741823L,
                2147483646L,
                2147483647L,
                2147483648L,
                4611686018427387903L,
                4611686018427387904L,
                4611686018427387905L,
                4611686018427387906L,
                9223372036854775807L));
        for (Long offset : offsets) {
            RecordStatusObserverImpl recordStatusObserver = new RecordStatusObserverImpl(subpartitionObserver, offset);
            if (batchStatusObserver == null) {
                batchStatusObserver = recordStatusObserver.toBatch();
            } else {
                batchStatusObserver.append(recordStatusObserver);
            }
        }

        // then
        assertThat(batchStatusObserver.offsets()).isEqualTo(offsets);

    }

    @Test
    public void shouldCombineBatchObservers() {

        when(subpartitionObserver.subpartition()).thenReturn(new WorkerSubpartition("topic", 0, 0));

        BatchStatusObserver batchStatusObserver1 = null;
        BatchStatusObserver batchStatusObserver2 = null;

        Set<Long> offsets = LongStream.rangeClosed(0, 10).boxed().collect(Collectors.toSet());

        for (Long offset : offsets) {
            RecordStatusObserverImpl recordStatusObserver = new RecordStatusObserverImpl(subpartitionObserver, offset);

            if (offset % 2 == 0) {
                if (batchStatusObserver1 == null) {
                    batchStatusObserver1 = recordStatusObserver.toBatch();
                } else {
                    batchStatusObserver1.append(recordStatusObserver);
                }
            } else {
                if (batchStatusObserver2 == null) {
                    batchStatusObserver2 = recordStatusObserver.toBatch();
                } else {
                    batchStatusObserver2.append(recordStatusObserver);
                }
            }

        }

        batchStatusObserver1.combine(batchStatusObserver2);

        // then
        assertThat(batchStatusObserver1.offsets()).isEqualTo(offsets);

    }

}
