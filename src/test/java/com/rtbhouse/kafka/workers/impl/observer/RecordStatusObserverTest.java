package com.rtbhouse.kafka.workers.impl.observer;

import static org.assertj.core.api.Assertions.assertThat;
import com.rtbhouse.kafka.workers.api.WorkersConfig;
import com.rtbhouse.kafka.workers.api.record.WorkerRecord;
import com.rtbhouse.kafka.workers.impl.observer.RecordStatusObserverImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class RecordStatusObserverTest {

    @Test
    public void shouldCombineOffsets() {

        WorkersConfig config = new WorkersConfig(Collections.singletonMap("consumer.topics", "topic"));

        RecordStatusObserverImpl<byte[], byte[]> combinedObserver = null;
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
            WorkerRecord<byte[], byte[]> record = new WorkerRecord<>(new ConsumerRecord<>("topic", 0, offset, null, null), 0);
            RecordStatusObserverImpl<byte[], byte[]> observer = new RecordStatusObserverImpl<>(record, null);
            if (combinedObserver == null) {
                combinedObserver = observer;
            } else {
                combinedObserver.combine(observer);
            }
        }
        System.out.println(combinedObserver.compressedOffsets);
        System.out.println(offsets);
        System.out.println(combinedObserver.offsets());

        // then
        assertThat(combinedObserver.offsets()).isEqualTo(offsets);

    }

}
