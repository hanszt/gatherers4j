/*
 * Copyright 2024 Todd Ginsberg
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ginsberg.gatherers4j;

import com.ginsberg.gatherers4j.util.GathererUtils;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static com.ginsberg.gatherers4j.Gatherers4j.throttle;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ThrottlingGathererTest {

    @Test
    void amountIsNegative() {
        final var duration = Duration.ofSeconds(1);
        assertThatThrownBy(() -> throttle(-1, duration))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void amountIsZero() {
        final var duration = Duration.ofSeconds(1);
        assertThatThrownBy(() -> throttle(-1, duration))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void clockMustNotBeNull() {
        final var throttle = throttle(1, Duration.ofSeconds(1));
        assertThatThrownBy(() -> throttle.withInstantSource(null))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void durationIsNegative() {
        final var duration = Duration.ofSeconds(-1);
        assertThatThrownBy(() -> throttle(1, duration))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void durationIsNull() {
        assertThatThrownBy(() -> throttle(1, null))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void durationIsZero() {
        assertThatThrownBy(() -> throttle(1, Duration.ZERO))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @SuppressWarnings("DataFlowIssue")
    @Test
    void limitRuleIsNotNull() {
        final var duration = Duration.ofSeconds(1);
        final InstantSource instantSource = () -> Instant.EPOCH;
        assertThatThrownBy(() -> new ThrottlingGatherer<>(null, 1, duration, instantSource))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testThrottlingCrossesPeriod() {
        // Arrange
        final var input = Stream.of("A", "B", "C");
        final var duration = Duration.ofMillis(100);
        final var instantSource = new PredictableInstantSource(0, 0, 0, 101, 0, 0);

        // Act
        final var output = input
                .gather(throttle(2, duration).withInstantSource(instantSource))
                .map(_ -> System.currentTimeMillis())
                .toList();

        // Assert
        assertThat(output.get(1) - output.get(0)).isLessThan(duration.toMillis());
        assertThat(output.get(2) - output.get(0)).isGreaterThanOrEqualTo(duration.toMillis());
    }

    @Test
    void testThrottlingWithDrop() {
        // Arrange
        final var input = Stream.of("A", "B", "C");
        final var duration = Duration.ofMillis(100);

        // Act
        final var output = input
                .gather(Gatherers4j.debounce(2, duration))
                .toList();

        // Assert
        assertThat(output).containsExactly("A", "B");
    }

    @Test
    void testThrottlingWithPause() {
        // Arrange
        final var input = Stream.of("A", "B", "C");
        final var duration = Duration.ofMillis(100);
        final long offset = 3;

        // Act
        final var output = input
                .gather(throttle(2, duration))
                .map(_ -> System.currentTimeMillis())
                .toList();

        // Assert
        assertThat(output.get(1) - output.get(0)).isLessThan(duration.toMillis());
        assertThat(output.get(2) - output.get(0)).isGreaterThanOrEqualTo(duration.toMillis() - offset);
    }

    private static class PredictableInstantSource implements InstantSource {

        private final int[] pauses;
        private int invocation;

        private PredictableInstantSource(final int... pauses) {
            this.pauses = pauses;
        }

        @Override
        public Instant instant() {
            final var when = pauses[invocation];
            if (when > 0) {
                LockSupport.parkNanos(when * GathererUtils.NANOS_PER_MILLIS);
            }
            invocation = (invocation + 1) % pauses.length;
            return Instant.now();
        }
    }
}