package com.ginsberg.gatherers4j.util;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CircularBufferTest {

    @Test
    void add() {
        // Arrange
        final var cb = new CircularBuffer<String>(2);

        // Act
        Stream.of("A", "B").forEach(cb::add);

        // Assert
        assertThat(cb.toList()).containsExactly("A", "B");
    }

    @Test
    void addOverwritesFirst() {
        // Arrange
        final var cb = new CircularBuffer<String>(2);

        // Act
        List.of("A", "B", "C").forEach(cb::add);

        // Assert
        assertThat(cb).containsExactly("B", "C");
        assertThat(cb.toList()).containsExactly("B", "C");
    }

    @Test
    void toList() {
        // Arrange
        final var cb = new CircularBuffer<String>(4);
        List.of("A", "B", "C", "D").forEach(cb::add);

        // Act
        final var output = cb.toList();
        cb.add("E");

        // Assert
        assertThat(output).containsExactly("A", "B", "C", "D");
        //noinspection DataFlowIssue
        assertThatThrownBy(() -> output.add("E")).isInstanceOf(UnsupportedOperationException.class);
        assertThat(cb.toList()).containsExactly("B", "C", "D", "E");
    }

    @Test
    void toListEmpty() {
        // Arrange
        final var cb = new CircularBuffer<String>(5);

        // Act
        final var output = cb.toList();

        // Assert
        assertThat(output).isEmpty();
    }

    @Test
    void createdEmpty() {
        // Arrange
        final var cb = new CircularBuffer<String>(2);

        // Assert
        assertThat(cb.isEmpty()).isTrue();
        assertThat(cb.size()).isZero();
    }

    @Test
    void drop() {
        // Arrange
        final var cb = new CircularBuffer<String>(5);
        Stream.of("A", "B", "C", "D").forEach(cb::add);

        // Act
        cb.drop(3);

        // Assert
        assertThat(cb.size()).isEqualTo(1);
        assertThat(cb.toList()).containsExactly("D");
    }

    @Test
    void dropMoreThanSize() {
        // Arrange
        final var cb = new CircularBuffer<String>(5);
        Stream.of("A", "B", "C", "D").forEach(cb::add);

        // Act
        cb.drop(6);

        // Assert
        assertThat(cb.size()).isZero();
        assertThat(cb.isEmpty()).isTrue();
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    void dropWithoutEffect(final int drops) {
        // Arrange
        final var cb = new CircularBuffer<String>(2);
        Stream.of("A", "B").forEach(cb::add);

        // Act
        cb.drop(drops);

        // Assert
        assertThat(cb.toList()).containsExactly("A", "B");
    }

    @Nested
    class Iterator {

        @Test
        void iterator() {
            // Arrange
            final var cb = new CircularBuffer<String>(5);
            List.of("A", "B", "C", "D").forEach(cb::add);

            // Act
            final var iterator = cb.iterator();

            // Assert
            assertThat(iterator).toIterable().containsExactly("A", "B", "C", "D");
        }

        @Test
        void iteratorWhenOverrides() {
            // Arrange
            final var cb = new CircularBuffer<String>(3);
            List.of("A", "B", "C", "D", "E").forEach(cb::add);

            // Act
            final var iterator = cb.iterator();

            // Assert
            assertThat(iterator).toIterable().containsExactly("C", "D", "E");
        }

        @Test
        void iteratorDoesNotSupportRemove() {
            final var cb = new CircularBuffer<String>(5);
            Stream.of("A", "B", "C", "D").forEach(cb::add);
            final var iterator = cb.iterator();

            assertThatThrownBy(iterator::remove)
                    .isExactlyInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        void iteratorEmpty() {
            // Arrange
            final var cb = new CircularBuffer<String>(5);

            // Act
            final var iterator = cb.iterator();

            // Assert
            assertThat(iterator).toIterable().isEmpty();
        }

        @Test
        void iteratorEmptyWhenNext() {
            final var iterator = new CircularBuffer<>(1).iterator();
            assertThatThrownBy(iterator::next)
                    .isExactlyInstanceOf(NoSuchElementException.class);
        }
    }

    @Test
    void removeFirst() {
        // Arrange
        final var cb = new CircularBuffer<String>(2);
        Stream.of("A", "B").forEach(cb::add);

        // Act
        final var removed = cb.removeFirst();

        // Assert
        assertThat(removed).isEqualTo("A");
        assertThat(cb.size()).isEqualTo(1);
    }

    @Test
    void removeFirstWhenEmpty() {
        final var buffer = new CircularBuffer<>(1);
        assertThatThrownBy(buffer::removeFirst)
                .isExactlyInstanceOf(NoSuchElementException.class);
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0})
    void sizeMustBeGreaterThanZero(final int value) {
        assertThatThrownBy(() -> new CircularBuffer<>(value))
                .isExactlyInstanceOf(IllegalArgumentException.class);
    }
}