/*
 * Copyright 2025 Todd Ginsberg
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

import com.ginsberg.gatherers4j.dto.Pair;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static com.ginsberg.gatherers4j.Gatherers4j.crossWith;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

class CrossGathererTest {

    @Nested
    class FromIterable {
        @Test
        @SuppressWarnings("DataFlowIssue")
        void crossIterableMustNotBeNull() {
            assertThatThrownBy(() -> crossWith((Iterable<String>) null)).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void crossesMultipleIterables() {
            // Arrange
            final var input = Stream.of("A", "B", "C");
            final Iterable<Integer> cross1 = List.of(1, 2, 3);
            final Iterable<String> cross2 = List.of("X", "Y");

            // Act
            final var output = input
                    .gather(crossWith(cross1, (item, other) -> item + other))
                    .gather(crossWith(cross2, (item, other) -> item + other))
                    .toList();

            // Assert
            assertThat(output).containsExactly(
                    "A1X", "A1Y", "A2X", "A2Y", "A3X", "A3Y",
                    "B1X", "B1Y", "B2X", "B2Y", "B3X", "B3Y",
                    "C1X", "C1Y", "C2X", "C2Y", "C3X", "C3Y"
            );
        }

        @Test
        void crossesSingleIterable() {
            // Arrange
            final var input = Stream.of("A", "B", "C");
            final Iterable<Integer> cross = List.of(1, 2, 3);

            // Act
            final var output = input
                    .gather(crossWith(cross))
                    .toList();

            // Assert
            assertThat(output).containsExactly(
                    new Pair<>("A", 1), new Pair<>("A", 2), new Pair<>("A", 3),
                    new Pair<>("B", 1), new Pair<>("B", 2), new Pair<>("B", 3),
                    new Pair<>("C", 1), new Pair<>("C", 2), new Pair<>("C", 3)
            );
        }

        @Test
        void emptyCrossIterable() {
            // Arrange
            final var input = Stream.of("A", "B", "C");
            final Iterable<Integer> cross = emptyList();

            // Act
            final var output = input
                    .gather(crossWith(cross))
                    .toList();

            // Assert
            assertThat(output).isEmpty();
        }
    }

    @Nested
    class FromIterator {
        @Test
        @SuppressWarnings("DataFlowIssue")
        void crossIteratorMustNotBeNull() {
            assertThatThrownBy(() -> crossWith((Iterator<String>) null)).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @SuppressWarnings("DataFlowIssue")
        void crossesMultipleStreams() {
            // Arrange
            final var input = Stream.of("A", "B", "C");
            final var cross1 = List.of(1, 2, 3).iterator();
            final var cross2 = List.of("X", "Y").iterator();

            // Act
            final var output = input
                    .gather(crossWith(cross1))
                    .gather(crossWith(cross2))
                    .map(pair -> pair.first().first() + pair.first().second() + pair.second())
                    .toList();

            // Assert
            assertThat(output).containsExactly(
                    "A1X", "A1Y", "A2X", "A2Y", "A3X", "A3Y",
                    "B1X", "B1Y", "B2X", "B2Y", "B3X", "B3Y",
                    "C1X", "C1Y", "C2X", "C2Y", "C3X", "C3Y"
            );
        }

        @Test
        void crossesSingleStream() {
            // Arrange
            final var input = Stream.of("A", "B", "C");
            final var cross = List.of(1, 2, 3).iterator();

            // Act
            final var output = input
                    .gather(crossWith(cross))
                    .toList();

            // Assert
            assertThat(output).containsExactly(
                    new Pair<>("A", 1), new Pair<>("A", 2), new Pair<>("A", 3),
                    new Pair<>("B", 1), new Pair<>("B", 2), new Pair<>("B", 3),
                    new Pair<>("C", 1), new Pair<>("C", 2), new Pair<>("C", 3)
            );
        }

        @Test
        void emptyCrossStream() {
            // Arrange
            final var input = Stream.of("A", "B", "C");
            final Iterator<Integer> cross = Collections.emptyIterator();

            // Act
            final var output = input
                    .gather(crossWith(cross))
                    .toList();

            // Assert
            assertThat(output).isEmpty();
        }
    }

    @Nested
    class FromStream {
        @Test
        @SuppressWarnings("DataFlowIssue")
        void crossStreamMustNotBeNull() {
            assertThatThrownBy(() -> crossWith((Stream<String>) null)).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void crossesMultipleStreams() {
            // Arrange
            final var input = Stream.of("A", "B", "C");
            final var cross1 = Stream.of(1, 2, 3);
            final var cross2 = Stream.of("X", "Y");

            // Act
            final var output = input
                    .gather(crossWith(cross1, (item, other) -> item + other))
                    .gather(crossWith(cross2, (item, other) -> item + other))
                    .toList();

            // Assert
            assertThat(output).containsExactly(
                    "A1X", "A1Y", "A2X", "A2Y", "A3X", "A3Y",
                    "B1X", "B1Y", "B2X", "B2Y", "B3X", "B3Y",
                    "C1X", "C1Y", "C2X", "C2Y", "C3X", "C3Y"
            );
        }

        @Test
        void crossesSingleStream() {
            // Arrange
            final var input = Stream.of("A", "B", "C");
            final var cross = Stream.of(1, 2, 3);

            // Act
            final var output = input
                    .gather(crossWith(cross))
                    .toList();

            // Assert
            assertThat(output).containsExactly(
                    new Pair<>("A", 1), new Pair<>("A", 2), new Pair<>("A", 3),
                    new Pair<>("B", 1), new Pair<>("B", 2), new Pair<>("B", 3),
                    new Pair<>("C", 1), new Pair<>("C", 2), new Pair<>("C", 3)
            );
        }

        @Test
        void emptyCrossStream() {
            // Arrange
            final var input = Stream.of("A", "B", "C");
            final Stream<Integer> cross = Stream.empty();

            // Act
            final var output = input
                    .gather(crossWith(cross))
                    .toList();

            // Assert
            assertThat(output).isEmpty();
        }
    }


    @Nested
    class FromVarArgs {
        @Test
        @SuppressWarnings("DataFlowIssue")
        void crossVarargsNotBeNull() {
            assertThatThrownBy(() -> crossWith((String[]) null)).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @SuppressWarnings("DataFlowIssue")
        void crossesMultipleVarargs() {
            // Arrange
            final var input = Stream.of("A", "B", "C");

            // Act
            final var output = input
                    .gather(crossWith(1, 2, 3))
                    .gather(crossWith("X", "Y"))
                    .map(pair -> pair.first().first() + pair.first().second() + pair.second())
                    .toList();

            // Assert
            assertThat(output).containsExactly(
                    "A1X", "A1Y", "A2X", "A2Y", "A3X", "A3Y",
                    "B1X", "B1Y", "B2X", "B2Y", "B3X", "B3Y",
                    "C1X", "C1Y", "C2X", "C2Y", "C3X", "C3Y"
            );
        }

        @Test
        void crossesSingleVararg() {
            // Arrange
            final var input = Stream.of("A", "B", "C");

            // Act
            final var output = input
                    .gather(crossWith(1, 2, 3))
                    .toList();

            // Assert
            assertThat(output).containsExactly(
                    new Pair<>("A", 1), new Pair<>("A", 2), new Pair<>("A", 3),
                    new Pair<>("B", 1), new Pair<>("B", 2), new Pair<>("B", 3),
                    new Pair<>("C", 1), new Pair<>("C", 2), new Pair<>("C", 3)
            );
        }

        @Test
        void emptyCrossVararg() {
            // Arrange
            final var input = Stream.of("A", "B", "C");
            final var cross = new Integer[] {};

            // Act
            final var output = input
                    .gather(crossWith(cross))
                    .toList();

            // Assert
            assertThat(output).isEmpty();
        }
    }
}