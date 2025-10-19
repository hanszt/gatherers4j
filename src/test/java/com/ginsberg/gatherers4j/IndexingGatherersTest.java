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

import com.ginsberg.gatherers4j.dto.WithIndex;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static com.ginsberg.gatherers4j.Gatherers4j.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexingGatherersTest {

    @Nested
    class FilterIndexed {

        @Test
        void filterWithIndex() {
            // Arrange
            final var input = Stream.of("A", "B", "C", "D");

            // Act
            final var output = input
                    .gather(filterIndexed((index, element) ->
                            index % 2 == 0 || "D".equals(element))
                    )
                    .toList();

            // Assert
            assertThat(output).containsExactly("A", "C", "D");
        }

        @SuppressWarnings("DataFlowIssue")
        @Test
        void predicateMustNotBeNull() {
            assertThatThrownBy(() -> filterIndexed(null)).isExactlyInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    class MapIndexed {
        @SuppressWarnings("DataFlowIssue")
        @Test
        void mappingFunctionMustNotBeNull() {
            assertThatThrownBy(() -> mapIndexed(null))
                    .isExactlyInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void mapIndexedEmpty() {
            // Arrange
            final Stream<String> input = Stream.empty();

            // Act
            final var output = input.gather(mapIndexed((_, element) -> element)).toList();

            // Assert
            assertThat(output).isEmpty();
        }

        @Test
        void testMapIndexed() {
            // Arrange
            final var input = Stream.of("A", "B", "C");

            // Act
            final var output = input.gather(mapIndexed((index, element) -> element + index)).toList();

            // Assert
            assertThat(output).containsExactly("A0", "B1", "C2");
        }

    }

    @Nested
    class PeekIndexed {
        @SuppressWarnings("DataFlowIssue")
        @Test
        void peekingFunctionMustNotBeNull() {
            assertThatThrownBy(() -> peekIndexed(null))
                    .isExactlyInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void peekIndexedEmpty() {
            // Arrange
            final Stream<String> input = Stream.empty();
            final List<String> peeked = new ArrayList<>();

            // Act
            final var output = input.gather(peekIndexed((index, element) -> peeked.add(element + index))).toList();

            // Assert
            assertThat(output).isEmpty();
            assertThat(peeked).isEmpty();
        }

        @Test
        void testPeekIndexed() {
            // Arrange
            final var input = Stream.of("A", "B", "C");
            final List<String> peeked = new ArrayList<>();

            // Act
            final var output = input.gather(peekIndexed((index, element) -> peeked.add(element + index))).toList();

            // Assert
            assertThat(output).containsExactly("A", "B", "C");
            assertThat(peeked).containsExactly("A0", "B1", "C2");
        }

    }

    @Nested
    class WithIndexes {
        @Test
        void objectWithIndex() {
            // Arrange
            final var input = Stream.of("A", "B", "C");

            // Act
            final var output = input
                    .gather(withIndex())
                    .toList();

            // Assert
            assertThat(output)
                    .containsExactly(
                            new WithIndex<>(0, "A"),
                            new WithIndex<>(1, "B"),
                            new WithIndex<>(2, "C")
                    );
        }

        @Test
        void integerWithIndex() {
            // Arrange
            final var input = Stream.of(1, 2, 3);

            // Act
            final var output = input
                    .gather(withIndex())
                    .toList();

            // Assert
            assertThat(output)
                    .containsExactly(
                            new WithIndex<>(0, 1),
                            new WithIndex<>(1, 2),
                            new WithIndex<>(2, 3)
                    );
        }
    }
}