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

import com.ginsberg.gatherers4j.dto.Pair;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import static com.ginsberg.gatherers4j.Gatherers4j.zipWith;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ZipWithGathererTest {

    @Test
    void argumentIterableMustNotBeNull() {
        assertThatThrownBy(() ->zipWith((Iterable<String>) null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void argumentIteratorMustNotBeNull() {
        assertThatThrownBy(() -> zipWith((Iterator<String>) null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void argumentStreamMustNotBeNull() {
        assertThatThrownBy(() -> zipWith((Stream<String>) null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void argumentVarargsMustNotBeNull() {
        assertThatThrownBy(() -> zipWith((String[]) null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void argumentWhenSourceLongerFunctionMustNotBeNull() {
        final var gatherer = zipWith(List.of("A"));
        assertThatThrownBy(() -> gatherer.argumentWhenSourceLonger(null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void sourceWhenArgumentLongerFunctionMustNotBeNull() {
        final var gatherer = zipWith(List.of("A"));
        assertThatThrownBy(() -> gatherer.sourceWhenArgumentLonger(null))
                .isInstanceOf(IllegalArgumentException.class);
    }


    @Test
    void zipWhenArgumentIsLongerFromFunction() {
        // Arrange
        final var left = Stream.of("A");
        final var right = Stream.of(1, 2, 3, 4);

        // Act
        final var output = left
                .gather(zipWith(right).sourceWhenArgumentLonger(String::valueOf))
                .toList();

        // Assert
        assertThat(output)
                .hasSize(4)
                .containsExactly(
                        new Pair<>("A", 1),
                        new Pair<>("2", 2),
                        new Pair<>("3", 3),
                        new Pair<>("4", 4)
                );
    }

    @Test
    void zipWhenArgumentIsLongerNull() {
        // Arrange
        final var left = Stream.of("A");
        final var right = Stream.of(1, 2, 3, 4);

        // Act
        final var output = left
                .gather(Gatherers4j.<String, Integer>zipWith(right).nullSourceWhenArgumentLonger())
                .toList();

        // Assert
        assertThat(output)
                .hasSize(4)
                .containsExactly(
                        new Pair<>("A", 1),
                        new Pair<>(null, 2),
                        new Pair<>(null, 3),
                        new Pair<>(null, 4)
                );
    }

    @Test
    void zipWhenOtherIsEmpty() {
        // Arrange
        final var left = Stream.of("A", "B", "C");
        final Stream<Integer> right = Stream.empty();

        // Act
        final var output = left
                .gather(zipWith(right))
                .toList();

        // Assert
        assertThat(output).isEmpty();
    }

    @Test
    void zipWhenSourceIsLongerFromFunction() {
        // Arrange
        final var left = Stream.of("A", "Bb", "Ccc", "Dddd");
        final var right = Stream.of(4);

        // Act
        final var output = left
                .gather(Gatherers4j.<String, Integer>zipWith(right).argumentWhenSourceLonger(String::length))
                .toList();

        // Assert
        assertThat(output)
                .hasSize(4)
                .containsExactly(
                        new Pair<>("A", 4),
                        new Pair<>("Bb", 2),
                        new Pair<>("Ccc", 3),
                        new Pair<>("Dddd", 4)
                );
    }

    @Test
    void zipWhenSourceIsLongerNull() {
        // Arrange
        final var left = Stream.of("A", "B", "C", "D");
        final var right = Stream.of(1);

        // Act
        final var output = left
                .gather(Gatherers4j.<String, Integer>zipWith(right).nullArgumentWhenSourceLonger())
                .toList();

        // Assert
        assertThat(output)
                .hasSize(4)
                .containsExactly(
                        new Pair<>("A", 1),
                        new Pair<>("B", null),
                        new Pair<>("C", null),
                        new Pair<>("D", null)
                );
    }

    @Test
    void zipWhenThisIsEmpty() {
        // Arrange
        final Stream<String> left = Stream.empty();
        final var right = Stream.of(1, 2, 3);

        // Act
        final var output = left
                .gather(zipWith(right))
                .toList();

        // Assert
        assertThat(output).isEmpty();
    }

    @Test
    void zipWithIterableGatherer() {
        // Arrange
        final var left = Stream.of("A", "B", "C");
        final Iterable<Integer> right = List.of(1, 2, 3, 4);

        // Act
        final var output = left
                .gather(zipWith(right))
                .toList();

        // Assert
        assertThat(output)
                .containsExactly(
                        new Pair<>("A", 1),
                        new Pair<>("B", 2),
                        new Pair<>("C", 3)
                );
    }

    @Test
    void zipWithIterableGathererSourceLonger() {
        // Arrange
        final var left = Stream.of("A", "B", "C");
        final Iterable<Integer> right = List.of(1, 2);

        // Act
        final var output = left
                .gather(zipWith(right))
                .toList();

        // Assert
        assertThat(output)
                .containsExactly(
                        new Pair<>("A", 1),
                        new Pair<>("B", 2)
                );
    }


    @Test
    void zipWithIteratorGatherer() {
        // Arrange
        final var left = Stream.of("A", "B", "C");
        final var right = List.of(1, 2, 3).iterator();

        // Act
        final var output = left
                .gather(zipWith(right))
                .toList();

        // Assert
        assertThat(output)
                .containsExactly(
                        new Pair<>("A", 1),
                        new Pair<>("B", 2),
                        new Pair<>("C", 3)
                );
    }

    @Test
    void zipWithTransform() {
        // Arrange
        final var left = List.of("A", "B", "C");
        final var right = List.of(1, 2, 3);

        // Act
        final ZipWithGatherer<String, ?, String> zipWith = zipWith(right, (s, n) -> s + n);
        final var output1 = left.stream()
                .gather(zipWith)
                .toList();

        final var output2 = left.stream()
                .gather(zipWith)
                .toList();

        // Assert
        assertThat(output1).containsExactly("A1", "B2", "C3");
        assertThat(output2).containsExactly("A1", "B2", "C3");
    }

    @Test
    void zipWithStreamGatherer() {
        // Arrange
        final var left = Stream.of("A", "B", "C");
        final var right = Stream.of(1, 2, 3);

        // Act
        final var output = left
                .gather(zipWith(right))
                .toList();

        // Assert
        assertThat(output)
                .containsExactly(
                        new Pair<>("A", 1),
                        new Pair<>("B", 2),
                        new Pair<>("C", 3)
                );
    }

    @Test
    void zipWithVarargs() {
        // Arrange
        final var left = Stream.of("A", "B", "C");

        // Act
        final var output = left
                .gather(zipWith(1, 2, 3))
                .toList();

        // Assert
        assertThat(output)
                .containsExactly(
                        new Pair<>("A", 1),
                        new Pair<>("B", 2),
                        new Pair<>("C", 3)
                );
    }
}