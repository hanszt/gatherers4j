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

import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static com.ginsberg.gatherers4j.Gatherers4j.flattenSingleOrFail;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FlattenSingleOrFailTest {

    @Test
    void doesNotEmitAnythingDuringFailureCase() {
        // Arrange
        final var input = Stream.of(List.of("A"), List.of("B"));
        final Set<Object> emitted = new HashSet<>();

        // Act
        assertThatThrownBy(() ->
                input.gather(flattenSingleOrFail("More than one input collection"))
                        .peek(emitted::add)
                        .toList()
        ).isExactlyInstanceOf(IllegalStateException.class);

        // Assert
        assertThat(emitted).isEmpty();
    }

    @Test
    void emitsListWhenSinglePresent() {
        // Arrange
        final var input = Stream.of(List.of("A", "B"));

        // Act
        final var output = input.gather(flattenSingleOrFail("More than one input collection")).toList();

        // Assert
        assertThat(output).containsExactly("A", "B");
    }

    @Test
    void emptyStream() {
        // Arrange
        final Stream<List<String>> input = Stream.empty();

        // Act
        final var output = input.gather(flattenSingleOrFail("More than one input collection")).toList();

        // Assert
        assertThat(output).isEmpty();
    }

    @Test
    void failsWhenThereAreMoreThanOneList() {
        assertThatThrownBy(() ->
                Stream.of(List.of("A"), Set.of("A"))
                        .gather(flattenSingleOrFail("More than one input collection"))
                        .toList()
        ).isExactlyInstanceOf(IllegalStateException.class);
    }

    @Test
    void singleElementNull() {
        // Arrange
        final var input = Stream.of((List<String>) null);

        // Act
        final var output = input.gather(flattenSingleOrFail("More than one input collection")).toList();

        // Assert
        assertThat(output).isEmpty();
    }
}