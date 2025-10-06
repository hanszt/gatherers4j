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

import java.util.stream.Stream;

import static com.ginsberg.gatherers4j.Gatherers4j.intersperse;
import static org.assertj.core.api.Assertions.assertThat;

class IntersperseGathererTest {

    @Test
    void testIntersperse() {
        // Arrange
        final var input = Stream.of("A", "B", "C");

        // Act
        final var output = input.gather(intersperse("::")).toList();

        // Assert
        assertThat(output).containsExactly("A", "::", "B", "::", "C");
    }

    @Test
    void intersperseEmpty() {
        // Arrange
        final Stream<String> input = Stream.empty();

        // Act
        final var output = input.gather(intersperse("-")).toList();

        // Assert
        assertThat(output).isEmpty();
    }

    @Test
    void intersperseNull() {
        // Arrange
        final var input = Stream.of("A", "B", "C");

        // Act
        final var output = input.gather(intersperse(null)).toList();

        // Assert
        assertThat(output).containsExactly("A", null, "B", null, "C");
    }

    @Test
    void intersperseSingle() {
        // Arrange
        final var input = Stream.of("A");

        // Act
        final var output = input.gather(intersperse("-")).toList();

        // Assert
        assertThat(output).containsExactly("A");
    }

}