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

import com.ginsberg.gatherers4j.enums.Order;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.ginsberg.gatherers4j.Gatherers4j.*;
import static com.ginsberg.gatherers4j.Gatherers4j.groupBy;
import static java.util.Comparator.comparingInt;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("DataFlowIssue")
class GroupChangingGathererTest {

    @Nested
    class Equals {

        @Test
        void emptyStream() {
            // Arrange
            final Stream<String> input = Stream.empty();

            // Act
            final var output = input.gather(group()).toList();

            // Assert
            assertThat(output).isEmpty();
        }


        @Test
        void groupingByIdentity() {
            // Arrange
            final var input = Stream.of("A", "A", "B", "B", "C", "C", "C");

            // Act
            final var output = input.gather(group()).toList();

            // Assert
            assertThat(output).containsExactly(
                    List.of("A", "A"),
                    List.of("B", "B"),
                    List.of("C", "C", "C")
            );
        }

        @Test
        void nullsMatch() {
            // Arrange
            final var input = Stream.of(null, null, "A");

            // Act
            final var output = input
                    .gather(group()).toList();

            // Assert
            assertThat(output)
                    .containsExactly(
                            Arrays.asList(null, null),
                            List.of("A")
                    );
        }

        @Test
        void returnedListUnmodifiable() {
            // Arrange
            final var input = Stream.of("A", "A", "B", "B", "C", "C", "C");

            // Act
            final var output = input.gather(group()).toList();

            // Assert
            assertThat(output).hasSize(3);
            output.forEach(it ->
                    assertThatThrownBy(() ->
                            it.add("D")
                    ).isInstanceOf(UnsupportedOperationException.class)
            );
        }

    }

    @Nested
    class EqualsBy {

        @Test
        void emptyStream() {
            // Arrange
            final Stream<String> input = Stream.empty();

            // Act
            final var output = input.gather(groupBy(String::length)).toList();

            // Assert
            assertThat(output).isEmpty();
        }

        @Test
        void groupingByFunction() {
            // Arrange
            final var input = Stream.of("A", "B", "AA", "BB", "CCC", "A", "BB", "CCC");

            // Act
            final var output = input.gather(groupBy(String::length)).toList();

            // Assert
            assertThat(output).containsExactly(
                    List.of("A", "B"),
                    List.of("AA", "BB"),
                    List.of("CCC"),
                    List.of("A"),
                    List.of("BB"),
                    List.of("CCC")
            );
        }

        @SuppressWarnings("DataFlowIssue")
        @Test
        void mappingFunctionMustNotBeNull() {
            assertThatThrownBy(() -> groupBy(null)).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        void nullsMatch() {
            // Arrange
            final var input = Stream.of(null, null, "A");

            // Act
            final var output = input
                    .gather(groupBy(it -> it == null ? null : it.length()))
                    .toList();

            // Assert
            assertThat(output)
                    .containsExactly(
                            Arrays.asList(null, null),
                            List.of("A")
                    );
        }

        @Test
        void singleElementStream() {
            // Arrange
            final var input = Stream.of("A");

            // Act
            final var output = input.gather(groupBy(String::length)).toList();

            // Assert
            assertThat(output).containsExactly(
                    List.of("A")
            );
        }
    }

    @Nested
    class WithComparable {
        @Nested
        class Descending {

            @Test
            void descending() {
                // Arrange
                final var input = Stream.of(3, 2, 1, 2, 2, 1);

                // Act
                final var output = input
                        .gather(groupOrdered(Order.Descending))
                        .toList();

                // Assert
                assertThat(output)
                        .containsExactly(
                                List.of(3, 2, 1),
                                List.of(2),
                                List.of(2, 1)
                        );
            }

            @Test
            void emptyStream() {
                // Arrange
                final Stream<String> input = Stream.empty();

                // Act
                final var output = input
                        .gather(groupOrdered(Order.Descending))
                        .toList();

                // Assert
                assertThat(output).isEmpty();
            }

            @Test
            void ensureDescending() {
                // Arrange
                final var input = Stream.of(4, 3, 2, 1);

                // Act
                final var output = input.gather(ensureOrdered(Order.Descending)).toList();

                // Assert
                assertThat(output).containsExactly(4, 3, 2, 1);
            }

            @Test
            void ensureDescendingFailureCase() {
                assertThatThrownBy(() ->
                        Stream.of(1, 1).gather(ensureOrdered(Order.Descending)).toList()
                ).isExactlyInstanceOf(IllegalStateException.class);
            }

            @Test
            void singleElementStream() {
                // Arrange
                final var input = Stream.of("A");

                // Act
                final var output = input
                        .gather(groupOrdered(Order.Descending))
                        .toList();

                // Assert
                assertThat(output).containsExactly(List.of("A"));
            }
        }

        @Nested
        class Ascending {
            @Test
            void emptyStream() {
                // Arrange
                final Stream<String> input = Stream.empty();

                // Act
                final var output = input
                        .gather(groupOrdered(Order.Ascending))
                        .toList();

                // Assert
                assertThat(output).isEmpty();
            }

            @Test
            void ensureAscending() {
                // Arrange
                final var input = Stream.of(1, 2, 3, 4);

                // Act
                final var output = input.gather(ensureOrdered(Order.Ascending)).toList();

                // Assert
                assertThat(output).containsExactly(1, 2, 3, 4);
            }

            @Test
            void ensureAscendingFailureCase() {
                assertThatThrownBy(() ->
                        Stream.of(1, 1).gather(ensureOrdered(Order.Ascending)).toList()
                ).isExactlyInstanceOf(IllegalStateException.class);
            }

            @Test
            void ascending() {
                // Arrange
                final var input = Stream.of(1, 2, 3, 3, 2, 3);

                // Act
                final var output = input
                        .gather(groupOrdered(Order.Ascending))
                        .toList();

                // Assert
                assertThat(output)
                        .containsExactly(
                                List.of(1, 2, 3),
                                List.of(3),
                                List.of(2, 3)
                        );
            }

            @Test
            void singleElementStream() {
                // Arrange
                final var input = Stream.of("A");

                // Act
                final var output = input
                        .gather(groupOrdered(Order.Ascending))
                        .toList();

                // Assert
                assertThat(output).containsExactly(List.of("A"));
            }
        }

        @Nested
        class AscendingOrEqual {
            @Test
            void emptyStream() {
                // Arrange
                final Stream<String> input = Stream.empty();

                // Act
                final var output = input
                        .gather(groupOrdered(Order.AscendingOrEqual))
                        .toList();

                // Assert
                assertThat(output).isEmpty();
            }

            @Test
            void ensureAscendingOrEqual() {
                // Arrange
                final var input = Stream.of(4, 4, 5, 6);

                // Act
                final var output = input.gather(ensureOrdered(Order.AscendingOrEqual)).toList();

                // Assert
                assertThat(output).containsExactly(4, 4, 5, 6);
            }

            @Test
            void ensureAscendingOrEqualFailureCase() {
                final var stream = Stream.of(1, 0).gather(ensureOrdered(Order.AscendingOrEqual));
                assertThatThrownBy(stream::toList).isExactlyInstanceOf(IllegalStateException.class);
            }

            @Test
            void ascendingOrEqual() {
                // Arrange
                final var input = Stream.of(1, 2, 3, 3, 2, 3);

                // Act
                final var output = input
                        .gather(groupOrdered(Order.AscendingOrEqual))
                        .toList();

                // Assert
                assertThat(output)
                        .containsExactly(
                                List.of(1, 2, 3, 3),
                                List.of(2, 3)
                        );
            }

            @Test
            void singleElementStream() {
                // Arrange
                final var input = Stream.of("A");

                // Act
                final var output = input
                        .gather(groupOrdered(Order.AscendingOrEqual))
                        .toList();

                // Assert
                assertThat(output).containsExactly(List.of("A"));
            }
        }

        @Nested
        class DescendingOrEqual {
            @Test
            void emptyStream() {
                // Arrange
                final Stream<String> input = Stream.empty();

                // Act
                final var output = input
                        .gather(groupOrdered(Order.DescendingOrEqual))
                        .toList();

                // Assert
                assertThat(output).isEmpty();
            }

            @Test
            void ensureDescendingOrEqual() {
                // Arrange
                final var input = Stream.of(4, 3, 2, 2);

                // Act
                final var output = input.gather(ensureOrdered(Order.DescendingOrEqual)).toList();

                // Assert
                assertThat(output).containsExactly(4, 3, 2, 2);
            }

            @Test
            void ensureDescendingOrEqualFailureCase() {
                assertThatThrownBy(() ->
                        Stream.of(1, 2).gather(ensureOrdered(Order.DescendingOrEqual)).toList()
                ).isExactlyInstanceOf(IllegalStateException.class);
            }

            @Test
            void descendingOrEqual() {
                // Arrange
                final var input = Stream.of(3, 2, 1, 2, 2, 1);

                // Act
                final var output = input
                        .gather(groupOrdered(Order.DescendingOrEqual))
                        .toList();

                // Assert
                assertThat(output)
                        .containsExactly(
                                List.of(3, 2, 1),
                                List.of(2, 2, 1)
                        );
            }

            @Test
            void singleElementStream() {
                // Arrange
                final var input = Stream.of("A");

                // Act
                final var output = input
                        .gather(groupOrdered(Order.DescendingOrEqual))
                        .toList();

                // Assert
                assertThat(output).containsExactly(List.of("A"));
            }

        }
    }

    @Nested
    class WithComparator {

        @Nested
        class Common {
            @SuppressWarnings("DataFlowIssue")
            @Test
            void comparatorMustNotBeNull() {
                assertThatThrownBy(() ->
                        groupOrderedBy(Order.Ascending, null)
                ).isExactlyInstanceOf(IllegalArgumentException.class);
            }

            @SuppressWarnings("DataFlowIssue")
            @Test
            void operationMustNotBeNull() {
                assertThatThrownBy(() ->
                        groupOrderedBy(null, (_, _) -> 0)
                ).isExactlyInstanceOf(IllegalArgumentException.class);
            }

            @Test
            void returnedListUnmodifiable() {
                // Arrange
                final var input = Stream.of("A", "B", "C");

                // Act
                final var output = input
                        .gather(groupOrderedBy(Order.Ascending, comparingInt(String::length)))
                        .toList();

                // Assert
                assertThat(output)
                        .hasSize(3)
                        .allSatisfy(it ->
                                assertThatThrownBy(() -> it.add("D"))
                                        .isInstanceOf(UnsupportedOperationException.class));
            }
        }

        @Nested
        class Descending {
            @Test
            void descending() {
                // Arrange
                final var input = Stream.of("AAA", "AA", "A", "AA", "AA", "A");

                // Act
                final var output = input
                        .gather(groupOrderedBy(Order.Descending, comparingInt(String::length)))
                        .toList();

                // Assert
                assertThat(output)
                        .containsExactly(
                                List.of("AAA", "AA", "A"),
                                List.of("AA"),
                                List.of("AA", "A")
                        );
            }

            @Test
            void emptyStream() {
                // Arrange
                final Stream<String> input = Stream.empty();

                // Act
                final var output = input
                        .gather(groupOrderedBy(Order.Descending, comparingInt(String::length)))
                        .toList();

                // Assert
                assertThat(output).isEmpty();
            }

            @Test
            void ensureDecreasingFailureCase() {
                final var stream = Stream.of("A", "AA")
                        .gather(ensureOrderedBy(Order.Descending, comparingInt(String::length)));
                assertThatThrownBy(stream::toList).isExactlyInstanceOf(IllegalStateException.class);
            }

            @Test
            void ensureOrderedDescending() {
                // Arrange
                final var input = Stream.of("AAA", "AA", "A");

                // Act
                final var output = input
                        .gather(ensureOrderedBy(Order.Descending, comparingInt(String::length)))
                        .toList();

                // Assert
                assertThat(output).containsExactly("AAA", "AA", "A");
            }

            @Test
            void singleElementStream() {
                // Arrange
                final var input = Stream.of("A");

                // Act
                final var output = input
                        .gather(groupOrderedBy(Order.Descending, comparingInt(String::length)))
                        .toList();

                // Assert
                assertThat(output).containsExactly(List.of("A"));
            }
        }

        @Nested
        class Ascending {
            @Test
            void emptyStream() {
                // Arrange
                final Stream<String> input = Stream.empty();

                // Act
                final var output = input
                        .gather(groupOrderedBy(Order.Ascending, comparingInt(String::length)))
                        .toList();

                // Assert
                assertThat(output).isEmpty();
            }

            @Test
            void ensureAscending() {
                // Arrange
                final var input = Stream.of("A", "AA", "AAA");

                // Act
                final var output = input
                        .gather(ensureOrderedBy(Order.Ascending, comparingInt(String::length)))
                        .toList();

                // Assert
                assertThat(output).containsExactly("A", "AA", "AAA");
            }

            @Test
            void ensureAscendingFailureCase() {
                final var stream = Stream.of("AA", "A")
                        .gather(ensureOrderedBy(Order.Ascending, comparingInt(String::length)));
                assertThatThrownBy(stream::toList)
                        .isExactlyInstanceOf(IllegalStateException.class);
            }

            @Test
            void ascending() {
                // Arrange
                final var input = Stream.of("A", "AA", "AAA", "AAA", "AA", "AAA");

                // Act
                final var output = input
                        .gather(groupOrderedBy(Order.Ascending, comparingInt(String::length)))
                        .toList();

                // Assert
                assertThat(output)
                        .containsExactly(
                                List.of("A", "AA", "AAA"),
                                List.of("AAA"),
                                List.of("AA", "AAA")
                        );
            }

            @Test
            void singleElementStream() {
                // Arrange
                final var input = Stream.of("A");

                // Act
                final var output = input
                        .gather(groupOrderedBy(Order.Ascending, comparingInt(String::length)))
                        .toList();

                // Assert
                assertThat(output).containsExactly(List.of("A"));
            }
        }

        @Nested
        class AscendingOrEqual {

            @Test
            void emptyStream() {
                // Arrange
                final Stream<String> input = Stream.empty();

                // Act
                final var output = input
                        .gather(groupOrderedBy(Order.AscendingOrEqual, comparingInt(String::length)))
                        .toList();

                // Assert
                assertThat(output).isEmpty();
            }

            @Test
            void ensureAscendingOrEqual() {
                // Arrange
                final var input = Stream.of("A", "A", "A");

                // Act
                final var output = input
                        .gather(ensureOrderedBy(Order.AscendingOrEqual, comparingInt(String::length)))
                        .toList();

                // Assert
                assertThat(output).containsExactly("A", "A", "A");
            }

            @Test
            void ensureAscendingOrEqualFailureCase() {
                final var stream = Stream.of("AA", "A")
                        .gather(ensureOrderedBy(Order.AscendingOrEqual, comparingInt(String::length)));
                assertThatThrownBy(stream::toList).isExactlyInstanceOf(IllegalStateException.class);
            }

            @Test
            void nonDecreasing() {
                // Arrange
                final var input = Stream.of("A", "AA", "AAA", "AAA", "AA", "AAA");

                // Act
                final var output = input
                        .gather(groupOrderedBy(Order.AscendingOrEqual, comparingInt(String::length)))
                        .toList();

                // Assert
                assertThat(output)
                        .containsExactly(
                                List.of("A", "AA", "AAA", "AAA"),
                                List.of("AA", "AAA")
                        );
            }

            @Test
            void singleElementStream() {
                // Arrange
                final var input = Stream.of("A");

                // Act
                final var output = input
                        .gather(groupOrderedBy(Order.AscendingOrEqual, comparingInt(String::length)))
                        .toList();

                // Assert
                assertThat(output).containsExactly(List.of("A"));
            }
        }

        @Nested
        class DescendingOrEqual {
            @Test
            void emptyStream() {
                // Arrange
                final Stream<String> input = Stream.empty();

                // Act
                final var output = input
                        .gather(groupOrderedBy(Order.DescendingOrEqual, comparingInt(String::length)))
                        .toList();

                // Assert
                assertThat(output).isEmpty();
            }

            @Test
            void ensureDescendingOrEqual() {
                // Arrange
                final var input = Stream.of("A", "A", "A");

                // Act
                final var output = input
                        .gather(ensureOrderedBy(Order.DescendingOrEqual, comparingInt(String::length)))
                        .toList();

                // Assert
                assertThat(output).containsExactly("A", "A", "A");
            }

            @Test
            void ensureDescendingOrEqualFailureCase() {
                final var stream = Stream.of("AA", "AAA")
                        .gather(ensureOrderedBy(Order.DescendingOrEqual, comparingInt(String::length)));
                assertThatThrownBy(stream::toList).isExactlyInstanceOf(IllegalStateException.class);
            }

            @Test
            void descendingOrEqual() {
                // Arrange
                final var input = Stream.of("AAA", "AA", "A", "AA", "AA", "A");

                // Act
                final var output = input
                        .gather(groupOrderedBy(Order.DescendingOrEqual, comparingInt(String::length)))
                        .toList();

                // Assert
                assertThat(output)
                        .containsExactly(
                                List.of("AAA", "AA", "A"),
                                List.of("AA", "AA", "A")
                        );
            }

            @Test
            void singleElementStream() {
                // Arrange
                final var input = Stream.of("A");

                // Act
                final var output = input
                        .gather(groupOrderedBy(Order.DescendingOrEqual, comparingInt(String::length)))
                        .toList();

                // Assert
                assertThat(output).containsExactly(List.of("A"));
            }
        }
    }

}