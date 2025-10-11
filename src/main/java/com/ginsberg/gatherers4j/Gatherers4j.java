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

import module com.ginsberg.gatherers4j;
import module java.base;
import com.ginsberg.gatherers4j.util.CircularBuffer;
import com.ginsberg.gatherers4j.util.GathererUtils;
import org.jspecify.annotations.Nullable;

import java.util.stream.Gatherer.Downstream;
import java.util.stream.Gatherer.Integrator;

import static com.ginsberg.gatherers4j.util.GathererUtils.*;

/// This is the main entry-point for the Gatherers4j library. All available gatherers
/// are created from static methods on this class.
public final class Gatherers4j {

    private Gatherers4j() {
        // No
    }

    /// Cross every element of the input stream with every element of the given `Iterable`, emitting them
    /// to the output stream as a `Pair<T, S>`.
    ///
    /// @param <T>    Type of element in the input stream
    /// @param <S>    Type of element in the crossWith `Iterable`
    /// @param source The Iterable to source with
    /// @return A non-null Gatherer
    public static <T extends @Nullable Object, S extends @Nullable Object> Gatherer<T, ?, Pair<T, S>> crossWith(
            final Iterable<S> source
    ) {
        return crossWith(source, Pair::new);
    }

    public static <T extends @Nullable Object, S extends @Nullable Object, R extends @Nullable Object> Gatherer<T, ?, R> crossWith(
            final Iterable<S> source,
            final BiFunction<? super T, ? super S, ? extends R> crossFunction
    ) {
        mustNotBeNull(source, "source must not be null");
        mustNotBeNull(crossFunction, "crossFunction must not be null");
        return Gatherer.of((_, element, downstream) -> {
            for (final var cross : source) {
                downstream.push(crossFunction.apply(element, cross));
            }
            return !downstream.isRejecting();
        });
    }

    /// Cross every element of the input stream with every element of the given `Iterator`, emitting them
    /// to the output stream as a `Pair<T, S>`.
    ///
    /// Note: the Iterator is consumed fully and stored as a List in memory.
    ///
    /// @param <T>    Type of element in the input stream
    /// @param <S>    Type of element in the crossWith `Iterator`
    /// @param source The Iterator to cross with
    /// @return A non-null Gatherer
    public static <T extends @Nullable Object, S extends @Nullable Object> Gatherer<T, ?, Pair<T, S>> crossWith(
            final Iterator<S> source
    ) {
        mustNotBeNull(source, "source must not be null");
        return crossWith(StreamSupport.stream(Spliterators.spliteratorUnknownSize(source, Spliterator.ORDERED), false).toList());
    }

    /// Cross every element of the input stream with every element of the given `Stream`, emitting them
    /// to the output stream as a `Pair<T, S>`.
    ///
    /// Note: the Iterator is consumed fully and stored as a List in memory.
    /// Note: The Ghostbusters warned us about this and I hereby absolve myself of any responsibility if you cause some kind of cataclysm.
    ///
    /// @param <T>    Type of element in the input stream
    /// @param <S>    Type of element in the crossWith `Iterator`
    /// @param source The Stream to cross with
    /// @return A non-null Gatherer
    public static <T extends @Nullable Object, S extends @Nullable Object> Gatherer<T, ?, Pair<T, S>> crossWith(
            final Stream<S> source
    ) {
        mustNotBeNull(source, "source must not be null");
        return crossWith(source.toList());
    }

    public static <T extends @Nullable Object, S extends @Nullable Object, R extends @Nullable Object> Gatherer<T, ?, R> crossWith(
            final Stream<S> source,
            final BiFunction<? super T, ? super S, ? extends R> crossFunction
    ) {
        mustNotBeNull(source, "source must not be null");
        return crossWith(source.toList(), crossFunction);
    }

    /// Cross every element of the input stream with every element provided, emitting them
    /// to the output stream as a `Pair<T, S>`.
    ///
    /// Note: The Ghostbusters warned us about this and I hereby absolve myself of any responsibility if you cause some kind of cataclysm.
    ///
    /// @param <T>    Type of element in the input stream
    /// @param <S>    Type of element in the crossWith `Iterator`
    /// @param source Elements to cross with the input stream
    /// @return A non-null Gatherer
    @SafeVarargs
    public static <T extends @Nullable Object, S extends @Nullable Object> Gatherer<T, ?, Pair<T, S>> crossWith(
            final S... source
    ) {
        mustNotBeNull(source, "source must not be null");
        return crossWith(() -> Spliterators.iterator(Arrays.spliterator(source)));
    }

    /// Limit the number of elements in the stream to some number per period, dropping anything over the
    /// limit during the period.
    ///
    /// @param amount   A positive number of elements to allow per period
    /// @param duration A positive duration for the length of the period
    /// @param <T>      Type of elements in both the input and output streams
    /// @return A non-null `ThrottlingGatherer`
    public static <T extends @Nullable Object> ThrottlingGatherer<T> debounce(
            final int amount,
            final Duration duration
    ) {
        return throttle(ThrottlingGatherer.LimitRule.Drop, amount, duration, Clock.systemUTC());
    }

    /// Remove consecutive duplicate elements from a stream as measured by `Object.equals(Object)`
    ///
    /// @param <T> Type of elements in both the input and output streams
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, T> dedupeConsecutive() {
        return dedupeConsecutiveBy(e -> e);
    }

    /// Remove consecutive duplicates from a stream where duplication is measured by the given `function`.
    ///
    /// @param selector A non-null function, the results of which will be used to check for consecutive duplication.
    /// @param <T>      Type of elements in both the input and output streams
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, T> dedupeConsecutiveBy(
            final Function<? super T, @Nullable Object> selector
    ) {
        mustNotBeNull(selector, "Mapping function must not be null");
        class State {
            @Nullable
            Object value = null;
            boolean hasValue = false;

            boolean integrate(T element, Downstream<? super T> downstream) {
                final var mapped = selector.apply(element);
                if (!hasValue) {
                    hasValue = true;
                    value = mapped;
                    return downstream.push(element);
                } else if (!Objects.equals(value, mapped)) {
                    value = mapped;
                    return downstream.push(element);
                }
                return !downstream.isRejecting();
            }
        }
        return Gatherer.ofSequential(State::new, Integrator.<State, T, T>ofGreedy(State::integrate));
    }

    /// Filter a stream such that it only contains distinct elements measured by the given `function`.
    ///
    /// @param selector A non-null mapping function, the results of which will be used to check for distinct elements
    /// @param <T>      Type of elements in both the input and output streams
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, T> distinctBy(
            final Function<T, @Nullable Object> selector
    ) {
        mustNotBeNull(selector, "Mapping function must not be null");
        return Gatherer.ofSequential(
                HashSet::new,
                Integrator.ofGreedy((seen, item, downstream) -> {
                    if (seen.add(selector.apply(item))) {
                        downstream.push(item);
                    }
                    return !downstream.isRejecting();
                })
        );
    }

    /// Drop every nth element of the stream.
    ///
    /// @param count The number of the elements to drop, must be at least 2
    /// @param <T>   Type of elements in both the input and output streams
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, T> dropEveryNth(final int count) {
        if (count < 2) {
            throw new IllegalArgumentException("Count must be a minimum of 2");
        }
        return filterIndexed((index, _) -> index % count != 0);
    }

    /// Keep all elements except the last `count` elements of the stream.
    ///
    /// @param count A positive number of elements to drop from the end of the stream
    /// @param <T>   Type of elements in both the input and output streams
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, T> dropLast(final int count) {
        require(count > 0, "DropLast count must be greater than zero");
        return Gatherer.ofSequential(
                () -> new CircularBuffer<T>(count),
                Integrator.ofGreedy((items, item, downstream) -> {
                    if (items.size() == count) {
                        downstream.push(items.removeFirst());
                    }
                    items.add(item);
                    return !downstream.isRejecting();
                })
        );
    }

    /// Ensure that the `Comparable` elements in the input stream are in the given `Order`, and fail exceptionally if they are not.
    ///
    /// @param <T>   Type of elements in the input stream
    /// @param order The non-null order the stream must be in.
    /// @return A non-null Gatherer
    public static <T extends Comparable<T>> Gatherer<T, ?, T> ensureOrdered(final Order order) {
        final Gatherer<T, ?, List<T>> groupOrderedBy = groupOrdered(order);
        return groupOrderedBy.andThen(flattenSingleOrFail("Elements not in proper order: " + order.name()));
    }

    /// Note: "Single" in this case means at most one. The naming of this more precisely seemed clumsy.
    static <I extends Iterable<T>, T> Gatherer<I, ?, T> flattenSingleOrFail(final String message) {
        mustNotBeNull(message, "message must not be null");
        class State {
            boolean isFirst = true;
            @Nullable I firstIterable = null;

            private boolean integrate(I iterable, Downstream<? super T> downstream) {
                if (isFirst) {
                    firstIterable = iterable;
                    isFirst = false;
                    return !downstream.isRejecting();
                } else {
                    throw new IllegalStateException(message);
                }
            }

            private void finish(Downstream<? super T> downstream) {
                if (firstIterable != null) {
                    pushWhileNotRejecting(firstIterable, downstream);
                }
            }
        }
        return Gatherer.<I, State, T>ofSequential(State::new, State::integrate, State::finish);
    }

    /// Ensure that the elements in the input stream are in the given `Order` as measured by the given `Comparator`, and fail exceptionally if they are not.
    ///
    /// @param <T>        Type of elements in the input stream
    /// @param order      The non-null order the stream must be in.
    /// @param comparator The non-null comparator used to compare stream elements
    /// @return A non-null Gatherer
    public static <T> Gatherer<T, ?, T> ensureOrderedBy(final Order order, final Comparator<T> comparator) {
        return groupOrderedBy(order, comparator)
                .andThen(flattenSingleOrFail("Elements not in proper order: " + order.name()));
    }

    /// Ensure the input stream's meets the given `size` criteria, and emit all elements if so.
    /// If not, throw an `IllegalStateException`.
    ///
    /// @param size   The Size to measure the stream length against
    /// @param length Number to compare stream length against
    /// @param <T>    Type of elements in both the input and output streams
    /// @return A non-null `SizeGatherer`
    /// @throws IllegalStateException when the input stream is not exactly `size` elements long
    public static <T extends @Nullable Object> SizeGatherer<T> ensureSize(
            final Size size,
            final long length
    ) {
        return new SizeGatherer<>(size, length);
    }

    /// Create a Stream that represents the exponential moving average of a `Stream<BigDecimal>`, with the given `alpha`.
    ///
    /// @param alpha The alpha value to use in the EMA calculation.
    /// @return A non-null `BigDecimalExponentialMovingAverageGatherer`
    public static BigDecimalExponentialMovingAverageGatherer<@Nullable BigDecimal> exponentialMovingAverageWithAlpha(final double alpha) {
        return BigDecimalExponentialMovingAverageGatherer.withAlpha(alpha, Function.identity());
    }

    /// Create a Stream that represents the exponential moving average of a `BigDecimal` objects mapped from a `Stream<T>`
    /// via a `mappingFunction` and using the given `alpha`.
    ///
    /// @param alpha           The alpha value to use in the EMA calculation.
    /// @param mappingFunction A function to map `<T>` objects to `BigDecimal`, the results of which will be used in the exponential average calculation
    /// @param <T>             Type of elements in the input stream, to be remapped to `BigDecimal` by the `mappingFunction`
    /// @return A non-null `BigDecimalExponentialMovingAverageGatherer`
    public static <T extends @Nullable Object> BigDecimalExponentialMovingAverageGatherer<T> exponentialMovingAverageWithAlphaBy(
            final double alpha,
            final Function<T, BigDecimal> mappingFunction
    ) {
        return BigDecimalExponentialMovingAverageGatherer.withAlpha(alpha, mappingFunction);
    }

    /// Create a Stream that represents the exponential moving average of a `Stream<BigDecimal>`, over the given number of `periods`.
    ///
    /// @param periods The number of periods to use in the EMA calculation.
    /// @return A non-null `BigDecimalExponentialMovingAverageGatherer`
    public static BigDecimalExponentialMovingAverageGatherer<@Nullable BigDecimal> exponentialMovingAverageWithPeriod(final int periods) {
        return BigDecimalExponentialMovingAverageGatherer.withPeriod(periods, Function.identity());
    }

    /// Create a Stream that represents the exponential moving average of a `BigDecimal` objects mapped from a `Stream<T>`
    /// via a `mappingFunction` over the given number of `periods`.
    ///
    /// @param periods         The number of periods to use in the EMA calculation.
    /// @param mappingFunction A function to map `<T>` objects to `BigDecimal`, the results of which will be used in the exponential average calculation
    /// @param <T>             Type of elements in the input stream, to be remapped to `BigDecimal` by the `mappingFunction`
    /// @return A non-null `BigDecimalExponentialMovingAverageGatherer`
    public static <T extends @Nullable Object> BigDecimalExponentialMovingAverageGatherer<T> exponentialMovingAverageWithPeriodBy(
            final int periods,
            final Function<T, BigDecimal> mappingFunction
    ) {
        return BigDecimalExponentialMovingAverageGatherer.withPeriod(periods, mappingFunction);
    }

    /// Filter a stream according to the given `predicate`, which takes both the item being examined,
    /// and its index.
    ///
    /// @param predicate A non-null `BiPredicate<Integer,T>` where the `Integer` is the zero-based index of the element being filtered, and the `T` is the element itself.
    /// @param <T>       Type of elements in the input stream
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, T> filterIndexed(
            final BiPredicate<Integer, T> predicate
    ) {
        return IndexingGatherers.filterIndexed(predicate);
    }

    /// Filter the elements in the stream to only include elements of the given types.
    /// Note, due to how generics work you may end up with some... interesting stream types as a result
    ///
    /// @param <T>        Type of elements in the input stream
    /// @param <R>        Type of elements in the output stream
    /// @param validTypes A non-empty array of types to filter for
    /// @return A non-null `Gatherer`
    @SafeVarargs
    public static <T extends @Nullable Object, R extends @Nullable Object> Gatherer<T, ?, R> filterInstanceOf(
            final Class<? extends R>... validTypes
    ) {
        mustNotBeNull(validTypes, "validTypes must not be null");
        require(validTypes.length != 0, "Must provide at least one type");
        return Gatherer.of((_, element, downstream) -> {
            for (final var type : validTypes) {
                if (type.isInstance(element)) {
                    return downstream.push(type.cast(element));
                }
            }
            return !downstream.isRejecting();
        });
    }

    /// Filter the input stream so that it contains `Comparable` elements in the `order` specified. Anything not matching
    /// that order is removed as it is encountered.
    ///
    /// @param <T> Type of elements in the input and output stream
    /// @return A non-null gatherer
    public static <T extends Comparable<T>> Gatherer<T, ?, T> filterOrdered(final Order order) {
        return filterOrderedBy(order, Comparable::compareTo);
    }

    /// Filter the input stream so that it contains elements in the `order` specified as measured by the given `Comparator`.
    /// Anything not matching that order is removed as it is encountered.
    ///
    /// @param <T>        Type of elements in the input and output stream
    /// @param comparator A non-null `Comparator` to compare stream elements
    /// @return A non-null gatherer
    public static <T> Gatherer<T, ?, T> filterOrderedBy(final Order order, final Comparator<T> comparator) {
        mustNotBeNull(order, "Order must not be null");
        mustNotBeNull(comparator, "Comparator must not be null");
        class State {
            boolean first = true;
            @Nullable T previous = null;

            boolean integrate(T item, Downstream<? super T> downstream) {
                if (first) {
                    downstream.push(item);
                    previous = item;
                    first = false;
                } else if (order.allows(comparator.compare(item, previous))) {
                    downstream.push(item);
                    previous = item;
                }
                return !downstream.isRejecting();
            }
        }
        return Gatherer.ofSequential(State::new, (Integrator.Greedy<State, T, T>) State::integrate);
    }

    ///  Perform a fold over every element in the input stream along with its index
    ///
    /// @param <T>          Type of elements in the input stream
    /// @param <R>          Type elements are folded to (the accumulated value)
    /// @param initialValue Initial value of the fold
    /// @param foldFunction Function that performs the fold given an element, its index, and the carry value
    /// @return A non-null Gatherer
    public static <T extends @Nullable Object, R extends @Nullable Object> Gatherer<T, ?, R> foldIndexed(
            final Supplier<R> initialValue,
            final IndexedAccumulatorFunction<? super R, ? super T, ? extends R> foldFunction
    ) {
        return accumulate(false, initialValue, foldFunction);
    }

    private static <T extends @Nullable Object, R extends @Nullable Object> Gatherer<T, ?, R> accumulate(
            boolean running,
            Supplier<R> initialValue,
            IndexedAccumulatorFunction<? super R, ? super T, ? extends R> accumulatorFunction
    ) {
        mustNotBeNull(accumulatorFunction, "Accumulator function must not be null");
        mustNotBeNull(initialValue, "Initial value supplier must not be null");
        class State {
            @Nullable R carriedValue = initialValue.get();
            int index = 0;

            boolean integrate(T element, Downstream<? super R> downstream) {
                carriedValue = accumulatorFunction.apply(index++, carriedValue, element);
                if (running) {
                    downstream.push(carriedValue);
                }
                return !downstream.isRejecting();
            }

            void finish(Downstream<? super R> downstream) {
                if (!downstream.isRejecting() && !running) {
                    downstream.push(carriedValue);
                }
            }
        }
        final Integrator.Greedy<State, T, R> integrator = State::integrate;
        return Gatherer.<T, State, R>ofSequential(State::new, integrator, State::finish);
    }

    /// Turn a `Stream<T>` into a `Stream<List<T>>` where adjacent equal elements are in the same `List`
    /// and equality is measured by `Object.equals(Object)`. The lists emitted to the output stream are unmodifiable.
    ///
    /// @param <T> Type of elements in the input stream
    /// @return A non-null `GroupingByGatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, List<T>> group() {
        return groupOrderedBy(Order.Equal, equalityOnlyComparator());
    }

    /// Turn a `Stream<T>` into a `Stream<List<T>>` where adjacent equal elements are in the same `List`
    /// and equality is measured by the given `mappingFunction`. The lists emitted to the output stream are unmodifiable.
    ///
    /// @param mappingFunction A non-null function, the results of which are used to measure equality of consecutive elements.
    /// @param <T>             Type of elements in the input stream
    /// @return A non-null `GroupingByGatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, List<T>> groupBy(
            final Function<@Nullable T, @Nullable Object> mappingFunction
    ) {
        mustNotBeNull(mappingFunction, "mappingFunction must not be null");
        return groupOrderedBy(Order.Equal, equalityOnlyComparator(mappingFunction));
    }

    /// Turn a `Stream<Comparable>` into a `Stream<List<>>` where adjacent equal elements are in the same `List`
    /// and order is measured by the order imposed by the `Comparable`. The lists emitted to the output stream are unmodifiable.
    ///
    /// @param <T> Type of elements in the input stream, implementing `Comparable`
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Comparable<T>> Gatherer<T, ?, List<T>> groupOrdered(final Order order) {
        return groupOrderedBy(order, Comparable::compareTo);
    }

    /// Turn a `Stream<T>` into a `Stream<List<T>>` where adjacent equal elements are in the same `List`
    /// and order is measured by the given `Comparator`. The lists emitted to the output stream are unmodifiable.
    ///
    /// @param comparator A non-null function, the results of which are used to measure equality of consecutive elements.
    /// @param <T>        Type of elements in the input stream
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, List<T>> groupOrderedBy(
            final Order order,
            final Comparator<T> comparator
    ) {
        mustNotBeNull(order, "Order must not be null");
        mustNotBeNull(comparator, "Comparator must not be null");
        class State {
            final List<T> items = new ArrayList<>();

            boolean integrate(T item, Downstream<? super List<T>> downstream) {
                if (!items.isEmpty()) {
                    final T previous = items.getLast();
                    if (!order.allows(comparator.compare(item, previous))) {
                        downstream.push(Collections.unmodifiableList(new ArrayList<>(items)));
                        items.clear();
                    }
                }
                items.add(item);
                return !downstream.isRejecting();
            }

            void finish(Downstream<? super List<T>> downstream) {
                if (!downstream.isRejecting() && !items.isEmpty()) {
                    downstream.push(Collections.unmodifiableList(items));
                }
            }
        }
        final Integrator.Greedy<State, T, List<T>> integrator = State::integrate;
        return Gatherer.<T, State, List<T>>ofSequential(State::new, integrator, State::finish);
    }

    /// Creates a stream of alternating objects from the input stream and the argument iterable
    ///
    /// @param other A non-null Iterable to interleave
    /// @param <T>   Type of elements in both the input stream and argument iterable
    /// @return A non-null `InterleavingGatherer`
    public static <T extends @Nullable Object> InterleavingGatherer<T> interleaveWith(final Iterable<T> other) {
        mustNotBeNull(other, "Other iterable must not be null");
        return new InterleavingGatherer<>(other.spliterator(), false, false);
    }

    /// Creates a stream of alternating objects from the input stream and the argument iterator
    ///
    /// @param other A non-null Iterator to interleave
    /// @param <T>   Type of elements in both the input stream and argument iterator
    /// @return A non-null `InterleavingGatherer`
    public static <T extends @Nullable Object> InterleavingGatherer<T> interleaveWith(final Iterator<T> other) {
        mustNotBeNull(other, "Other iterable must not be null");
        return interleaveWith(() -> other);
    }

    /// Creates a stream of alternating objects from the input stream and the argument stream
    ///
    /// @param other A non-null stream to interleave
    /// @param <T>   Type of elements in both the input and argument streams
    /// @return A non-null `InterleavingGatherer`
    public static <T extends @Nullable Object> InterleavingGatherer<T> interleaveWith(final Stream<T> other) {
        mustNotBeNull(other, "Other stream must not be null");
        return new InterleavingGatherer<>(other.spliterator(), false, false);
    }

    /// Creates a stream of alternating objects from the input stream and the provided elements
    ///
    /// @param other Non-null elements to interleave
    /// @param <T>   Type of elements in both the input stream and argument iterator
    /// @return A non-null `InterleavingGatherer`
    @SafeVarargs
    public static <T extends @Nullable Object> InterleavingGatherer<T> interleaveWith(final T... other) {
        mustNotBeNull(other, "Other stream must not be null");
        return new InterleavingGatherer<>(Arrays.spliterator(other), false, false);
    }

    /// Intersperse the given `intersperseElement` between each element of the input stream.
    ///
    /// @param intersperseElement The element to intersperse, which may be null
    /// @param <T>                The type of elements in the stream and the element to intersperse
    /// @return A non-null Gatherer
    public static <T extends @Nullable Object> Gatherer<T, ?, T> intersperse(final T intersperseElement) {
        return intersperseBy(_ -> intersperseElement);
    }

    public static <T extends @Nullable Object> Gatherer<T, ?, T> intersperseBy(final Function<? super T, ? extends T> intersperser) {
        mustNotBeNull(intersperser, "intersperser must not be null");
        return Gatherer.ofSequential(
                () -> new Object() {
                    boolean hasStarted = false;
                },
                Integrator.ofGreedy((state, item, downstream) -> {
                            if (state.hasStarted) {
                                downstream.push(intersperser.apply(item));
                            } else {
                                state.hasStarted = true;
                            }
                            return downstream.push(item);
                        }
                )
        );
    }

    /// Perform a mapping operation given the element being mapped and its zero-based index.
    ///
    /// @param <T>             The type of elements in the input stream
    /// @param <R>             The type of elements in the output stream
    /// @param mappingFunction A non-null function to map input to output, given an input and its index
    /// @return A non-null Gatherer
    public static <T extends @Nullable Object, R extends @Nullable Object> Gatherer<T, ?, R> mapIndexed(
            final BiFunction<Integer, T, R> mappingFunction) {
        return IndexingGatherers.mapIndexed(mappingFunction);
    }

    /// Create a Stream that represents the moving product of a `Stream<BigDecimal>` looking
    /// back `windowSize` number of elements.
    ///
    /// @param windowSize The trailing number of elements to multiply, must be greater than 1.
    /// @return A non-null `BigDecimalMovingProductGatherer`
    public static BigDecimalMovingProductGatherer<@Nullable BigDecimal> movingProduct(final int windowSize) {
        return new BigDecimalMovingProductGatherer<>(windowSize, Function.identity());
    }

    /// Create a Stream that represents the moving product of a `BigDecimal` objects mapped from a `Stream<T>`
    /// via a `mappingFunction` and looking back `windowSize` number of elements.
    ///
    /// @param windowSize      The trailing number of elements to multiply, must be greater than 1.
    /// @param mappingFunction A function to map `<T>` objects to `BigDecimal`, the results of which will be used in the moving product calculation
    /// @param <T>             Type of elements in the input stream, to be remapped to `BigDecimal` by the `mappingFunction`
    /// @return A non-null `BigDecimalMovingProductGatherer`
    public static <T extends @Nullable Object> BigDecimalMovingProductGatherer<T> movingProductBy(
            final int windowSize,
            final Function<T, BigDecimal> mappingFunction
    ) {
        return new BigDecimalMovingProductGatherer<>(windowSize, mappingFunction);
    }

    /// Create a Stream that represents the moving sum of a `Stream<BigDecimal>` looking
    /// back `windowSize` number of elements.
    ///
    /// @param windowSize The trailing number of elements to add, must be greater than 1.
    /// @return A non-null `BigDecimalMovingSumGatherer`
    public static BigDecimalMovingSumGatherer<@Nullable BigDecimal> movingSum(final int windowSize) {
        return new BigDecimalMovingSumGatherer<>(windowSize, Function.identity());
    }

    /// Create a Stream that represents the moving sum of a `BigDecimal` objects mapped from a `Stream<T>`
    /// via a `mappingFunction` and looking back `windowSize` number of elements.
    ///
    /// @param windowSize      The trailing number of elements to multiply, must be greater than 1.
    /// @param mappingFunction A function to map `<T>` objects to `BigDecimal`, the results of which will be used in the moving sum calculation
    /// @param <T>             Type of elements in the input stream, to be remapped to `BigDecimal` by the `mappingFunction`
    /// @return A non-null `BigDecimalMovingSumGatherer`
    public static <T extends @Nullable Object> BigDecimalMovingSumGatherer<T> movingSumBy(
            final int windowSize,
            final Function<T, BigDecimal> mappingFunction
    ) {
        return new BigDecimalMovingSumGatherer<>(windowSize, mappingFunction);
    }

    /// Emit elements in the input stream ordered by frequency in the direction specified. Elements are emitted wrapped
    /// in `WithCount<T>` objects that carry the element and the number of occurrences.
    ///
    /// Note: This consumes the entire stream and holds it in memory, so it will not work on infinite
    /// streams and may cause memory pressure on very large streams.
    ///
    /// @param <T> Type of elements in the input stream
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, WithCount<T>> orderByFrequency(final Frequency order) {
        mustNotBeNull(order, "Order must be specified");
        class State {
            final Map<T, Long> counts = new HashMap<>();

            boolean integrate(T element, Downstream<? super WithCount<T>> downstream) {
                counts.merge(element, 1L, Long::sum);
                return !downstream.isRejecting();
            }

            State combine(State other) {
                other.counts.forEach((key, value) -> counts.merge(key, value, Long::sum));
                return this;
            }

            void finish(Downstream<? super WithCount<T>> downstream) {
                final var counts = this.counts
                        .entrySet()
                        .stream().map(it -> new WithCount<>(it.getKey(), it.getValue()))
                        .sorted(comparator());
                pushWhileNotRejecting(counts, downstream);
            }

            Comparator<WithCount<T>> comparator() {
                return order == Frequency.Descending ?
                        ((o1, o2) -> (int) (o2.count() - o1.count())) :
                        ((o1, o2) -> (int) (o1.count() - o2.count()));
            }
        }
        final Integrator.Greedy<State, T, WithCount<T>> integrator = State::integrate;
        return Gatherer.<T, State, WithCount<T>>of(State::new, integrator, State::combine, State::finish);
    }

    /// Peek at each element along with its zero-based index.
    ///
    /// @param <T>             The type of elements in the input stream
    /// @param peekingConsumer A non-null consumer to peek at each element and its index
    /// @return A non-null Gatherer
    public static <T extends @Nullable Object> Gatherer<T, ?, T> peekIndexed(
            final BiConsumer<Integer, T> peekingConsumer) {
        return IndexingGatherers.peekIndexed(peekingConsumer);
    }

    private static final int INFINITE = -1;

    /// Repeatedly emit the input stream to the output stream a given number of times.
    /// Note: This implementation consumes the entire input stream into memory, so it must be used on finite streams.
    ///
    /// @param <T>     Type of elements in the input and output stream
    /// @param repeats Number of repeats, must be greater than 1
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, T> repeat(final int repeats) {
        require(repeats >= 0, "Number of repeats must not be negative");
        return repeatInternal(repeats);
    }

    /// Repeatedly emit the input stream to the output stream infinitely.
    /// Note: This implementation consumes the entire input stream into memory, so it must be used on finite streams.
    ///
    /// @param <T> Type of elements in the input and output stream
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, T> repeatInfinitely() {
        return repeatInternal(INFINITE);
    }

    private static <T extends @Nullable Object> Gatherer<T, ?, T> repeatInternal(final int repeats) {
        return Gatherer.ofSequential(
                () -> new Object() {
                    int repeatsRemaining = repeats;
                    final List<T> items = new ArrayList<>();
                },
                Integrator.ofGreedy((state, element, downstream) -> {
                    state.items.add(element);
                    return repeats != 0 && !downstream.isRejecting();
                }),
                (state, downstream) -> {
                    while (!downstream.isRejecting() && (state.repeatsRemaining == INFINITE || state.repeatsRemaining > 0)) {

                        pushWhileNotRejecting(state.items, downstream);
                        if (state.repeatsRemaining != INFINITE) {
                            state.repeatsRemaining--;
                        }
                    }
                }
        );
    }

    /// Reverse the order of the input Stream.
    ///
    /// Note: This consumes the entire stream and holds it in memory, so it will not work on infinite
    /// streams and may cause memory pressure on very large streams.
    ///
    /// @param <T> Type of elements in the input stream
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, T> reverse() {
        return Gatherer.ofSequential(
                ArrayList<T>::new,
                Integrator.ofGreedy((items, element, downstream) ->
                        items.add(element) && !downstream.isRejecting()),
                (items, downstream) -> {
                    for (var i = items.size() - 1; i >= 0 && !downstream.isRejecting(); i--) {
                        downstream.push(items.get(i));
                    }
                }
        );
    }

    /// Consume the entire stream and emit its elements rotated in the direction specified `distance` number of spaces
    ///
    /// @param <T>       Type of elements in the input and output stream
    /// @param direction Which direction to rotate the stream in
    /// @param distance  Distance to rotate elements
    /// @return A non-null Gatherer
    public static <T extends @Nullable Object> Gatherer<T, ?, T> rotate(final Rotate direction, final int distance) {
        mustNotBeNull(direction, "direction must not be null");
        class State {
            final List<T> fullStream = new ArrayList<>();
            final Rotate dir = distance < 0 ? direction.flip() : direction;
            final int dist = Math.abs(distance);

            boolean rotate(T element, Downstream<? super T> downstream) {
                if (dist == 0) {
                    downstream.push(element);
                } else if (dir == Rotate.Left && fullStream.size() == dist) {
                    downstream.push(element);
                } else {
                    fullStream.add(element);
                }
                return !downstream.isRejecting();
            }

            void flush(Downstream<? super T> downstream) {
                final var size = fullStream.size();
                if (size == 0) {
                    return;
                }
                final var rotateDistance = dist % size;
                for (var i = 0; i < size; i++) {
                    if (dir == Rotate.Left) {
                        downstream.push(fullStream.get((i + rotateDistance) % size));
                    } else {
                        downstream.push(fullStream.get((i - rotateDistance + size) % size));
                    }
                }
            }
        }
        return Gatherer.<T, State, T>ofSequential(
                State::new,
                Integrator.<State, T, T>ofGreedy(State::rotate),
                State::flush
        );
    }

    /// Create a `Stream<BigDecimal>` that represents the running population standard
    /// deviation of a `Stream<BigDecimal>`.
    ///
    /// @return A non-null `BigDecimalStandardDeviationGatherer`
    public static BigDecimalStandardDeviationGatherer<@Nullable BigDecimal> runningPopulationStandardDeviation() {
        return new BigDecimalStandardDeviationGatherer<>(
                BigDecimalStandardDeviationGatherer.Mode.Population,
                Function.identity()
        );
    }

    /// Create a `Stream<BigDecimal>` that represents the running population standard deviation of a `BigDecimal`
    /// objects mapped from a `Stream<BigDecimal>` via a `mappingFunction`.
    ///
    /// @param mappingFunction A function to map `<T>` objects to `BigDecimal`, the results of which will be used in the standard deviation calculation
    /// @param <T>             Type of elements in the input stream, to be remapped to `BigDecimal` by the `mappingFunction`
    /// @return A non-null `BigDecimalStandardDeviationGatherer`
    public static <T extends @Nullable Object> BigDecimalStandardDeviationGatherer<T> runningPopulationStandardDeviationBy(
            final Function<T, BigDecimal> mappingFunction
    ) {
        return new BigDecimalStandardDeviationGatherer<>(
                BigDecimalStandardDeviationGatherer.Mode.Population,
                mappingFunction
        );
    }

    /// Create a `Stream<BigDecimal>` that represents the running product of a `Stream<BigDecimal>`.
    ///
    /// @return A non-null `BigDecimalProductGatherer`
    public static BigDecimalProductGatherer<@Nullable BigDecimal> runningProduct() {
        return new BigDecimalProductGatherer<>(Function.identity());
    }

    /// Create a `Stream<BigDecimal>` that represents the running product of `BigDecimal` objects mapped
    /// from a `Stream<T>` via a `mappingFunction`.
    ///
    /// @param mappingFunction A function to map `<T>` objects to `BigDecimal`, the results of which will be used in the product calculation
    /// @param <T>             Type of elements in the input stream, to be remapped to `BigDecimal` by the `mappingFunction`
    /// @return A non-null `BigDecimalProductGatherer`
    public static <T extends @Nullable Object> BigDecimalProductGatherer<T> runningProductBy(
            final Function<T, BigDecimal> mappingFunction
    ) {
        return new BigDecimalProductGatherer<>(mappingFunction);
    }

    /// Create a `Stream<BigDecimal>` that represents the running sample standard deviation of a `Stream<BigDecimal>`.
    ///
    /// @return A non-null `BigDecimalStandardDeviationGatherer`
    public static BigDecimalStandardDeviationGatherer<@Nullable BigDecimal> runningSampleStandardDeviation() {
        return new BigDecimalStandardDeviationGatherer<>(
                BigDecimalStandardDeviationGatherer.Mode.Sample,
                Function.identity()
        );
    }

    /// Create a `Stream<BigDecimal>` that represents the running sample standard deviation of `BigDecimal` objects mapped
    /// from a `Stream<T>` via a `mappingFunction`.
    ///
    /// @param mappingFunction A function to map `<T>` objects to `BigDecimal`, the results of which will be used in the standard deviation calculation
    /// @param <T>             Type of elements in the input stream, to be remapped to `BigDecimal` by the `mappingFunction`
    /// @return A non-null `BigDecimalStandardDeviationGatherer`
    public static <T extends @Nullable Object> BigDecimalStandardDeviationGatherer<T> runningSampleStandardDeviationBy(
            final Function<T, BigDecimal> mappingFunction
    ) {
        return new BigDecimalStandardDeviationGatherer<>(
                BigDecimalStandardDeviationGatherer.Mode.Sample,
                mappingFunction
        );
    }

    /// Create a `Stream<BigDecimal>` that represents the running sum of a `Stream<BigDecimal>`.
    ///
    /// @return A non-null `BigDecimalSumGatherer`
    public static BigDecimalSumGatherer<@Nullable BigDecimal> runningSum() {
        return new BigDecimalSumGatherer<>(Function.identity());
    }

    /// Create a `Stream<BigDecimal>` that represents the running sum of `BigDecimal` objects mapped
    /// from a `Stream<T>` via a `mappingFunction`.
    ///
    /// @param mappingFunction A function to map `<T>` objects to `BigDecimal`, the results of which will be used in the running sum calculation
    /// @param <T>             Type of elements in the input stream, to be remapped to `BigDecimal` by the `mappingFunction`
    /// @return A non-null `BigDecimalSumGatherer`
    public static <T extends @Nullable Object> BigDecimalSumGatherer<T> runningSumBy(
            final Function<T, BigDecimal> mappingFunction
    ) {
        return new BigDecimalSumGatherer<>(mappingFunction);
    }

    /// Perform a fixed size sampling over the input stream. This method uses the Reservoir method internally, which
    /// should guarantee the correct number of elements returned. If the stream is shorter than the specified `sampleSize`
    /// then all elements are emitted. Elements will be emitted in the order in which they are encountered.
    /// This implementation reads the entire stream before emitting any results making it inappropriate for infinite streams.
    ///
    /// @param sampleSize Number of elements to sample.
    /// @param <T>        Type of elements in the input and output stream
    /// @return A non-null Gatherer
    public static <T extends @Nullable Object> Gatherer<T, ?, T> sampleFixedSize(final int sampleSize) {
        return sampleFixedSize(sampleSize, RandomGenerator.getDefault());
    }

    public static <T extends @Nullable Object> Gatherer<T, ?, T> sampleFixedSize(
            final int sampleSize,
            final RandomGenerator random
    ) {
        require(sampleSize > 0, "sampleSize must be at least 1");
        class State {
            private final List<T> elements = new ArrayList<>();
            private int index = 0;

            boolean take(final @Nullable T element, Downstream<? super T> downstream) {
                if (index < sampleSize) {
                    elements.add(element);
                } else {
                    final var n = random.nextInt(0, index);
                    if (n < sampleSize) {
                        // Not replacing element at n because we want to keep iteration order.
                        elements.remove(n);
                        elements.add(element);
                    }
                }
                index++;
                return !downstream.isRejecting();
            }
        }
        return Gatherer.ofSequential(
                State::new,
                Integrator.<State, T, T>ofGreedy(State::take),
                (state, downstream) -> pushWhileNotRejecting(state.elements, downstream)
        );
    }

    /// Perform a percentage-based sampling over the input stream. This method uses Poisson sampling internally, so
    /// the number of elements emitted to the downstream may not be strictly in line with the given `percentage`.
    /// Elements will be emitted in the order in which they are encountered.
    ///
    /// @param percentage      Percentage of elements that should be sampled, on average.
    /// @param randomGenerator the random generator to use for sampling
    /// @param <T>             Type of elements in the input and output stream
    /// @return A non-null Gatherer
    public static <T extends @Nullable Object> Gatherer<T, ?, T> samplePercentage(
            final double percentage,
            final RandomGenerator randomGenerator
    ) {
        require(percentage > 0.0 && percentage <= 1.0, "percentage must be between 0.0 and 1.0");
        return Gatherer.ofSequential(
                Integrator.ofGreedy((_, element, downstream) -> {
                    if (randomGenerator.nextDouble() < percentage) {
                        return downstream.push(element);
                    }
                    return !downstream.isRejecting();
                })
        );
    }

    ///  Perform a scan over every element in the input stream along with its index
    ///
    /// @param <T>          Type of elements in the input stream
    /// @param <R>          Type elements are accumulated to
    /// @param initialValue Initial value of the scan
    /// @param scanFunction Function that performs the accumulation given an element, its index, and the carry value
    /// @return A non-null Gatherer
    public static <T extends @Nullable Object, R extends @Nullable Object> Gatherer<T, ?, R> scanIndexed(
            final Supplier<R> initialValue,
            final IndexedAccumulatorFunction<R, T, R> scanFunction
    ) {
        return accumulate(true, initialValue, scanFunction);
    }

    /// Shuffle the input stream into a random order.
    ///
    /// Note: This consumes the entire stream and holds it in memory, so it will not work on infinite
    /// streams and may cause memory pressure on very large streams.
    ///
    /// @param <T> Type of elements in the input stream
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, T> shuffle() {
        return shuffle(RandomGenerator.getDefault());
    }

    /// Shuffle the input stream into a random order.
    ///
    /// Note: This consumes the entire stream and holds it in memory, so it will not work on infinite
    /// streams and may cause memory pressure on very large streams.
    ///
    /// @param randomGenerator A non-null `RandomGenerator` to use as a random source for the shuffle
    /// @param <T>             Type of elements in the input stream
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, T> shuffle(final RandomGenerator randomGenerator) {
        mustNotBeNull(randomGenerator, "RandomGenerator must not be null");
        return Gatherer.ofSequential(
                ArrayList<T>::new,
                Integrator.ofGreedy((items, item, downstream) ->
                        items.add(item) && !downstream.isRejecting()),
                (items, downstream) -> {
                    while (!items.isEmpty() && !downstream.isRejecting()) {
                        final var randomSlot = randomGenerator.nextInt(items.size());
                        final var last = items.removeLast();
                        if (randomSlot < items.size()) {
                            downstream.push(items.set(randomSlot, last));
                        } else {
                            downstream.push(last);
                        }
                    }
                }
        );
    }

    /// Create a Stream that represents the simple moving average of a `Stream<BigDecimal>` looking
    /// back `windowSize` number of elements.
    ///
    /// @param windowSize The number of elements to average, must be greater than 1.
    /// @return A non-null `BigDecimalSimpleMovingAverageGatherer`
    public static BigDecimalSimpleMovingAverageGatherer<@Nullable BigDecimal> simpleMovingAverage(final int windowSize) {
        return new BigDecimalSimpleMovingAverageGatherer<>(windowSize, Function.identity());
    }

    /// Create a Stream that represents the simple moving average of a `BigDecimal` objects mapped from a `Stream<T>`
    /// via a `mappingFunction` and looking back `windowSize` number of elements.
    ///
    /// @param windowSize      The number of elements to average, must be greater than 1.
    /// @param mappingFunction A function to map `<T>` objects to `BigDecimal`, the results of which will be used in the moving average calculation
    /// @param <T>             Type of elements in the input stream, to be remapped to `BigDecimal` by the `mappingFunction`
    /// @return A non-null `BigDecimalSimpleMovingAverageGatherer`
    public static <T extends @Nullable Object> BigDecimalSimpleMovingAverageGatherer<T> simpleMovingAverageBy(
            final int windowSize,
            final Function<T, BigDecimal> mappingFunction
    ) {
        return new BigDecimalSimpleMovingAverageGatherer<>(windowSize, mappingFunction);
    }

    /// Create a Stream that is the running average of `Stream<BigDecimal>`
    ///
    /// @return BigDecimalSimpleAverageGatherer
    public static BigDecimalSimpleAverageGatherer<@Nullable BigDecimal> simpleRunningAverage() {
        return simpleRunningAverageBy(Function.identity());
    }

    /// Create a Stream that is the running average of `BigDecimal` objects as mapped by
    /// the given function. This is useful when paired with the `withOriginal` function.
    ///
    /// @param mappingFunction A function to map `<T>` objects to `BigDecimal`, the results of which will be used in the running average calculation
    /// @param <T>             Type of elements in the input stream, to be remapped to `BigDecimal` by the `mappingFunction`
    /// @return A non-null `BigDecimalSimpleAverageGatherer`
    public static <T extends @Nullable Object> BigDecimalSimpleAverageGatherer<T> simpleRunningAverageBy(
            final Function<T, BigDecimal> mappingFunction
    ) {
        return new BigDecimalSimpleAverageGatherer<>(mappingFunction);
    }

    /// Take every nth element of the stream.
    ///
    /// @param count The number of the elements to keep, must be at least 2
    /// @param <T>   Type of elements in both the input and output streams
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, T> takeEveryNth(final int count) {
        if (count < 2) {
            throw new IllegalArgumentException("Count must be a minimum of 2");
        }
        return filterIndexed((index, _) -> index % count == 0);
    }

    /// Emit the last `count` elements from the stream. If there are fewer than `count` elements they are all emitted.
    ///
    /// @param count A non-negative integer, the number of elements to return
    /// @param <T>   Type of elements in the input stream
    /// @return A non-null `Gatherer`
    public static <T> Gatherer<T, ?, T> takeLast(final int count) {
        require(count >= 0, "Last count must not be negative");
        return Gatherer.ofSequential(
                () -> new CircularBuffer<T>(count),
                Integrator.ofGreedy((items, item, downstream) ->
                        items.add(item) && !downstream.isRejecting()),
                GathererUtils::pushWhileNotRejecting
        );
    }

    /// Take elements from the input stream until the `predicate` is met, including the first element that
    /// matches the `predicate`.
    ///
    /// @param predicate A non-null predicate function
    /// @param <T>       Type of elements in both the input and output streams
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, T> takeUntil(
            final Predicate<T> predicate
    ) {
        mustNotBeNull(predicate, "Predicate must not be null");
        return Gatherer.ofSequential(
                () -> new Object() {
                    boolean done = false;
                },
                (state, item, downstream) -> {
                    if (state.done) return false;
                    state.done = predicate.test(item);
                    return !downstream.isRejecting() && downstream.push(item);
                }
        );
    }

    /// Limit the number of elements in the stream to some number per period. When the limit is reached,
    /// consumption is paused until a new period starts and the count resets.
    ///
    /// @param amount   A positive number of elements to allow per period
    /// @param duration A positive duration for the length of the period
    /// @param <T>      Type of elements in the input stream
    /// @return A non-null `ThrottlingGatherer`
    public static <T extends @Nullable Object> ThrottlingGatherer<T> throttle(
            final int amount,
            final Duration duration
    ) {
        return new ThrottlingGatherer<>(ThrottlingGatherer.LimitRule.Pause, amount, duration, Clock.systemUTC());
    }

    /// Emit only those elements that occur in the input stream a single time.
    ///
    /// @param <T> Type of elements in the input stream
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, T> uniquelyOccurring() {
        return uniquelyOccurringBy(e -> e);
    }

    /// Emit only those elements that occur in the input stream a single time.
    ///
    /// @param <T> Type of elements in the input stream
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object, S extends @Nullable Object> Gatherer<T, ?, T> uniquelyOccurringBy(
            final Function<? super T, ? extends S> selector
    ) {
        mustNotBeNull(selector, "Selector must not be null");
        class State {
            final Set<S> duplicates = new HashSet<>();
            final Map<S, T> found = new LinkedHashMap<>();

            boolean integrate(T element, Downstream<? super T> downstream) {
                final var selected = selector.apply(element);
                if (!duplicates.contains(selected)) {
                    if (found.containsKey(selected)) {
                        duplicates.add(selected);
                        found.remove(selected);
                    } else {
                        found.put(selected, element);
                    }
                }
                return !downstream.isRejecting();
            }

            State combine(State other) {
                for (final var element : other.duplicates) {
                    duplicates.add(element);
                    found.remove(element);
                }
                for (final var e : other.found.entrySet()) {
                    final var selected = e.getKey();
                    if (!duplicates.contains(selected)) {
                        if (found.containsKey(selected)) {
                            found.remove(selected);
                            duplicates.add(selected);
                        } else {
                            found.put(selected, e.getValue());
                        }
                    }
                }
                return this;
            }
        }
        return Gatherer.of(
                State::new,
                Integrator.<State, T, T>ofGreedy(State::integrate),
                State::combine,
                (state, downstream) -> pushWhileNotRejecting(state.found.values(), downstream)
        );
    }

    /// Create windows over the elements of the input stream that are `windowSize` in length, sliding over `stepping` number of elements
    /// and optionally including partial windows at the end of ths stream.
    ///
    /// @param <T>             Type of elements in the input and output stream
    /// @param windowSize      Size of the window, must be greater than 0
    /// @param stepping        Number of elements to slide over each time a window has filled, must be greater than 0
    /// @param includePartials To include left-over partial windows at the end of the stream or not
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, List<T>> window(final int windowSize, final int stepping, final boolean includePartials) {
        require(windowSize > 0, "Window size must be greater than zero");
        require(stepping > 0, "Stepping must be greater than zero");
        class Window {
            int stepDelta = 0;
            final CircularBuffer<T> window = new CircularBuffer<>(windowSize);

            boolean integrate(T element, Downstream<? super List<T>> downstream) {
                if (stepDelta == 0) {
                    window.add(element);
                } else {
                    stepDelta--;
                }
                if (window.size() == windowSize) {
                    downstream.push(window.toList());
                    stepDelta = Math.max(0, stepping - windowSize);
                    window.drop(stepping);
                }
                return !downstream.isRejecting();
            }

            void finish(Downstream<? super List<T>> downstream) {
                if (includePartials) {
                    while (!downstream.isRejecting() && !window.isEmpty()) {
                        downstream.push(window.toList());
                        window.drop(stepping);
                    }
                }
            }
        }
        final Integrator.Greedy<Window, T, List<T>> integrator = Window::integrate;
        return Gatherer.<T, Window, List<T>>ofSequential(Window::new, integrator, Window::finish);
    }

    /// Maps all elements of the stream as-is along with their 0-based index.
    ///
    /// @param <T> Type of elements in the input stream
    /// @return A non-null `SimpleIndexingGatherers`
    public static <T extends @Nullable Object> Gatherer<T, ?, WithIndex<T>> withIndex() {
        return IndexingGatherers.withIndex();
    }

    /// Creates a stream of `Pair<T,S>` objects whose values come from the stream this is called on
    /// and the argument collection
    ///
    /// @param other A non-null iterable to zip with
    /// @param <T>   Type of object in the source stream
    /// @param <S>   Type of object in the argument `Iterable`
    /// @return A non-null `ZipWithGatherer`
    public static <T extends @Nullable Object, S extends @Nullable Object> ZipWithGatherer<T, S, Pair<T, S>> zipWith(
            final Iterable<S> other
    ) {
        return zipWith(other, Pair::new);
    }

    public static <T extends @Nullable Object, S extends @Nullable Object, R extends @Nullable Object> ZipWithGatherer<T, S, R> zipWith(
            final Iterable<S> other,
            final BiFunction<? super T, ? super S, ? extends R> mapper
    ) {
        mustNotBeNull(other, "Other iterable must not be null");
        return new ZipWithGatherer<>(other.spliterator(), mapper, null, null);
    }

    /// Creates a stream of `Pair<T,S>` objects whose values come from the stream this is called on
    /// and the argument iterator
    ///
    /// @param other A non-null iterator to zip with
    /// @param <T>   Type of object in the source stream
    /// @param <S>   Type of object in the argument `Iterator`
    /// @return A non-null `ZipWithGatherer`
    public static <T extends @Nullable Object, S extends @Nullable Object> ZipWithGatherer<T, S, Pair<T, S>> zipWith(
            final Iterator<S> other
    ) {
        mustNotBeNull(other, "Other iterator must not be null");
        return zipWith(() -> other, Pair::new);
    }

    /// Creates a stream of `Pair<T,S>` objects whose values come from the stream this is called on
    /// and the argument stream
    ///
    /// @param other A non-null stream to zip with
    /// @param <T>   Type of object in the source stream
    /// @param <S>   Type of object in the argument `Stream`
    /// @return A non-null `ZipWithGatherer`
    public static <T extends @Nullable Object, S extends @Nullable Object> ZipWithGatherer<T, S, Pair<T, S>> zipWith(
            final Stream<S> other
    ) {
        return zipWith(other, Pair::new);
    }

    public static <T extends @Nullable Object, S extends @Nullable Object, R extends @Nullable Object> ZipWithGatherer<T, S, R> zipWith(
            final Stream<S> other,
            final BiFunction<? super T, ? super S, ? extends R> mapper
    ) {
        mustNotBeNull(other, "Other stream must not be null");
        return new ZipWithGatherer<>(other.spliterator(), mapper, null, null);
    }

    /// Creates a stream of `Pair<T,S>` objects whose values come from the stream this is called on
    /// and the argument elements provide as a varargs
    ///
    /// @param other A non-zero number of elements to zip with
    /// @param <T>   Type of object in the source stream
    /// @param <S>   Type of object in the argument `Stream`
    /// @return A non-null `ZipWithGatherer`
    @SafeVarargs
    public static <T extends @Nullable Object, S extends @Nullable Object> ZipWithGatherer<T, S, Pair<T, S>> zipWith(
            final S... other
    ) {
        mustNotBeNull(other, "Other must not be null");
        return zipWith(Arrays.stream(other), Pair::new);
    }

    /// Creates a stream of `List` objects which contain each two adjacent elements in the input stream.
    ///
    /// @param <T> Type of elements in the input stream
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object> Gatherer<T, ?, List<T>> zipWithNext() {
        return zipWithNext((item, next) -> Collections.unmodifiableList(Arrays.asList(item, next)));
    }

    /// Creates a stream of `R` objects which is assembled from the two adjacent elements in the input stream.
    ///
    /// @param <T>    Type of elements in the input stream
    /// @param <R>>   Type of elements in the output stream
    /// @param zipper A function that takes two adjacent elements from the input stream and returns a single element
    /// @return A non-null `Gatherer`
    public static <T extends @Nullable Object, R extends @Nullable Object> Gatherer<T, ?, R> zipWithNext(
            final BiFunction<? super T, ? super T, ? extends R> zipper
    ) {
        mustNotBeNull(zipper, "Zipper must not be null");
        class State {
            boolean first = true;
            @Nullable
            T previous = null;

            boolean zipNext(final T item, final Downstream<? super R> downstream) {
                if (first) {
                    first = false;
                } else {
                    downstream.push(zipper.apply(previous, item));
                }
                previous = item;
                return !downstream.isRejecting();
            }
        }
        return Gatherer.ofSequential(State::new, Integrator.<State, T, R>ofGreedy(State::zipNext));
    }

    private static <T extends @Nullable Object> ThrottlingGatherer<T> throttle(
            final ThrottlingGatherer.LimitRule limitRule,
            final int allowed,
            final Duration duration,
            final Clock clock
    ) {
        return new ThrottlingGatherer<>(limitRule, allowed, duration, clock);
    }
}
