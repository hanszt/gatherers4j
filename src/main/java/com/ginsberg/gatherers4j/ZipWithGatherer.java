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

import org.jspecify.annotations.Nullable;

import java.util.Spliterator;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.ginsberg.gatherers4j.util.GathererUtils.mustNotBeNull;

public record ZipWithGatherer<T extends @Nullable Object, S extends @Nullable Object, R extends @Nullable Object>(
        Iterable<S> other, BiFunction<? super T, ? super S, ? extends R> mapper,
        @Nullable Function<? super S, ? extends T> sourceWhenArgumentLonger,
        @Nullable Function<? super T, ? extends S> argumentWhenSourceLonger)
        implements Gatherer4j.Stateful.WithFinisher<T, R> {

    public ZipWithGatherer {
        mustNotBeNull(other, "Other spliterator must not be null");
        mustNotBeNull(mapper, "Mapper must not be null");
    }

    /// When the argument `Iterable`, `Iterator` or `Stream` runs out of elements before the source stream does,
    /// use the result of the `function` provided for the remaining `S` elements of each `Pair`
    /// until the source is exhausted.
    ///
    /// Note: You may need a type witness when using this:
    ///
    /// `source.gather(Gatherers4j.<String, Integer>zipWith(right).argumentWhenSourceLonger(String::length))`
    ///
    /// @param mappingFunction A non-null function which takes a possibly null `<T>`
    ///                                                                      and emits a possibly null `<S>`
    public ZipWithGatherer<T, S, R> argumentWhenSourceLonger(final Function<? super T, ? extends S> mappingFunction) {
        mustNotBeNull(mappingFunction, "Mapping function must not be null, use nullArgumentWhenSourceLonger() to insert nulls");
        return new ZipWithGatherer<>(
                other,
                mapper,
                sourceWhenArgumentLonger,
                mappingFunction
        );
    }

    /// When the source stream runs out of elements before the argument `Iterable`, `Iterator` or `Stream` does,
    /// use the result of the `function` provided for the remaining `T` elements of each `Pair`
    /// until the argument is exhausted.
    ///
    /// Note: You may need a type witness when using this:
    ///
    /// `source.gather(Gatherers4j.<String, Integer>zipWith(right).sourceWhenArgumentLonger(String::valueOf))`
    ///
    /// @param mappingFunction A non-null function which takes a possibly null `<S>`
    ///                                                                      and emits a possibly null `<T>`
    public ZipWithGatherer<T, S, R> sourceWhenArgumentLonger(final Function<? super S, ? extends T> mappingFunction) {
        mustNotBeNull(mappingFunction, "Mapping function must not be null, use nullSourceWhenArgumentLonger() to insert nulls");
        return new ZipWithGatherer<>(
                other,
                mapper,
                mappingFunction,
                argumentWhenSourceLonger
        );
    }

    /// When the argument `Iterable`, `Iterator` or `Stream` runs out of elements before the source stream does,
    /// use `null` for the remaining `S` elements of each `Pair` until the source is exhausted.
    ///
    /// Note: You may need a type witness when using this:
    ///
    /// `source.gather(Gatherers4j.<String, Integer>zipWith(right).nullArgumentWhenSourceLonger())`
    ///
    public ZipWithGatherer<T, S, R> nullArgumentWhenSourceLonger() {
        return argumentWhenSourceLonger(_ -> null);
    }

    /// When the source stream runs out of elements before the argument `Iterable`, `Iterator` or `Stream` does,
    /// use `null` for the remaining `T` elements of each `Pair` until the argument is exhausted.
    ///
    /// Note: You may need a type witness when using this:
    ///
    /// `source.gather(Gatherers4j.<String, Integer>zipWith(right).nullSourceWhenArgumentLonger())`
    public ZipWithGatherer<T, S, R> nullSourceWhenArgumentLonger() {
        return sourceWhenArgumentLonger(_ -> null);
    }

    @Override
    public State.WithFinisher<T, R> initialize() {
        return new State.WithFinisher<>() {
            final Spliterator<S> otherSpliterator = other.spliterator();

            @Override
            public boolean integrate(final T item, final Downstream<? super R> downstream) {
                final var advanced = otherSpliterator.tryAdvance(it -> downstream.push(mapper.apply(item, it)));
                if (!advanced && argumentWhenSourceLonger != null) {
                    return downstream.push(mapper.apply(item, argumentWhenSourceLonger.apply(item)));
                }
                return advanced && !downstream.isRejecting();
            }

            @Override
            public void finish(final Downstream<? super R> downstream) {
                if (sourceWhenArgumentLonger != null) {
                    var downstreamIsRejecting = downstream.isRejecting();
                    while (!downstreamIsRejecting) {
                        downstreamIsRejecting = !otherSpliterator.tryAdvance(arg ->
                                downstream.push(mapper.apply(sourceWhenArgumentLonger.apply(arg), arg))
                        );
                    }
                }
            }
        };
    }
}
