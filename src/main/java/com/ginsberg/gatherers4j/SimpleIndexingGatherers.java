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
import org.jspecify.annotations.Nullable;

import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.stream.Gatherer;

import static com.ginsberg.gatherers4j.util.GathererUtils.mustNotBeNull;

public class SimpleIndexingGatherers {

    public static <T extends @Nullable Object> Gatherer<T, ?, T> filterIndexed(
            final BiPredicate<Integer, @Nullable T> predicate
    ) {
        mustNotBeNull(predicate, "Predicate must not be null");
        return Gatherer.ofSequential(
                State::new,
                Gatherer.Integrator.ofGreedy((state, element, downstream) -> {
                    if (predicate.test(state.index++, element)) {
                        downstream.push(element);
                    }
                    return !downstream.isRejecting();
                })
        );
    }

    public static <T extends @Nullable Object, R extends @Nullable Object> Gatherer<T, ?, R> mapIndexed(
            final BiFunction<Integer, @Nullable T, @Nullable R> mappingFunction
    ) {
        mustNotBeNull(mappingFunction, "mappingFunction must not be null");
        return Gatherer.ofSequential(
                State::new,
                Gatherer.Integrator.ofGreedy((state, element, downstream) ->
                        downstream.push(mappingFunction.apply(state.index++, element))
                )
        );
    }

    public static <T extends @Nullable Object> Gatherer<T, ?, T> peekIndexed(
            final BiConsumer<Integer, @Nullable T> peekingConsumer
    ) {
        mustNotBeNull(peekingConsumer, "peekingConsumer must not be null");
        return Gatherer.ofSequential(
                State::new,
                Gatherer.Integrator.ofGreedy((state, element, downstream) -> {
                    peekingConsumer.accept(state.index++, element);
                            return downstream.push(element);
                        }
                )
        );
    }

    public static <T extends @Nullable Object> Gatherer<T, ?, WithIndex<T>> withIndex() {
        return mapIndexed(WithIndex::new);
    }

    private static class State {
        int index;
    }
}
