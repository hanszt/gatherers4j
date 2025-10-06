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

import org.jspecify.annotations.Nullable;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Gatherer;

import static com.ginsberg.gatherers4j.util.GathererUtils.pushAll;

record UniquelyOccurringGatherer<INPUT extends @Nullable Object, SELECTED extends @Nullable Object>(
        Function<? super INPUT, ? extends SELECTED> selector
) implements Gatherer<INPUT, UniquelyOccurringGatherer.State<INPUT, SELECTED>, INPUT> {

    @Override
    public Supplier<State<INPUT, SELECTED>> initializer() {
        return State::new;
    }

    @Override
    public Integrator<State<INPUT, SELECTED>, INPUT, INPUT> integrator() {
        return Integrator.ofGreedy((state, element, downstream) -> {
            final var selected = selector.apply(element);
            if (!state.duplicates.contains(selected)) {
                if (state.found.containsKey(selected)) {
                    state.duplicates.add(selected);
                    state.found.remove(selected);
                } else {
                    state.found.put(selected, element);
                }
            }
            return !downstream.isRejecting();
        });
    }

    @Override
    public BinaryOperator<State<INPUT, SELECTED>> combiner() {
        return (left, right) -> {
            for (final var element : right.duplicates) {
                left.duplicates.add(element);
                left.found.remove(element);
            }

            for (final var e : right.found.entrySet()) {
                final var selected = e.getKey();
                if (!left.duplicates.contains(selected)) {
                    if (left.found.containsKey(selected)) {
                        left.found.remove(selected);
                        left.duplicates.add(selected);
                    } else {
                        left.found.put(selected, e.getValue());
                    }
                }
            }
            return left;
        };
    }

    @Override
    public BiConsumer<State<INPUT, SELECTED>, Downstream<? super INPUT>> finisher() {
        return (inputState, downstream) -> pushAll(inputState.found.values(), downstream);
    }

    public static class State<INPUT extends @Nullable Object, SELECTED extends @Nullable Object> {
        final Set<SELECTED> duplicates = new HashSet<>();
        final Map<SELECTED, INPUT> found = new LinkedHashMap<>();
    }
}
