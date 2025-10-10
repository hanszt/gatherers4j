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

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.random.RandomGenerator;
import java.util.stream.Gatherer;

import static com.ginsberg.gatherers4j.util.GathererUtils.mustNotBeNull;

public class ShufflingGatherer<T extends @Nullable Object> implements
        Gatherer<T, ShufflingGatherer.State<T>, T> {

    private final RandomGenerator randomGenerator;

    ShufflingGatherer(final RandomGenerator randomGenerator) {
        this.randomGenerator = mustNotBeNull(randomGenerator, "RandomGenerator must not be null");
    }

    @Override
    public Supplier<ShufflingGatherer.State<T>> initializer() {
        return State::new;
    }

    @Override
    public Integrator<ShufflingGatherer.State<T>, T, T> integrator() {
        return Integrator.ofGreedy((state, element, downstream) -> {
            state.inputs.add(element);
            return !downstream.isRejecting();
        });
    }

    @Override
    public BiConsumer<ShufflingGatherer.State<T>, Downstream<? super T>> finisher() {
        return (state, downstream) -> {
            while (!state.inputs.isEmpty() && !downstream.isRejecting()) {
                final var randomSlot = randomGenerator.nextInt(state.inputs.size());
                final var last = state.inputs.removeLast();
                if (randomSlot < state.inputs.size()) {
                    downstream.push(state.inputs.set(randomSlot, last));
                } else {
                    downstream.push(last);
                }
            }
        };
    }

    public static class State<T> {
        final List<T> inputs = new ArrayList<>();
    }
}
