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

import com.ginsberg.gatherers4j.util.CircularBuffer;
import org.jspecify.annotations.Nullable;

import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Gatherer;

public class LastGatherer<T extends @Nullable Object>
        implements Gatherer<T, LastGatherer.State<T>, T> {

    private final int lastCount;

    LastGatherer(final int lastCount) {
        if (lastCount < 0) {
            throw new IllegalArgumentException("Last count must not be negative");
        }
        this.lastCount = lastCount;
    }

    @Override
    public BiConsumer<State<T>, Downstream<? super T>> finisher() {
        return (state, downstream) -> {
            final var iterator = state.elements.iterator();
            while (iterator.hasNext() && !downstream.isRejecting()) {
                downstream.push(iterator.next());
            }
        };
    }

    @Override
    public Supplier<State<T>> initializer() {
        return () -> new State<>(lastCount);
    }

    @Override
    public Integrator<State<T>, T, T> integrator() {
        return Integrator.ofGreedy((state, element, downstream) -> {
            state.elements.add(element);
            return !downstream.isRejecting();
        });
    }

    public static class State<T extends @Nullable Object> {
        final CircularBuffer<T> elements;
        State(final int capacity) {
            elements = new CircularBuffer<>(capacity);
        }
    }
}
