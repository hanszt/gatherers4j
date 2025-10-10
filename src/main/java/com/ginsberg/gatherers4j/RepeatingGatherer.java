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

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Gatherer;

import static com.ginsberg.gatherers4j.util.GathererUtils.pushAll;

public class RepeatingGatherer<T extends @Nullable Object>
        implements Gatherer<T, RepeatingGatherer.State<T>, T> {

    private static final int INFINITE = -1;
    private final int repeats;

    public static <T> RepeatingGatherer<T> ofInfinite() {
        return new RepeatingGatherer<>(INFINITE);
    }

    public static <T> RepeatingGatherer<T> ofFinite(final int repeats) {
        if (repeats < 0) {
            throw new IllegalArgumentException("Number of repeats must not be negative");
        }
        return new RepeatingGatherer<>(repeats);
    }

    private RepeatingGatherer(final int repeats) {
        this.repeats = repeats;
    }

    @Override
    public Supplier<RepeatingGatherer.State<T>> initializer() {
        return () -> new State<>(repeats);
    }

    @Override
    public Integrator<RepeatingGatherer.State<T>, T, T> integrator() {
        return Integrator.ofGreedy((state, element, downstream) -> {
            state.theStream.add(element);
            return repeats != 0 && !downstream.isRejecting();
        });
    }

    @Override
    public BiConsumer<RepeatingGatherer.State<T>, Downstream<? super T>> finisher() {
        return (inputState, downstream) -> {
            while (!downstream.isRejecting() && (inputState.repeatsRemaining == INFINITE || inputState.repeatsRemaining > 0)) {

                pushAll(inputState.theStream, downstream);
                if (inputState.repeatsRemaining != INFINITE) {
                    inputState.repeatsRemaining--;
                }
            }
        };
    }

    public static class State<T> {
        int repeatsRemaining;
        final List<T> theStream = new ArrayList<>();

        State(final int repeatsRemaining) {
            this.repeatsRemaining = repeatsRemaining;
        }
    }
}
