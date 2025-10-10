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

import java.util.Collection;
import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Gatherer;

import static com.ginsberg.gatherers4j.util.GathererUtils.pushAll;

/// Note: "Single" in this case means at most one. The naming of this more precisely seemed clumsy.
class FlattenSingleOrFail<T extends Collection<R>, R>
        implements Gatherer<T, FlattenSingleOrFail.State<T>, R> {

    private final String message;

    FlattenSingleOrFail(final String message) {
        this.message = message;
    }

    @Override
    public Supplier<State<T>> initializer() {
        return State::new;
    }

    @Override
    public Integrator<State<T>, T, R> integrator() {
        return (state, element, downstream) -> {
            if (state.isFirst) {
                state.firstCollection = element;
                state.isFirst = false;
                return !downstream.isRejecting();
            } else {
                throw new IllegalStateException(message);
            }
        };
    }

    @Override
    public BiConsumer<State<T>, Downstream<? super R>> finisher() {
        return (inputState, downstream) -> {
            if(inputState.firstCollection != null) {
                pushAll(inputState.firstCollection, downstream);
            }
        };
    }

    public static class State<T> {
        boolean isFirst = true;
        @Nullable T firstCollection;
    }
}
