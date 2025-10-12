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

import com.ginsberg.gatherers4j.dto.WithOriginal;
import com.ginsberg.gatherers4j.util.CircularBuffer;
import org.jspecify.annotations.Nullable;

import java.util.function.Supplier;
import java.util.stream.Gatherer;

record WithOriginalGatherer<T extends @Nullable Object, A, R extends @Nullable Object>(Gatherer<T, A, R> delegate)
        implements Gatherer<T, A, WithOriginal<T, R>> {

    @Override
    public Supplier<A> initializer() {
        return delegate.initializer();
    }

    @Override
    public Integrator<A, T, WithOriginal<T, R>> integrator() {
        final var buffer = new CircularBuffer<R>(1);
        final var delegateIntegrator = delegate.integrator();

        return (state, element, downstream) -> {
            final var response = delegateIntegrator.integrate(state, element, buffer::add);
            if (!buffer.isEmpty()) {
                downstream.push(new WithOriginal<>(element, buffer.removeFirst()));
            }
            return response;
        };
    }
}
