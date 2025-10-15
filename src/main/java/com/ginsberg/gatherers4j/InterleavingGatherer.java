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

public final class InterleavingGatherer<T extends @Nullable Object>
        implements Gatherer4j2.Stateful.WithFinisher<T, InterleavingGatherer<T>.State, T> {

    private final Iterable<T> other;
    private final boolean appendArgumentIfLonger;
    private final boolean appendSourceIfLonger;

    InterleavingGatherer(
            final Iterable<T> other,
            final boolean appendArgumentIfLonger,
            final boolean appendSourceIfLonger
    ) {
        this.other = other;
        this.appendArgumentIfLonger = appendArgumentIfLonger;
        this.appendSourceIfLonger = appendSourceIfLonger;
    }

    /// If the source stream and the argument stream/iterator/iterable/varargs provide a different
    /// number of elements, append all the remaining elements from either one to the output stream.
    public InterleavingGatherer<T> appendLonger() {
        return new InterleavingGatherer<>(other, true, true);
    }

    /// If the argument stream/iterator/iterable/varargs provides more elements than the source stream,
    /// append all remaining elements from the argument stream/iterator/iterable/varargs to the output stream.
    public InterleavingGatherer<T> appendArgumentIfLonger() {
        return new InterleavingGatherer<>(other, true, false);
    }

    /// If the source stream provides more elements than the argument stream/iterator/iterable/varargs,
    /// append all the remaining elements to the output stream.
    public InterleavingGatherer<T> appendSourceIfLonger() {
        return new InterleavingGatherer<>(other, false, true);
    }

    @Override
    public State initialize() {
        return new State();
    }

    public final class State implements WithFinisher.State<T, T> {
        private final Spliterator<T> otherSpliterator = other.spliterator();

        @Override
        public boolean integrate(final T item, final Downstream<? super T> downstream) {
            downstream.push(item);
            if (appendSourceIfLonger) {
                otherSpliterator.tryAdvance(downstream::push);
                return !downstream.isRejecting();
            } else {
                // End immediately if we are not appending source if it is longer and other is finished
                return otherSpliterator.tryAdvance(downstream::push) && !downstream.isRejecting();
            }
        }

        @Override
        public void finish(final Downstream<? super T> downstream) {
            var downstreamRejecting = downstream.isRejecting();
            while (appendArgumentIfLonger && !downstreamRejecting) {
                downstreamRejecting = !otherSpliterator.tryAdvance(downstream::push);
            }
        }
    }
}
