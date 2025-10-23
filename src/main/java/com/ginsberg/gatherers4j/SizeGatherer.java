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

import com.ginsberg.gatherers4j.enums.Size;
import com.ginsberg.gatherers4j.util.GathererUtils;
import org.jspecify.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.ginsberg.gatherers4j.util.GathererUtils.*;

public final class SizeGatherer<T extends @Nullable Object> implements Gatherer4j.Stateful.WithFinisher<T, T> {

    private final long targetSize;
    private final Size operation;
    private final Supplier<Stream<T>> orElse;

    SizeGatherer(final Size operation, final long targetSize) {
        this(operation, targetSize, () -> {
            throw new IllegalStateException("Invalid stream size: wanted " + operation.name() + " " + targetSize);
        });
    }

    SizeGatherer(final Size operation, final long targetSize, final Supplier<Stream<T>> orElse) {
        require(targetSize >= 0, "Target size cannot be negative");
        this.operation = operation;
        this.targetSize = targetSize;
        this.orElse = mustNotBeNull(orElse, "The orElse function must not be null");

    }

    /// When the current stream does not have the correct length, call the given
    /// `Supplier<Stream<T>>` to produce an output instead of throwing an exception (the default behavior).
    ///
    /// Note: You will need a type witness when using this:
    ///
    /// `source.gather(Gatherers4j.<String>ensureSizeExactly(2).orElse(() -> Stream.of("A", "B")))`
    ///
    /// @param orElse - A non-null `Supplier`, the results of which will be used instead of the input stream.
    public SizeGatherer<T> orElse(final Supplier<Stream<T>> orElse) {
        return new SizeGatherer<>(this.operation, this.targetSize, orElse);
    }

    /// When the current stream does not have the correct length, produce an empty stream instead of throwing
    /// an exception (the default behavior).
    ///
    /// Note: You will need a type witness when using this:
    ///
    /// `source.gather(Gatherers4j.<String>ensureSizeExactly(2).orElseEmpty())`
    ///
    public SizeGatherer<T> orElseEmpty() {
        return orElse(Stream::empty);
    }

    @Override
    public Gatherer4j.State.WithFinisher<T, T> initialize() {
        return new Gatherer4j.State.WithFinisher<>() {
            boolean failed = false;
            final List<@Nullable T> elements = new ArrayList<>();

            @Override
            public boolean integrate(final T item, final Downstream<? super T> downstream) {
                if (operation.tryAccept(elements.size() + 1L, targetSize)) {
                    elements.add(item);
                } else {
                    failed = true;
                }
                return !failed || !downstream.isRejecting();
            }

            @Override
            public void finish(final Downstream<? super T> downstream) {
                if (!failed && operation.accept(elements.size(), targetSize)) {
                    GathererUtils.pushWhileNotRejecting(elements, downstream);
                } else {
                    pushWhileNotRejecting(orElse.get(), downstream);
                }
            }
        };
    }
}
