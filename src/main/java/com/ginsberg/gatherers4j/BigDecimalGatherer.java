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
import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.function.Function;
import java.util.stream.Gatherer;

import static com.ginsberg.gatherers4j.util.GathererUtils.mustNotBeNull;

public interface BigDecimalGatherer<T extends @Nullable Object>
        extends Gatherer<T, BigDecimalGatherer.State, BigDecimal> {

    @Nullable BigDecimal nullReplacement();
    Function<? super T, @Nullable BigDecimal> mappingFunction();
    MathContext mathContext();

    @Override
    default Integrator<BigDecimalGatherer.State, T, BigDecimal> integrator() {
        return Integrator.ofGreedy((state, element, downstream) -> {
            final var mappedElement = getMappedElement(element);
            if (mappedElement != null) {
                final var next = state.calculate(mappedElement, mathContext());
                if (state.shouldPush()) {
                    return downstream.push(next);
                }
            }
            return !downstream.isRejecting();
        });
    }

    private @Nullable BigDecimal getMappedElement(final T element) {
        if (element == null) {
            return nullReplacement();
        }
        final var mapped = mappingFunction().apply(element);
        return mapped == null ? nullReplacement() : mapped;
    }

    /// When encountering a `null` value in a stream, treat it as the given `replacement` value instead.
    ///
    /// @param replacement The value to replace `null` with
    default BigDecimalGatherer<T> treatNullAs(@Nullable final BigDecimal replacement) {
        return copy(replacement, mathContext());
    }

    /// When encountering a `null` value in a stream, treat it as `BigDecimal.ONE` instead.
    default BigDecimalGatherer<T> treatNullAsOne() {
        return treatNullAs(BigDecimal.ONE);
    }

    /// Replace the `MathContext` used for all mathematical operations in this class.
    ///
    /// @param mathContext A non-null `MathContext`
    default BigDecimalGatherer<T> withMathContext(final MathContext mathContext) {
        mustNotBeNull(mathContext, "MathContext must not be null");
        return copy(nullReplacement(), mathContext);
    }

    BigDecimalGatherer<T> copy(@Nullable final BigDecimal replacement, final MathContext mathContext);

    /// When encountering a `null` value in a stream, treat it as `BigDecimal.ZERO` instead.
    default BigDecimalGatherer<T> treatNullAsZero() {
        return treatNullAs(BigDecimal.ZERO);
    }

    /// Include the original input value from the stream in addition to the calculated average.
    default Gatherer<T, BigDecimalGatherer.State, WithOriginal<T, BigDecimal>> withOriginal() {
        return new WithOriginalGatherer<>(this);
    }

    interface State {
        BigDecimal calculate(final BigDecimal element, final MathContext mathContext);

        default boolean shouldPush() {
            return true;
        }
    }
}
