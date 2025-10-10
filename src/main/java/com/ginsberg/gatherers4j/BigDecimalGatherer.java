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

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.function.Function;
import java.util.stream.Gatherer;

import static com.ginsberg.gatherers4j.util.GathererUtils.mustNotBeNull;

abstract public class BigDecimalGatherer<T extends @Nullable Object>
        implements Gatherer<T, BigDecimalGatherer.State, BigDecimal> {
    private final Function<T, @Nullable BigDecimal> mappingFunction;
    private MathContext mathContext = MathContext.DECIMAL64;
    private @Nullable BigDecimal nullReplacement;

    BigDecimalGatherer(final Function<T, @Nullable BigDecimal> mappingFunction) {
        this.mappingFunction = mustNotBeNull(mappingFunction, "Mapping function must not be null");
    }

    @Override
    public Integrator<BigDecimalGatherer.State, T, BigDecimal> integrator() {
        return Integrator.ofGreedy((state, element, downstream) -> {
            final var mappedElement = getMappedElement(element);
            if (mappedElement != null) {
                state.update(mappedElement, mathContext);
                if (state.canCalculate()) {
                    return downstream.push(state.calculate());
                }
            }
            return !downstream.isRejecting();
        });
    }

    private @Nullable BigDecimal getMappedElement(final T element) {
        if(element == null) {
            return nullReplacement;
        }
        final var mapped = mappingFunction.apply(element);
        return mapped == null ? nullReplacement : mapped;
    }

    /// When encountering a `null` value in a stream, treat it as `BigDecimal.ZERO` instead.
    public BigDecimalGatherer<T> treatNullAsZero() {
        return treatNullAs(BigDecimal.ZERO);
    }

    /// When encountering a `null` value in a stream, treat it as the given `replacement` value instead.
    ///
    /// @param replacement The value to replace `null` with
    public BigDecimalGatherer<T> treatNullAs(@Nullable final BigDecimal replacement) {
        this.nullReplacement = replacement;
        return this;
    }

    /// Replace the `MathContext` used for all mathematical operations in this class.
    ///
    /// @param mathContext A non-null `MathContext`
    public BigDecimalGatherer<T> withMathContext(final MathContext mathContext) {
        this.mathContext = mustNotBeNull(mathContext, "MathContext must not be null");
        return this;
    }

    /// Include the original input value from the stream in addition to the calculated average.
    public WithOriginalGatherer<T, BigDecimalGatherer.State, BigDecimal> withOriginal() {
        return new WithOriginalGatherer<>(this);
    }

    public interface State {
        void update(final BigDecimal element, final MathContext mathContext);

        default boolean canCalculate() {
            return true;
        }

        BigDecimal calculate();
    }

}