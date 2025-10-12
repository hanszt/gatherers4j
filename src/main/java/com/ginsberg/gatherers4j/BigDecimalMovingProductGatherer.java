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
import java.util.Arrays;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.ginsberg.gatherers4j.util.GathererUtils.require;

record BigDecimalMovingProductGatherer<T extends @Nullable Object>(
        int windowSize,
        boolean includePartialValues,
        Function<T, @Nullable BigDecimal> mappingFunction,
        @Nullable BigDecimal nullReplacement,
        MathContext mathContext
) implements BigDecimalMovingGatherer<T> {

    BigDecimalMovingProductGatherer {
        require(windowSize > 1, "Window size must be greater than 1");
    }

    @Override
    public Supplier<BigDecimalGatherer.State> initializer() {
        return () -> new BigDecimalMovingProductGatherer.State(windowSize, includePartialValues);
    }

    @Override
    public BigDecimalMovingGatherer<T> copy(
            final @Nullable BigDecimal replacement,
            final MathContext mathContext,
            final boolean includePartialValues
    ) {
        return new BigDecimalMovingProductGatherer<>(windowSize, includePartialValues, mappingFunction, replacement, mathContext);
    }

    static class State extends BigDecimalMovingGatherer.State {
        BigDecimal product = BigDecimal.ONE;

        private State(final int lookBack, final boolean includePartialValues) {
            super(includePartialValues, new BigDecimal[lookBack]);
            Arrays.fill(series, BigDecimal.ONE);
        }

        @Override
        public BigDecimal calculate(final BigDecimal element, final MathContext mathContext) {
            product = product.divide(series[index % series.length], mathContext).multiply(element, mathContext);
            series[index % series.length] = element;
            index++;
            return product;
        }
    }
}
