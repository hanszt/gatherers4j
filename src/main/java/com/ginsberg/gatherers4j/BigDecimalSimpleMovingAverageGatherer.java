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

record BigDecimalSimpleMovingAverageGatherer<T extends @Nullable Object>(
        int windowSize,
        boolean includePartialValues,
        Function<T, @Nullable BigDecimal> mappingFunction,
        @Nullable BigDecimal nullReplacement,
        MathContext mathContext
) implements BigDecimalMovingGatherer<T> {

    public BigDecimalSimpleMovingAverageGatherer {
        require(!(windowSize <= 1), "Window size must be greater than 1");
    }

    @Override
    public Supplier<BigDecimalGatherer.State> initializer() {
        return () -> new State(windowSize, includePartialValues);
    }

    @Override
    public BigDecimalMovingGatherer<T> copy(final @Nullable BigDecimal replacement, final MathContext mathContext, final boolean includePartialValues) {
        return new BigDecimalSimpleMovingAverageGatherer<>(windowSize, includePartialValues, mappingFunction, replacement, mathContext);
    }

    static class State implements BigDecimalGatherer.State {
        final boolean includePartialValues;
        final BigDecimal[] series;
        BigDecimal sum = BigDecimal.ZERO;
        BigDecimal count = BigDecimal.ZERO;
        int index = 0;

        private State(final int lookBack, final boolean includePartialValues) {
            this.includePartialValues = includePartialValues;
            this.series = new BigDecimal[lookBack];
            Arrays.fill(series, BigDecimal.ZERO);
        }

        @Override
        public boolean shouldPush() {
            return includePartialValues || count.intValue() >= series.length;
        }

        @Override
        public BigDecimal calculate(final BigDecimal element, final MathContext mathContext) {
            sum = sum.subtract(series[index]).add(element, mathContext);
            series[index % series.length] = element;
            index = (index + 1) % series.length;
            if (count.intValue() < series.length) {
                count = count.add(BigDecimal.ONE);
            }
            return sum.divide(count, mathContext);
        }
    }
}
