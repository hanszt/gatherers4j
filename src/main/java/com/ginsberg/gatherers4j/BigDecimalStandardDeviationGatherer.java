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
import java.util.function.Supplier;

import static com.ginsberg.gatherers4j.util.GathererUtils.mustNotBeNull;

record BigDecimalStandardDeviationGatherer<T extends @Nullable Object>(
        Mode mode,
        Function<? super T, ? extends @Nullable BigDecimal> mappingFunction,
        @Nullable BigDecimal nullReplacement,
        MathContext mathContext
) implements BigDecimalGatherer<T> {

    @Override
    public BigDecimalGatherer<T> copy(final @Nullable BigDecimal replacement, final MathContext mathContext) {
        return new BigDecimalStandardDeviationGatherer<>(mode, mappingFunction, replacement, mathContext);
    }

    enum Mode {
        Population,
        Sample
    }

    BigDecimalStandardDeviationGatherer {
        mustNotBeNull(mappingFunction, "Mapping function must not be null");
        mustNotBeNull(mode, "Mode must not be null");
    }

    @Override
    public Supplier<BigDecimalGatherer.State> initializer() {
        return State::new;
    }

    class State extends BigDecimalSimpleAverageGatherer.State {
        private BigDecimal dSquared = BigDecimal.ZERO;
        private BigDecimal stdDev = BigDecimal.ZERO;

        @Override
        public BigDecimal calculate(final BigDecimal element, final MathContext mathContext) {
            final var previousAverage = average;
            super.calculate(element, mathContext);
            dSquared = dSquared.add( element.subtract(average).multiply( element.subtract(previousAverage)));
            if (mode == Mode.Sample) {
                if (count > 1) {
                    stdDev = dSquared
                            .divide(BigDecimal.valueOf(count - 1), mathContext)
                            .sqrt(mathContext);
                }
            } else {
                stdDev = dSquared
                        .divide(BigDecimal.valueOf(count), mathContext)
                        .sqrt(mathContext);
            }
            return stdDev;
        }
    }
}
