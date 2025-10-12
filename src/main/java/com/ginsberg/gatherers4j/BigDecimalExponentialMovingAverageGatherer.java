package com.ginsberg.gatherers4j;

import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.ginsberg.gatherers4j.util.GathererUtils.require;

public record BigDecimalExponentialMovingAverageGatherer<T extends @Nullable Object>(
        double alpha,
        Function<T, @Nullable BigDecimal> mappingFunction,
        @Nullable BigDecimal nullReplacement,
        MathContext mathContext
) implements BigDecimalGatherer<T> {

    public static <T extends @Nullable Object> BigDecimalExponentialMovingAverageGatherer<T> withAlpha(
            final double alpha,
            final Function<T, @Nullable BigDecimal> mappingFunction
    ) {
        return new BigDecimalExponentialMovingAverageGatherer<>(alpha, mappingFunction, null, MathContext.DECIMAL64);
    }

    public static <T extends @Nullable Object> BigDecimalExponentialMovingAverageGatherer<T> withPeriod(
            final int periods,
            final Function<T, @Nullable BigDecimal> mappingFunction
    ) {
        require(periods > 1, "periods must be greater than 1");
        final var alpha = 2.0 / (((long) periods) + 1);
        return withAlpha(alpha, mappingFunction);
    }

    public BigDecimalExponentialMovingAverageGatherer {
        require(alpha > 0 && alpha < 1.0, "alpha must be between 0.0 and 1.0, exclusive, got " + alpha);
    }

    @Override
    public Supplier<BigDecimalGatherer.State> initializer() {
        return () -> new BigDecimalExponentialMovingAverageGatherer.State(alpha);
    }

    @Override
    public BigDecimalGatherer<T> copy(final @Nullable BigDecimal replacement, final MathContext mathContext) {
        return new BigDecimalExponentialMovingAverageGatherer<>(alpha, mappingFunction, replacement, mathContext);
    }

    static final class State implements BigDecimalGatherer.State {
        final BigDecimal alpha;
        final BigDecimal oneMinusAlpha;
        boolean first = true;
        BigDecimal ema = BigDecimal.ZERO;

        State(final double alpha) {
            this.alpha = BigDecimal.valueOf(alpha);
            this.oneMinusAlpha = BigDecimal.ONE.subtract(this.alpha);
        }

        @Override
        public BigDecimal calculate(final BigDecimal element, final MathContext mathContext) {
            if (first) {
                first = false;
                ema = element;
            } else {
                ema = element.multiply(alpha).add(ema.multiply(oneMinusAlpha));
            }
            return ema;
        }
    }
}
