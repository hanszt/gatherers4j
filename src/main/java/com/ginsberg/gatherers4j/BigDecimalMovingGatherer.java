package com.ginsberg.gatherers4j;

import org.jspecify.annotations.Nullable;

import java.math.BigDecimal;
import java.math.MathContext;

public interface BigDecimalMovingGatherer<T> extends BigDecimalGatherer<T> {

    boolean includePartialValues();

    /// When creating a moving sum and the full size of the window has not yet been reached, the
    /// gatherer should emit the sum of what it has.
    ///
    /// For example, if the trailing sum is over 10 values, but the stream has only emitted two
    /// values, the gatherer should calculate the two values and emit the answer. The default is to not
    /// emit anything until the full size of the window has been seen.
    default BigDecimalMovingGatherer<T> withIncludedPartialValues() {
        return copy(nullReplacement(), mathContext(), true);
    }

    BigDecimalMovingGatherer<T> copy(
            @Nullable final BigDecimal replacement,
            final MathContext mathContext,
            final boolean includePartialValues
    );

    @Override
    default BigDecimalMovingGatherer<T> copy(final @Nullable BigDecimal replacement, final MathContext mathContext) {
        return copy(replacement, mathContext, includePartialValues());
    }

    abstract class State implements BigDecimalGatherer.State {
        final boolean includePartialValues;
        final BigDecimal[] series;
        int index = 0;

        protected State(final boolean includePartialValues, final BigDecimal[] series) {
            this.includePartialValues = includePartialValues;
            this.series = series;
        }

        public boolean shouldPush() {
            return includePartialValues || index >= series.length;
        }
    }
}
