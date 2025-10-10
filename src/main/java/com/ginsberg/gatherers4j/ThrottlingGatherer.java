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

import java.time.Duration;
import java.time.InstantSource;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Supplier;
import java.util.stream.Gatherer;

import static com.ginsberg.gatherers4j.util.GathererUtils.NANOS_PER_MILLIS;
import static com.ginsberg.gatherers4j.util.GathererUtils.mustNotBeNull;

public record ThrottlingGatherer<T extends @Nullable Object>(
        ThrottlingGatherer.LimitRule limitRule,
        int allowed,
        Duration duration,
        InstantSource instantSource
) implements Gatherer<T, ThrottlingGatherer.State, T> {

    enum LimitRule {
        Drop,
        Pause
    }

    public ThrottlingGatherer {
        mustNotBeNull(instantSource, "InstantSource must not be null");
        mustNotBeNull(duration, "Duration must not be null");
        mustNotBeNull(limitRule, "LimitRule must not be null");
        if (duration.toMillis() < 1) {
            throw new IllegalArgumentException("Minimum duration is 1ms");
        }
        if (allowed <= 0) {
            throw new IllegalArgumentException("Allowed must be positive");
        }
    }

    public ThrottlingGatherer<T> withInstantSource(final InstantSource instantSource) {
        return new ThrottlingGatherer<>(limitRule, allowed, duration, instantSource);
    }

    @Override
    public Supplier<State> initializer() {
        return () -> new State(limitRule, duration, allowed, instantSource);
    }

    @Override
    public Integrator<State, T, T> integrator() {
        return Integrator.ofGreedy((state, element, downstream) -> {
            if (!downstream.isRejecting() && state.attempt()) {
                downstream.push(element);
            }
            return !downstream.isRejecting();
        });
    }

    public static class State {
        final int allowedPerPeriod;
        final long periodDurationMillis;
        final LimitRule limitRule;
        final InstantSource instantSource;
        long thisPeriodEnd;
        int remainingPermits;

        State(final LimitRule limitRule, final Duration duration, final int allowed, final InstantSource instantSource) {
            this.limitRule = limitRule;
            this.allowedPerPeriod = allowed;
            this.periodDurationMillis = duration.toMillis();
            this.instantSource = instantSource;
            resetPeriod();
        }

        private void resetPeriod() {
            thisPeriodEnd = instantSource.millis() + periodDurationMillis;
            remainingPermits = allowedPerPeriod;
        }

        // Assuming this is not run in parallel. Gate with a lock if that assumption fails/changes.
        boolean attempt() {
            final var now = instantSource.millis();
            if(now < thisPeriodEnd) {
                // The current period has not ended
                if(remainingPermits == 0) {
                    if(limitRule == LimitRule.Drop) {
                        return false;
                    }
                    // Wait until next period, reset counters, fall through to take permit.
                    LockSupport.parkNanos((thisPeriodEnd - now) * NANOS_PER_MILLIS);
                    resetPeriod();
                }
            } else {
                // We're in a new period, reset the counters
                // and fall through to take permit.
                resetPeriod();
            }
            remainingPermits--;
            return true;
        }
    }
}
