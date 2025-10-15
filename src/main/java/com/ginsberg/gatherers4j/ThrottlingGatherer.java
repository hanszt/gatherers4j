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

import static com.ginsberg.gatherers4j.util.GathererUtils.*;

public record ThrottlingGatherer<T extends @Nullable Object>(
        ThrottlingGatherer.LimitRule limitRule,
        int allowedPerPeriod,
        Duration duration,
        InstantSource instantSource
) implements Gatherer4j2.Greedy.Stateful<T, ThrottlingGatherer<T>.State, T> {

    enum LimitRule {
        Drop,
        Pause
    }

    public ThrottlingGatherer {
        mustNotBeNull(instantSource, "InstantSource must not be null");
        mustNotBeNull(duration, "Duration must not be null");
        mustNotBeNull(limitRule, "LimitRule must not be null");
        require(duration.toMillis() >= 1, "Minimum duration is 1ms");
        require(allowedPerPeriod > 0, "Allowed must be positive");
    }

    public ThrottlingGatherer<T> withInstantSource(final InstantSource instantSource) {
        return new ThrottlingGatherer<>(limitRule, allowedPerPeriod, duration, instantSource);
    }

    @Override
    public State initialize() {
        return new State();
    }

    public final class State implements Gatherer4j2.Stateful.State<T, T> {
        final long periodDurationMillis = duration.toMillis();
        long thisPeriodEnd;
        int remainingPermits;

        State() {
            resetPeriod();
        }

        public boolean integrate(T element, Downstream<? super T> downstream) {
            if (!downstream.isRejecting() && attempt()) {
                downstream.push(element);
            }
            return !downstream.isRejecting();
        }

        private void resetPeriod() {
            thisPeriodEnd = instantSource.millis() + periodDurationMillis;
            remainingPermits = allowedPerPeriod;
        }

        // Assuming this is not run in parallel. Gate with a lock if that assumption fails/changes.
        boolean attempt() {
            final var now = instantSource.millis();
            if (now < thisPeriodEnd) {
                // The current period has not ended
                if (remainingPermits == 0) {
                    if (limitRule == LimitRule.Drop) {
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
