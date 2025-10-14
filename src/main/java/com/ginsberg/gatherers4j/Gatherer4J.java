package com.ginsberg.gatherers4j;

import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Gatherer;

public interface Gatherer4J<T, A, R> extends Gatherer<T, A, R> {

    default IntegrationMode integrationMode() {
        return IntegrationMode.DEFAULT;
    }

    boolean integrate(A state, T item, Downstream<? super R> downstream);

    @Override
    default Integrator<A, T, R> integrator() {
        return switch (integrationMode()) {
            case DEFAULT -> this::integrate;
            case GREEDY -> Integrator.<A, T, R>ofGreedy(this::integrate);
        };
    }

    interface WithFinisher<T, A, R> extends Gatherer4J<T, A, R> {

        void finish(A state, Downstream<? super R> downstream);

        @Override
        default BiConsumer<A, Downstream<? super R>> finisher() {
            return this::finish;
        }
    }

    interface StatefulWithFinisher<T, A, R> extends Stateful<T, A, R> {

        void finish(A state, Downstream<? super R> downstream);

        @Override
        default BiConsumer<A, Downstream<? super R>> finisher() {
            return this::finish;
        }
    }

    interface Stateful<T, A, R> extends Gatherer4J<T, A, R> {

        A initialize();

        @Override
        default Supplier<A> initializer() {
            return this::initialize;
        }
    }

    enum IntegrationMode {
        GREEDY, DEFAULT
    }
}
