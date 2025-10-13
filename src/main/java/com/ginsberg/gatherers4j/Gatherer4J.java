package com.ginsberg.gatherers4j;

import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Gatherer;

public abstract class Gatherer4J<T, A, R> implements Gatherer<T, A, R> {

    private final IntegrationMode integrationMode;

    protected Gatherer4J(final IntegrationMode integrationMode) {
        this.integrationMode = integrationMode;
    }

    public abstract boolean integrate(A state, T item, Downstream<? super R> downstream);

    @Override
    public Integrator<A, T, R> integrator() {
        return switch (integrationMode) {
            case DEFAULT -> this::integrate;
            case GREEDY -> Integrator.<A, T, R>ofGreedy(this::integrate);
        };
    }

    public abstract static class WithFinisher<T, A, R> extends Gatherer4J<T, A, R> {

        public WithFinisher(final IntegrationMode integrationMode) {
            super(integrationMode);
        }

        public abstract void finish(A state, Downstream<? super R> downstream);

        @Override
        public BiConsumer<A, Downstream<? super R>> finisher() {
            return this::finish;
        }
    }

    public abstract static class StatefulWithFinisher<T, A, R> extends Stateful<T, A, R> {

        public StatefulWithFinisher(final IntegrationMode integrationMode, final Supplier<A> initializer) {
            super(integrationMode, initializer);
        }

        public abstract void finish(A state, Downstream<? super R> downstream);

        @Override
        public BiConsumer<A, Downstream<? super R>> finisher() {
            return this::finish;
        }
    }

    public abstract static class Stateful<T, A, R> extends Gatherer4J<T, A, R> {

        private final Supplier<A> initializer;

        public Stateful(final IntegrationMode integrationMode, final Supplier<A> initializer) {
            super(integrationMode);
            this.initializer = initializer;
        }

        public A initialize() {
            return this.initializer.get();
        }

        @Override
        public Supplier<A> initializer() {
            return this::initialize;
        }
    }

    public enum IntegrationMode {
        GREEDY, DEFAULT
    }
}
