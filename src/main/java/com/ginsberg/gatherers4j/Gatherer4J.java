package com.ginsberg.gatherers4j;

import org.jspecify.annotations.Nullable;

import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Gatherer;

@FunctionalInterface
public interface Gatherer4J<T, A, R> extends Gatherer<T, A, R> {

    @Nullable
    default A supply() {
        return null;
    }

    boolean integrate(A state, T item, Downstream<? super R> downstream);

    default A combine(A a1, A a2) {
        throw new UnsupportedOperationException("This combiner cannot be used!");
    }

    default void finish(A state, Downstream<? super R> downstream) {

    }

    @Override
    default Supplier<@Nullable A> initializer() {
        return this::supply;
    }

    @Override
    default Integrator<A, T, R> integrator() {
        return this::integrate;
    }

    @Override
    default BinaryOperator<A> combiner() {
        return this::combine;
    }

    @Override
    default BiConsumer<A, Downstream<? super R>> finisher() {
        return this::finish;
    }

    @FunctionalInterface
    interface OfGreedy<T, A, R> extends Gatherer4J<T, A, R> {

        @Override
        default Integrator<A, T, R> integrator() {
            return Integrator.<A, T, R>ofGreedy(this::integrate);
        }
    }
}
