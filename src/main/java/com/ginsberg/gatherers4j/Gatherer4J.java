package com.ginsberg.gatherers4j;

import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Gatherer;

/// A Gatherer interface extension that provides a fluent api for building a gatherer.
/// @param <T> The type of the input elements
/// @param <A> The type of the State
/// @param <R> The type of the elements in the downstream
public sealed interface Gatherer4J<T, A, R> extends Gatherer<T, A, R> {

    static <T, R> Gatherer4J.Stateless<T, R> ofSequential(
            BiPredicate<T, Downstream<? super R>> integrator
    ) {
        return integrator::test;
    }

    static <T, R> Gatherer4J.Stateless<T, R> of(
            BiPredicate<T, Downstream<? super R>> integrator
    ) {
        return new Gatherer4J.Stateless<>() {

            @Override
            public boolean integrate(final T item, final Downstream<? super R> downstream) {
                return integrator.test(item, downstream);
            }

            @Override
            public BinaryOperator<Void> combiner() {
                //noinspection DataFlowIssue
                return (_, _) -> null;
            }
        };
    }

    IntegrationMode integrationMode();

    boolean integrate(A state, T item, Downstream<? super R> downstream);

    @Override
    default Integrator<A, T, R> integrator() {
        return switch (integrationMode()) {
            case DEFAULT -> this::integrate;
            case GREEDY -> Integrator.<A, T, R>ofGreedy(this::integrate);
        };
    }

    @FunctionalInterface
    non-sealed interface Stateless<T, R> extends Gatherer4J<T, Void, R> {

        boolean integrate(T item, Downstream<? super R> downstream);

        @Override
        default boolean integrate(Void state, T item, Downstream<? super R> downstream) {
            return integrate(item, downstream);
        }

        default IntegrationMode integrationMode() {
            return IntegrationMode.GREEDY;
        }
    }

    non-sealed interface Stateful<T, A, R> extends Gatherer4J<T, A, R> {

        A initialize();

        @Override
        default Supplier<A> initializer() {
            return this::initialize;
        }

        @Override
        default IntegrationMode integrationMode() {
            return IntegrationMode.DEFAULT;
        }

        interface WithFinisher<T, A, R> extends Stateful<T, A, R> {

            void finish(A state, Downstream<? super R> downstream);

            @Override
            default BiConsumer<A, Downstream<? super R>> finisher() {
                return this::finish;
            }

            interface WithCombiner<T, A, R> extends WithFinisher<T, A, R> {

                A combine(final A state1, final A state2);

                @Override
                default BinaryOperator<A> combiner() {
                    return this::combine;
                }
            }
        }
    }

    enum IntegrationMode {
        GREEDY, DEFAULT
    }
}
