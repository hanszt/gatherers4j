package com.ginsberg.gatherers4j;

import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Gatherer;

/// A Gatherer interface extension that provides a fluent api for building a gatherer.
///
/// @param <T> The type of the input elements
/// @param <A> The type of the State
/// @param <R> The type of the elements in the downstream
public sealed interface Gatherer4j<T, A, R> extends Gatherer<T, A, R> {

    static <T, R> Gatherer4j.Stateless<T, R> ofSequential(
            BiPredicate<T, Downstream<? super R>> integrator
    ) {
        return integrator::test;
    }

    static <T, R> Gatherer4j.Stateless<T, R> of(
            BiPredicate<T, Downstream<? super R>> integrator
    ) {
        return new Gatherer4j.Stateless<>() {

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

    boolean integrate(A state, T item, Downstream<? super R> downstream);

    @Override
    default Integrator<A, T, R> integrator() {
        return this::integrate;
    }

    @FunctionalInterface
    non-sealed interface Stateless<T, R> extends Gatherer4j<T, Void, R> {

        boolean integrate(T item, Downstream<? super R> downstream);

        @Override
        default boolean integrate(Void state, T item, Downstream<? super R> downstream) {
            return integrate(item, downstream);
        }
    }

    non-sealed interface Stateful<T, A, R> extends Gatherer4j<T, A, R> {

        A initialize();

        @Override
        default Supplier<A> initializer() {
            return this::initialize;
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

    sealed interface Greedy<T, A, R> extends Gatherer4j<T, A, R> {

        boolean greedyIntegrate(A state, T item, Downstream<? super R> downstream);

        @Override
        default boolean integrate(A state, T item, Downstream<? super R> downstream) {
            return greedyIntegrate(state, item, downstream);
        }

        @Override
        default Integrator<A, T, R> integrator() {
            return Integrator.<A, T, R>ofGreedy(this::greedyIntegrate);
        }

        @FunctionalInterface
        non-sealed interface Stateless<T, R> extends Greedy<T, Void, R> {

            default boolean greedyIntegrate(Void state, T item, Downstream<? super R> downstream) {
                return Greedy.super.integrate(state, item, downstream);
            }

            boolean greedyIntegrate(T item, Downstream<? super R> downstream);
        }

        non-sealed interface Stateful<T, A, R> extends Greedy<T, A, R>, Gatherer4j.Stateful<T, A, R> {
            interface WithFinisher<T, A, R> extends Greedy.Stateful<T, A, R>, Gatherer4j.Stateful.WithFinisher<T, A, R> {
                interface WithCombiner<T, A, R> extends Greedy.Stateful.WithFinisher<T, A, R>, Gatherer4j.Stateful.WithFinisher.WithCombiner<T, A, R> {
                }
            }
        }
    }
}
