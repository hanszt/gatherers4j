package com.ginsberg.gatherers4j;

import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Gatherer;

/// A Gatherer interface extension that provides a fluent api for building a gatherer. Especially useful when a direct implementation or interface extension is desirable.
///
/// @param <T> The type of the input elements
/// @param <A> The type of the State
/// @param <R> The type of the elements in the downstream
public sealed interface Gatherer4j2<T, A, R> extends Gatherer<T, A, R> {

    boolean integrate(A state, T item, Downstream<? super R> downstream);

    @Override
    default Integrator<A, T, R> integrator() {
        return this::integrate;
    }

    @FunctionalInterface
    non-sealed interface Stateless<T, R> extends Gatherer4j2<T, Void, R> {

        boolean integrate(T item, Downstream<? super R> downstream);

        @Override
        default boolean integrate(Void state, T item, Downstream<? super R> downstream) {
            return integrate(item, downstream);
        }
    }

    @FunctionalInterface
    non-sealed interface Stateful<T, A extends Stateful.State<T, R>, R> extends Gatherer4j2<T, A, R> {

        A initialize();

        @Override
        default Supplier<A> initializer() {
            return this::initialize;
        }

        @Override
        default boolean integrate(A state, T item, Downstream<? super R> downstream) {
            return state.integrate(item, downstream);
        }

        interface WithFinisher<T, A extends WithFinisher.State<T, R>, R> extends Stateful<T, A, R> {

            @Override
            default BiConsumer<A, Downstream<? super R>> finisher() {
                return State::finish;
            }

            interface WithCombiner<T, A extends WithFinisher.WithCombiner.State<T, R>, R> extends WithFinisher<T, A, R> {

                @Override
                default BinaryOperator<A> combiner() {
                    //noinspection unchecked
                    return (state1, state2) -> (A) state1.combine(state2);
                }

                interface State<T, R> extends Stateful.WithFinisher.State<T, R> {

                    State<T, R> combine(State<T, R> other);
                }
            }

            interface State<T, R> extends Stateful.State<T, R> {

                void finish(Downstream<? super R> downstream);
            }
        }

        interface State<T, R> {

            boolean integrate(T item, Downstream<? super R> downstream);
        }
    }

    sealed interface Greedy<T, A, R> extends Gatherer4j2<T, A, R> {

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

        @FunctionalInterface
        non-sealed interface Stateful<T, A extends Gatherer4j2.Stateful.State<T, R>, R> extends Greedy<T, A, R>, Gatherer4j2.Stateful<T, A, R> {

            @Override
            default boolean greedyIntegrate(A state, T item, Downstream<? super R> downstream) {
                return state.integrate(item, downstream);
            }

            @Override
            default boolean integrate(A state, T item, Downstream<? super R> downstream) {
                return greedyIntegrate(state, item, downstream);
            }

            interface WithFinisher<T, A extends Gatherer4j2.Stateful.WithFinisher.State<T, R>, R> extends Greedy.Stateful<T, A, R>, Gatherer4j2.Stateful.WithFinisher<T, A, R> {
                interface WithCombiner<T, A extends
                        Gatherer4j2.Stateful.WithFinisher.WithCombiner.State<T, R>, R> extends Greedy.Stateful.WithFinisher<T, A, R>,
                        Gatherer4j2.Stateful.WithFinisher.WithCombiner<T, A, R> {
                }
            }
        }
    }
}
