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

    default IntegrationMode integrationMode() {
        return IntegrationMode.DEFAULT;
    }

    @Override
    default Integrator<A, T, R> integrator() {
        return switch (integrationMode()) {
            case DEFAULT -> this::integrate;
            case GREEDY -> Integrator.<A, T, R>ofGreedy(this::integrate);
        };
    }

    @FunctionalInterface
    non-sealed interface Stateless<T, R> extends Gatherer4j2<T, Void, R> {

        boolean integrate(T item, Downstream<? super R> downstream);

        @Override
        default boolean integrate(Void state, T item, Downstream<? super R> downstream) {
            return integrate(item, downstream);
        }
    }

    sealed interface StatefulBase<T, A extends Stateful.State<T, R>, R> extends Gatherer4j2<T, A, R> {

        A initialize();

        @Override
        default Supplier<A> initializer() {
            return this::initialize;
        }

        @Override
        default boolean integrate(A state, T item, Downstream<? super R> downstream) {
            return state.integrate(item, downstream);
        }
    }

    @FunctionalInterface
    non-sealed interface Stateful<T, R> extends StatefulBase<T, Stateful.State<T, R>, R> {

        @FunctionalInterface
        non-sealed interface WithFinisher<T, R> extends StatefulBase<T, WithFinisher.State<T, R>, R> {

            @Override
            default BiConsumer<WithFinisher.State<T, R>, Downstream<? super R>> finisher() {
                return WithFinisher.State::finish;
            }

            @FunctionalInterface
            non-sealed interface WithCombiner<T, R> extends StatefulBase<T, WithCombiner.State<T, R>, R> {

                @Override
                default BinaryOperator<WithCombiner.State<T, R>> combiner() {
                    return WithCombiner.State::combine;
                }

                interface State<T, R> extends WithFinisher.State<T, R> {
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

    enum IntegrationMode {
        DEFAULT, GREEDY
    }
}
