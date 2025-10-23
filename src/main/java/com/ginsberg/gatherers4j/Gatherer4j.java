package com.ginsberg.gatherers4j;

import com.ginsberg.gatherers4j.Gatherer4j.Stateful.WithFinisher.WithCombiner;

import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiPredicate;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Gatherer;

/// A Gatherer interface extension that provides a fluent api for building a gatherer. Especially useful when a direct implementation or interface extension is desirable.
///
/// @param <T> The type of the input elements
/// @param <A> The type of the State
/// @param <R> The type of the elements in the downstream
public sealed interface Gatherer4j<T, A, R> extends Gatherer<T, A, R> {

    static <T, R> Stateless<T, R> ofSequential(BiPredicate<? super T, ? super Downstream<? super R>> integrate) {
        return ofSequential(IntegrationMode.DEFAULT, integrate);
    }

    static <T, R> Stateless<T, R> ofSequential(IntegrationMode integrationMode, BiPredicate<? super T, ? super Downstream<? super R>> integrate) {
        validate(integrationMode, integrate);
        return new Stateless<>() {
            @Override
            public boolean integrate(final T t, final Downstream<? super R> u) {
                return integrate.test(t, u);
            }

            @Override
            public IntegrationMode integrationMode() {
                return integrationMode;
            }
        };
    }

    static <T, R> Stateless<T, R> of(BiPredicate<? super T, ? super Downstream<? super R>> integrate) {
        return of(IntegrationMode.DEFAULT, integrate);
    }

    static <T, R> Stateless<T, R> of(IntegrationMode integrationMode, BiPredicate<? super T, ? super Downstream<? super R>> integrate) {
        validate(integrationMode, integrate);
        return new Stateless<>() {

            @Override
            public boolean integrate(final T item, final Downstream<? super R> downstream) {
                return integrate.test(item, downstream);
            }

            @Override
            public BinaryOperator<Void> combiner() {
                return (s, _) -> s;
            }

            @Override
            public IntegrationMode integrationMode() {
                return integrationMode;
            }
        };
    }

    static <T, R> Stateful<T, R> ofSequential(IntegrationMode integrationMode, Stateful<T, R> initializer) {
        validate(integrationMode, initializer);
        return new Stateful<>() {
            @Override
            public Stateful.State<T, R> initialize() {
                return initializer.initialize();
            }

            @Override
            public IntegrationMode integrationMode() {
                return integrationMode;
            }
        };
    }

    static <T, R> Stateful<T, R> ofSequential(Stateful<T, R> initialize) {
        return ofSequential(IntegrationMode.DEFAULT, initialize);
    }

    static <T, R> Stateful.WithFinisher<T, R> ofSequential(
            final IntegrationMode integrationMode,
            final Stateful.WithFinisher<T, R> initializer
    ) {
        validate(integrationMode, initializer);
        return new Stateful.WithFinisher<>() {

            @Override
            public State.WithFinisher<T, R> initialize() {
                return initializer.initialize();
            }

            @Override
            public IntegrationMode integrationMode() {
                return integrationMode;
            }
        };
    }

    static <T, R> Stateful.WithFinisher<T, R> ofSequential(Stateful.WithFinisher<T, R> initializer) {
        return ofSequential(IntegrationMode.DEFAULT, initializer);
    }

    static <T, A extends State.WithFinisher.WithCombiner<T, A, R>, R> WithCombiner<T, A, R> of(
            final IntegrationMode integrationMode,
            final WithCombiner<T, A, R> initializer
    ) {
        validate(integrationMode, initializer);
        return new WithCombiner<>() {

            @Override
            public A initialize() {
                return initializer.initialize();
            }

            @Override
            public IntegrationMode integrationMode() {
                return integrationMode;
            }
        };
    }

    static <T, A extends State.WithFinisher.WithCombiner<T, A, R>, R> WithCombiner<T, A, R> of(WithCombiner<T, A, R> initializer) {
        return of(IntegrationMode.DEFAULT, initializer);
    }

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
    non-sealed interface Stateless<T, R> extends Gatherer4j<T, Void, R> {

        boolean integrate(T item, Downstream<? super R> downstream);

        @Override
        default boolean integrate(Void state, T item, Downstream<? super R> downstream) {
            return integrate(item, downstream);
        }
    }

    sealed interface StatefulBase<T, A extends State<T, R>, R> extends Gatherer4j<T, A, R> {

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
        non-sealed interface WithFinisher<T, R> extends StatefulBase<T, State.WithFinisher<T, R>, R> {

            @Override
            default BiConsumer<State.WithFinisher<T, R>, Downstream<? super R>> finisher() {
                return State.WithFinisher::finish;
            }

            @FunctionalInterface
            non-sealed interface WithCombiner<T, A extends State.WithFinisher.WithCombiner<T, A, R>, R> extends StatefulBase<T, A, R> {

                @Override
                default BinaryOperator<A> combiner() {
                    return State.WithFinisher.WithCombiner::combine;
                }

                @Override
                default BiConsumer<A, Downstream<? super R>> finisher() {
                    return State.WithFinisher::finish;
                }
            }
        }
    }

    interface State<T, R> {
        boolean integrate(T item, Downstream<? super R> downstream);

        interface WithFinisher<T, R> extends Stateful.State<T, R> {
            void finish(Downstream<? super R> downstream);

            interface WithCombiner<T, A extends WithCombiner<T, A, R>, R> extends WithFinisher<T, R> {
                A combine(A other);
            }
        }
    }

    private static void validate(final IntegrationMode integrationMode, final Object initializer) {
        Objects.requireNonNull(initializer, "Initializer/Integrator must not be null");
        Objects.requireNonNull(integrationMode, "Integration mode must not be null");
    }

    enum IntegrationMode {
        DEFAULT, GREEDY
    }
}
