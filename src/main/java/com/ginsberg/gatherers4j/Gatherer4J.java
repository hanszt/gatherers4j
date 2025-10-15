package com.ginsberg.gatherers4j;

import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Gatherer;

public sealed interface Gatherer4J<T, A, R> extends Gatherer<T, A, R> {

    IntegrationMode integrationMode();

    boolean integrate(A state, T item, Downstream<? super R> downstream);

    @Override
    default Integrator<A, T, R> integrator() {
        return switch (integrationMode()) {
            case DEFAULT -> this::integrate;
            case GREEDY -> Integrator.<A, T, R>ofGreedy(this::integrate);
        };
    }

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
