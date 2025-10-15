package com.ginsberg.gatherers4j;

import java.util.function.BiConsumer;
import java.util.function.Supplier;
import java.util.stream.Gatherer;

public interface Gatherer4J<T, A, R> extends Gatherer<T, A, R> {

    default IntegrationMode integrationMode() {
        return IntegrationMode.DEFAULT;
    }

    interface Stateless<T, R> extends Gatherer4J<T, Void, R> {

        boolean integrate(T item, Downstream<? super R> downstream);

        @Override
        default Integrator<Void, T, R> integrator() {
            return switch (integrationMode()) {
                case DEFAULT -> (_, item, downstream) -> integrate(item, downstream);
                case GREEDY -> Integrator.ofGreedy((_, item, downstream) -> integrate(item, downstream));
            };
        }

        default IntegrationMode integrationMode() {
            return IntegrationMode.GREEDY;
        }
    }

    interface Stateful<T, A, R> extends Gatherer4J<T, A, R> {

        A initialize();

        @Override
        default Supplier<A> initializer() {
            return this::initialize;
        }

        boolean integrate(A state, T item, Downstream<? super R> downstream);

        @Override
        default Integrator<A, T, R> integrator() {
            return switch (integrationMode()) {
                case DEFAULT -> this::integrate;
                case GREEDY -> Integrator.<A, T, R>ofGreedy(this::integrate);
            };
        }

        interface WithFinisher<T, A, R> extends Stateful<T, A, R> {

            void finish(A state, Downstream<? super R> downstream);

            @Override
            default BiConsumer<A, Downstream<? super R>> finisher() {
                return this::finish;
            }
        }
    }

    enum IntegrationMode {
        GREEDY, DEFAULT
    }
}
