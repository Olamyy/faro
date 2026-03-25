package dev.faro.core;

import java.io.Serializable;
import java.util.function.Function;

/**
 * A {@link Function} that is also {@link Serializable}.
 *
 * <p>Required for lambdas used inside Flink operators, which must survive job serialization.
 * Lambdas that capture non-serializable references from the enclosing scope will fail at
 * Flink job submission — not at compile time. Declaring extractors as this type surfaces
 * the constraint at the call site.
 */
@FunctionalInterface
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {
}
