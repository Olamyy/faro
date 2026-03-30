package dev.faro.core;

import java.io.Serializable;
import java.util.function.Function;

/**
 * A {@link Function} that is also {@link Serializable}.
 *
 * <p>Required for lambdas used inside distributed operators that must survive job serialization.
 * Declaring extractors as this type surfaces the serialization constraint at the call site.
 */
@FunctionalInterface
public interface SerializableFunction<T, R> extends Function<T, R>, Serializable {
}
