package net.wrap_trap.truffle_arrow;

@FunctionalInterface
public interface TryFunction<T, R, E extends Exception> {
  R apply(T t) throws E;

  static <T, E extends Exception> TryFunction<T, T, E> identity() {
    return r -> r;
  }
}
