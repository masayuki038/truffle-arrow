package net.wrap_trap.truffle_arrow.truffle;

public abstract class RelRowSink extends RowSink {

  protected RowSink then;

  protected RelRowSink(RowSink then) {
    this.then = then;
    this.insert(then);
  }
}
