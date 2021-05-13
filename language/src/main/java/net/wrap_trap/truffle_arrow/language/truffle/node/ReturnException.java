package net.wrap_trap.truffle_arrow.language.truffle.node;


public class ReturnException extends RuntimeException {
  private Object result;

  public ReturnException(Object result) {
    this.result = result;
  }

  public Object getResult() {
    return this.result;
  }
}
