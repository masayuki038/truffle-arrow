package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

public abstract class RelRowSink extends RowSink {

  protected RowSink then;

  protected RelRowSink(RowSink then) {
    this.then = then;
    this.insert(then);
  }

  @Override
  public void afterExecute(VirtualFrame frame, SinkContext context) throws UnexpectedResultException {
    then.afterExecute(frame, context);
  }
}
