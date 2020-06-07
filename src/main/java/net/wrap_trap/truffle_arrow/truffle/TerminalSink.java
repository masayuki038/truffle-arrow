package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

public class TerminalSink extends RowSource {

  private SinkContext context;
  private RowSink then;
  private FrameDescriptor frameDescriptor;

  public static RowSource compile(SinkContext context, ThenRowSink next) {
    FrameDescriptor empty = new FrameDescriptor();

    return new TerminalSink(empty, context, next.apply(empty));
  }

  private TerminalSink(FrameDescriptor frameDescriptor, SinkContext context, RowSink then) {
    this.frameDescriptor = frameDescriptor;
    this.context =context;
    this.then = then;
  }

  @Override
  protected void executeVoid() throws UnexpectedResultException {
    then.executeVoid(
      Truffle.getRuntime().createVirtualFrame(new Object[] { }, frameDescriptor),
      frameDescriptor,
      this.context);
  }
}
