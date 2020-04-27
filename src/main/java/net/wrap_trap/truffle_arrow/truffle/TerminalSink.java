package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

public class TerminalSink extends RowSource {

  private RowSink then;
  private FrameDescriptor frameDescriptor;

  public static RowSource compile(ThenRowSink next) {
    FrameDescriptor empty = new FrameDescriptor();

    return new TerminalSink(empty, next.apply(empty));
  }

  private TerminalSink(FrameDescriptor frameDescriptor, RowSink then) {
    this.frameDescriptor = frameDescriptor;
    this.then = then;
  }

  @Override
  protected void executeVoid() throws UnexpectedResultException {
    then.executeVoid(Truffle.getRuntime().createVirtualFrame(new Object[] { }, frameDescriptor), frameDescriptor);
  }
}
