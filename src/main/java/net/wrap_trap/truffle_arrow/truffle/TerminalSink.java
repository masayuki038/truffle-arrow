package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

public class TerminalSink extends RowSource {

  private SinkContext context;
  private RowSink then;
  private FrameDescriptorPart framePart;

  public static RowSource compile(SinkContext context, ThenRowSink next) {
    FrameDescriptorPart framePart = FrameDescriptorPart.root(0);
    return new TerminalSink(framePart, context, next.apply(framePart));
  }

  private TerminalSink(FrameDescriptorPart framePart, SinkContext context, RowSink then) {
    this.framePart = framePart;
    this.context =context;
    this.then = then;
  }

  @Override
  protected void executeVoid() throws UnexpectedResultException {
    then.executeVoid(
      Truffle.getRuntime().createVirtualFrame(new Object[] { }, this.framePart.frame()),
      this.context);
  }
}
