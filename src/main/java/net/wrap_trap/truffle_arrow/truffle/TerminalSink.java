package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

public class TerminalSink extends RowSource {

  private SinkContext context;
  private FrameDescriptorPart framePart;

  public static RowSource compile(CompileContext compileContext, ThenRowSink next) {
    FrameDescriptorPart framePart = FrameDescriptorPart.root(0);
    SinkContext sinkContext = new SinkContext(null, compileContext.getInputRefSlotMaps());
    return new TerminalSink(framePart, sinkContext, next.apply(framePart));
  }

  private TerminalSink(FrameDescriptorPart framePart, SinkContext context, RowSink then) {
    super(then);
    this.framePart = framePart;
    this.context =context;
  }

  @Override
  protected void executeVoid() throws UnexpectedResultException {
    VirtualFrame frame = Truffle.getRuntime().createVirtualFrame(new Object[] { }, this.framePart.frame());
    then.executeVoid(frame, this.context);
    then.afterExecute(frame, this.context);
  }
}
