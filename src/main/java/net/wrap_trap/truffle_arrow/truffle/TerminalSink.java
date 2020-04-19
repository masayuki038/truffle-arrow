package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.nodes.UnexpectedResultException;

public class TerminalSink extends RowSource {

  private RowSink then;
  private FrameDescriptorPart sourceFrame;

  public static RowSource compile(ThenRowSink next) {
    FrameDescriptorPart empty = FrameDescriptorPart.root(0);

    return new TerminalSink(empty, next.apply(empty));
  }

  private TerminalSink(FrameDescriptorPart sourceFrame, RowSink then) {
    this.sourceFrame = sourceFrame;
    this.then = then;
    assert sourceFrame.size() == 0;
  }

  @Override
  protected void executeVoid() throws UnexpectedResultException {
    then.executeVoid(Truffle.getRuntime().createVirtualFrame(new Object[] { }, sourceFrame.frame()), sourceFrame);
  }
}
