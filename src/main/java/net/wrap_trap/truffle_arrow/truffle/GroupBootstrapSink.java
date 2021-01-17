package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import org.apache.calcite.rel.type.RelDataType;

public class GroupBootstrapSink extends RelRowSink {

  private FrameDescriptorPart bootstrapPart;
  private CompileContext compileContext;
  private RowSink then;

  public static RowSink createSink(FrameDescriptorPart framePart, RelDataType relDataType, CompileContext context, ThenRowSink next) {
    FrameDescriptorPart newPart = framePart.newPart();
    for (int i = 0; i < relDataType.getFieldList().size(); i ++) {
      newPart.addFrameSlot();
    }
    RowSink rowSink = next.apply(newPart);
    return new GroupBootstrapSink(framePart, context, rowSink);
  }

  public GroupBootstrapSink(FrameDescriptorPart framePart, CompileContext compileContext, RowSink then) {
    super(then);
    this.bootstrapPart = framePart;
    this.compileContext = compileContext;
    this.then = then;
  }

  @Override
  protected FrameDescriptorPart getFrameDescriptorPart() {
    return this.bootstrapPart;
  }

  @Override
  public SinkContext executeByRow(VirtualFrame frame, FrameDescriptorPart framePart, SinkContext context) throws UnexpectedResultException {
    return then.executeByRow(frame, this.bootstrapPart, context);
  }
}
