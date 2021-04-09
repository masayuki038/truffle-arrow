package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import org.apache.calcite.rel.type.RelDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GroupBootstrapSink extends RelRowSink {

  private static final Logger log = LoggerFactory.getLogger(GroupBootstrapSink.class);

  private FrameDescriptorPart bootstrapPart;
  private RowSink then;

  public static RowSink createSink(FrameDescriptorPart framePart, RelDataType relDataType, CompileContext context, ThenRowSink next) {
    log.debug("createSink");

    FrameDescriptorPart newPart = framePart.newPart();
    for (int i = 0; i < relDataType.getFieldList().size(); i ++) {
      newPart.addFrameSlot();
    }
    RowSink rowSink = next.apply(newPart);
    return new GroupBootstrapSink(framePart, rowSink);
  }

  public GroupBootstrapSink(FrameDescriptorPart framePart, RowSink then) {
    super(then);
    this.bootstrapPart = framePart;
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
