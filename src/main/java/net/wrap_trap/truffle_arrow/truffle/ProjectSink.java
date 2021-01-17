package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.ArrowUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;


public class ProjectSink extends RelRowSink {

  public static ProjectSink createSink(
    FrameDescriptorPart framePart,
    List<? extends RexNode> projects,
    RelDataType relDataType,
    CompileContext compileContext,
    ThenRowSink next) {
    FrameDescriptorPart newFramePart = framePart.newPart();
    newFramePart.pushRelDataType(relDataType);
    for (int i = 0; i < projects.size(); i ++) {
      FrameSlot slot = newFramePart.addFrameSlot();
      FrameSlotKind slotKind = ArrowUtils.getFrameSlotKind(relDataType.getFieldList().get(i));
      newFramePart.setFrameSlotKind(slot, slotKind);
    }
    RowSink rowSink = next.apply(newFramePart);

    StatementWriteLocal[] locals = new StatementWriteLocal[projects.size()];
    for (int i = 0; i < projects.size(); i ++) {
      RexNode child = projects.get(i);
      ExprBase compiled = compile(newFramePart, child, compileContext);
      FrameSlot slot = newFramePart.findFrameSlot(i);
      locals[i] = StatementWriteLocalNodeGen.create(compiled, slot);
    }
    return new ProjectSink(newFramePart, locals, rowSink);
  }

  private static ExprBase compile(FrameDescriptorPart framePart, RexNode child, CompileContext compileContext) {
    return ProjectCompileExpr.compile(framePart, child, compileContext);
  }

  private FrameDescriptorPart framePart;
  private StatementWriteLocal[] locals;

  private ProjectSink(FrameDescriptorPart framePart, StatementWriteLocal[] locals, RowSink then) {
    super(then);
    this.framePart = framePart;
    this.locals = locals;
  }

  @Override
  protected FrameDescriptorPart getFrameDescriptorPart() {
    return this.framePart;
  }

  @Override
  public SinkContext executeByRow(VirtualFrame frame, FrameDescriptorPart framePart, SinkContext context) throws UnexpectedResultException {
    for (StatementWriteLocal local : locals) {
      local.executeVoid(frame);
    }
    return then.executeByRow(frame, this.framePart, context);
  }
}