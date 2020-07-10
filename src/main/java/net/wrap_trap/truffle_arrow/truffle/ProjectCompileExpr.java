package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameSlot;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.Objects;

public class ProjectCompileExpr extends CompileExpr {

  public static ExprBase compile(FrameDescriptorPart from, RexNode child, SinkContext context, boolean scan) {
    CompileExpr compiler = new ProjectCompileExpr(from, context, scan);
    return child.accept(compiler);
  }

  ProjectCompileExpr(FrameDescriptorPart from, SinkContext context, boolean scan) {
    super(from, context, scan);
  }

  @Override
  public ExprBase visitInputRef(RexInputRef inputRef) {
    int index = inputRef.getIndex();
    FrameSlot slot = from.findFrameSlotInPrevious(index);
    Objects.requireNonNull(slot);

    return ExprReadLocalNodeGen.create(slot);
  }
}
