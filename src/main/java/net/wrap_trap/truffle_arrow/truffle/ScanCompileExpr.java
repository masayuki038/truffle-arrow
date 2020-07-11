package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameSlot;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.Objects;

public class ScanCompileExpr extends CompileExpr {
  public static ExprBase compile(FrameDescriptorPart from, RexNode child, SinkContext context) {
    CompileExpr compiler = new ScanCompileExpr(from, context);
    return child.accept(compiler);
  }

  ScanCompileExpr(FrameDescriptorPart from, SinkContext context) {
    super(from, context);
  }

  @Override
  public ExprBase visitInputRef(RexInputRef inputRef) {
    FrameSlot slot = from.addFrameSlot();
    this.context.addInputRefSlotMap(inputRef.getIndex(), from.getCurrentSlotPosition());
    Objects.requireNonNull(slot);

    return ExprReadLocalNodeGen.create(slot);
  }

  @Override
  protected CompileExpr createCompileExpr(FrameDescriptorPart from, SinkContext context) {
    return new ScanCompileExpr(from, context);
  }
}
