package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameSlot;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.Objects;

public class FilterCompileExpr extends CompileExpr {

  public static ExprBase compile(FrameDescriptorPart from, RexNode child, SinkContext context) {
    CompileExpr compiler = new FilterCompileExpr(from, context);
    return child.accept(compiler);
  }

  FilterCompileExpr(FrameDescriptorPart from, SinkContext context) {
    super(from, context);
  }

  @Override
  public ExprBase visitInputRef(RexInputRef inputRef) {
    int index = inputRef.getIndex();
    FrameSlot slot = from.findFrameSlot(index);
    Objects.requireNonNull(slot);

    return ExprReadLocalNodeGen.create(slot);
  }

  @Override
  protected CompileExpr createCompileExpr(FrameDescriptorPart from, SinkContext context) {
    return new FilterCompileExpr(from, context);
  }
}
