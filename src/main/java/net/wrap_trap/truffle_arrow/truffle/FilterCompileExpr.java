package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameSlot;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;

import java.util.Objects;

public class FilterCompileExpr extends CompileExpr {

  public static ExprBase compile(FrameDescriptorPart from, RexNode child, CompileContext compileContext) {
    CompileExpr compiler = new FilterCompileExpr(from, compileContext);
    return child.accept(compiler);
  }

  FilterCompileExpr(FrameDescriptorPart from, CompileContext compileContext) {
    super(from, compileContext);
  }

  @Override
  public ExprBase visitInputRef(RexInputRef inputRef) {
    int index = inputRef.getIndex();
    FrameSlot slot = from.findFrameSlot(index);
    Objects.requireNonNull(slot);

    return ExprReadLocalNodeGen.create(slot);
  }

  @Override
  protected CompileExpr createCompileExpr(FrameDescriptorPart from, CompileContext compileContext) {
    return new FilterCompileExpr(from, compileContext);
  }
}
