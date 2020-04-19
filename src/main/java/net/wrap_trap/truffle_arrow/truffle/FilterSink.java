package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import org.apache.calcite.rex.RexNode;

public class FilterSink extends RowSink {

  public static FilterSink createSink(FrameDescriptorPart sourceFrame, RexNode condition, ThenRowSink next) {
    RowSink rowSink = next.apply(sourceFrame);
    return new FilterSink(CompileExpr.compile(sourceFrame, condition), rowSink);
  }

  RowSink then;
  ExprBase conditionExpr;

  private FilterSink(ExprBase conditionExpr, RowSink then) {
    this.conditionExpr = conditionExpr;
    this.then = then;
  }

  @Override
  public void executeVoid(VirtualFrame frame, FrameDescriptorPart sourceFrame) throws UnexpectedResultException {
    if (conditionExpr.executeBoolean(frame)) {
      then.executeVoid(frame, sourceFrame);
    }
  }
}
