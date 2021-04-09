package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.ArrowUtils;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class FilterSink extends RelRowSink {

  private static final Logger log = LoggerFactory.getLogger(FilterSink.class);

  public static FilterSink createSink(
    FrameDescriptorPart framePart,
    RexNode condition,
    CompileContext compileContext,
    ThenRowSink next) {
    log.debug("createSink");

    ExprBase conditionExpr = FilterCompileExpr.compile(framePart, condition, compileContext);
    RowSink rowSink = next.apply(framePart);
    return new FilterSink(framePart, conditionExpr, rowSink);
  }

  FrameDescriptorPart framePart;
  ExprBase conditionExpr;

  private FilterSink(FrameDescriptorPart framePart, ExprBase conditionExpr, RowSink then) {
    super(then);
    this.framePart = framePart;
    this.conditionExpr = conditionExpr;
  }

  @Override
  protected FrameDescriptorPart getFrameDescriptorPart() {
    return this.getFrameDescriptorPart();
  }

  @Override
  public SinkContext executeByRow(VirtualFrame frame, FrameDescriptorPart framePart, SinkContext context)
    throws UnexpectedResultException {
    if (this.conditionExpr.executeBoolean(frame)) {
      return then.executeByRow(frame, this.framePart, context);
    }
    return context;
  }
}
