package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.ArrowUtils;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.calcite.rex.RexNode;


public class FilterSink extends RelRowSink {

  public static FilterSink createSink(
    FrameDescriptorPart framePart,
    RexNode condition,
    SinkContext context,
    ThenRowSink next) {
    RowSink rowSink = next.apply(framePart);
    return new FilterSink(framePart, FilterCompileExpr.compile(framePart, condition, context), rowSink);
  }

  FrameDescriptorPart framePart;
  ExprBase conditionExpr;

  private FilterSink(FrameDescriptorPart framePart, ExprBase conditionExpr, RowSink then) {
    super(then);
    this.framePart = framePart;
    this.conditionExpr = conditionExpr;
  }

  @Override
  public void executeByRow(VirtualFrame frame, FrameDescriptorPart framePart, SinkContext context)
    throws UnexpectedResultException {
    if (this.conditionExpr.executeBoolean(frame)) {
      then.executeByRow(frame, this.framePart, context);
    }
  }

  class SelectionVector {
    private UInt4Vector selectionVector;
    private int index = 0;

    SelectionVector(int valueCount) {
      this.selectionVector = ArrowUtils.createSelectionVector();
      this.selectionVector.setValueCount(valueCount);
    }

    void add(int i) {
      this.selectionVector.set(this.index ++, i);
    }

    UInt4Vector getVector() {
      this.selectionVector.setValueCount(this.index);
      return this.selectionVector;
    }
  }
}
