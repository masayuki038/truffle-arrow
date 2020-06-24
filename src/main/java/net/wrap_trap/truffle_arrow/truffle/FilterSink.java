package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.ArrowUtils;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.calcite.rex.RexNode;


public class FilterSink extends RowSink {

  public static FilterSink createSink(
    FrameDescriptorPart framePart,
    RexNode condition,
    SinkContext context,
    ThenRowSink next) {
    FrameDescriptorPart newFramePart = framePart.newPart();
    RowSink rowSink = next.apply(newFramePart);
    return new FilterSink(newFramePart, CompileExpr.compile(newFramePart, condition, context), rowSink);
  }

  FrameDescriptorPart framePart;
  RowSink then;
  ExprBase conditionExpr;

  private FilterSink(FrameDescriptorPart framePart, ExprBase conditionExpr, RowSink then) {
    this.framePart = framePart;
    this.conditionExpr = conditionExpr;
    this.then = then;
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
