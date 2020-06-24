package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.ArrowUtils;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.calcite.rex.RexNode;

import java.util.List;


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
  public void executeVoid(VirtualFrame frame, SinkContext context) throws UnexpectedResultException {
    List<FieldVector> vectors = context.vectors();
    SelectionVector selectionVector = new SelectionVector(vectors.get(0).getValueCount());

    this.vectorEach(frame, this.framePart, context, i -> {
      try {
        if (this.conditionExpr.executeBoolean(frame)) {
          then.executeByRow(frame, this.framePart, context);
          selectionVector.add(i);
        }
      } catch (UnexpectedResultException e) {
        throw new RuntimeException(e);
      }
    });
    context.setSelectionVector(selectionVector.getVector());
    // Call only then.executeByRow
    // then.executeVoid(frame, frameDescriptor, context);
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
