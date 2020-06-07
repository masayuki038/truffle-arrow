package net.wrap_trap.truffle_arrow.truffle;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.calcite.rex.RexInputRef;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class SinkContext {
  private List<FieldVector> vectors;
  private UInt4Vector selectionVector;
  private List<RexInputRef> inputRefs;

  public SinkContext() {
    this.inputRefs = new ArrayList<>();
  }

  public void setVectors(List<FieldVector> vectors) {
    this.vectors = vectors;
  }

  public List<FieldVector> vectors() {
    if (this.vectors == null) {
      throw new IllegalStateException("vectors have not been initialized yet");
    }
    return this.vectors;
  }

  public void setSelectionVector(UInt4Vector selectionVector) {
    this.selectionVector = selectionVector;
  }

  public UInt4Vector selectionVector() {
    return selectionVector;
  }

  public void addInputRef(RexInputRef rexInputRef) {
    this.inputRefs.add(rexInputRef);
  }

  public List<Integer> getInputRefIndices() {
    return this.inputRefs.stream().map(inputRef -> inputRef.getIndex()).collect(Collectors.toList());
  }
}
