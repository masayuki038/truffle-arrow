package net.wrap_trap.truffle_arrow.truffle;

import net.wrap_trap.truffle_arrow.ArrowFieldType;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.calcite.rex.RexInputRef;

import java.util.*;
import java.util.stream.Collectors;

public class SinkContext {
  private Map<Integer, FieldVector> vectors;
  private Map<Integer, ArrowFieldType> arrowFieldTypes;
  private UInt4Vector selectionVector;
  private List<RexInputRef> inputRefs;

  public SinkContext() {
    this.inputRefs = new ArrayList<>();
  }

  public void setVectors(Map<Integer, FieldVector> vectors) {
    this.vectors = vectors;
    this.arrowFieldTypes = new HashMap<>();
    for (Map.Entry<Integer, FieldVector> entry: vectors.entrySet()) {
      ArrowFieldType arrowFieldType =
        ArrowFieldType.of(entry.getValue().getField().getFieldType().getType());
      this.arrowFieldTypes.put(entry.getKey(), arrowFieldType);
    }
  }

  public ArrowFieldType getArrowFieldType(int index) {
    return this.arrowFieldTypes.get(index);
  }

  public Map<Integer, FieldVector> vectors() {
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

  public Set<Integer> getInputRefIndices() {
    return this.inputRefs.stream().map(inputRef -> inputRef.getIndex()).collect(Collectors.toSet());
  }
}
