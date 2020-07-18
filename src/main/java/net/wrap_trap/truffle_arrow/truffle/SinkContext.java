package net.wrap_trap.truffle_arrow.truffle;

import net.wrap_trap.truffle_arrow.ArrowFieldType;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.calcite.rex.RexInputRef;

import java.util.*;
import java.util.stream.Collectors;

public class SinkContext {
  private Map<Integer, FieldVector> vectors;
  private UInt4Vector selectionVector;
  private Set<InputRefSlotMap> inputRefSlotMaps;

  public SinkContext() {
    this.inputRefSlotMaps = new HashSet<>();
  }

  public void setVectors(Map<Integer, FieldVector> vectors) {
    this.vectors = vectors;
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

  public void addInputRefSlotMap(int index, int slot) {
    this.inputRefSlotMaps.add(new InputRefSlotMap(index, slot));
  }

  public Set<InputRefSlotMap> getInputRefSlotMaps() {
    return this.inputRefSlotMaps;
  }
}
