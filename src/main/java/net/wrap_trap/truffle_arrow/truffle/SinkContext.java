package net.wrap_trap.truffle_arrow.truffle;

import net.wrap_trap.truffle_arrow.ArrowFieldType;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.calcite.rex.RexInputRef;

import java.util.*;
import java.util.stream.Collectors;

public class SinkContext {
  private Map<Integer, FieldVector> vectors;
  private Set<InputRefSlotMap> inputRefSlotMaps;
  private String partition;

  public SinkContext(Map<Integer, FieldVector> vectors,
                     Set<InputRefSlotMap> inputRefSlotMaps, String partition) {
    this.vectors = vectors;
    this.inputRefSlotMaps = inputRefSlotMaps;
    this.partition = partition;
  }

  public Map<Integer, FieldVector> vectors() {
    if (this.vectors == null) {
      throw new IllegalStateException("vectors have not been initialized yet");
    }
    return this.vectors;
  }

  public Set<InputRefSlotMap> getInputRefSlotMaps() {
    if (this.inputRefSlotMaps == null) {
      throw new IllegalStateException("inputRefSlotMaps have not been initialized yet");
    }
    return this.inputRefSlotMaps;
  }

  public String getPartition() {
    return this.partition;
  }
}
