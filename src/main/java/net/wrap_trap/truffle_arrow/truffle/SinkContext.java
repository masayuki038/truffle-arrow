package net.wrap_trap.truffle_arrow.truffle;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.File;
import java.util.*;

public class SinkContext {
  private Map<Integer, FieldVector> vectors;
  private Set<InputRefSlotMap> inputRefSlotMaps;
  private File partition;
  private List<Row> rows;
  private VectorSchemaRoot[] vectorSchemaRoots;

  public SinkContext(Set<InputRefSlotMap> inputRefSlotMaps, File partition, List<Row> rows) {
    this.inputRefSlotMaps = inputRefSlotMaps;
    this.partition = partition;
    this.rows = rows;
  }

  public Set<InputRefSlotMap> getInputRefSlotMaps() {
    if (this.inputRefSlotMaps == null) {
      throw new IllegalStateException("inputRefSlotMaps have not been initialized yet");
    }
    return this.inputRefSlotMaps;
  }

  public File getPartition() {
    return this.partition;
  }

  public List<Row> getRows() {
    return this.rows;
  }

  public void addRow(Row row) {
    this.rows.add(row);
  }

  public VectorSchemaRoot[] getVectorSchemaRoots() {
    return this.vectorSchemaRoots;
  }

  public SinkContext setVectorSchemaRoots(VectorSchemaRoot[] vectorSchemaRoots) {
    SinkContext newContext = new SinkContext(this.inputRefSlotMaps, this.partition, this.rows);
    newContext.vectorSchemaRoots = vectorSchemaRoots;
    return newContext;
  }
}
