package net.wrap_trap.truffle_arrow;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.UInt4Vector;
import org.apache.arrow.vector.VectorSchemaRoot;

public class VectorSchemaRootContainerImpl implements VectorSchemaRootContainer {

  private VectorSchemaRoot[] vectorSchemaRoots;
  private UInt4Vector selectionVector;

  public VectorSchemaRootContainerImpl(VectorSchemaRoot[] vectorSchemaRoots, UInt4Vector selectionVector) {
    this.vectorSchemaRoots = vectorSchemaRoots;
    this.selectionVector = selectionVector;
  }

  @Override
  public int getVectorSchemaRootCount() {
    return vectorSchemaRoots.length;
  }

  @Override
  public int getRowCount(int vectorSchemaRootIndex) {
    return vectorSchemaRoots[vectorSchemaRootIndex].getRowCount();
  }

  @Override
  public int getFieldCount(int vectorSchemaRootIndex) {
    return vectorSchemaRoots[vectorSchemaRootIndex].getFieldVectors().size();
  }

  @Override
  public FieldVector getFieldVector(int vectorSchemaRootIndex, int fieldIndex) {
    return vectorSchemaRoots[vectorSchemaRootIndex].getFieldVectors().get(fieldIndex);
  }

  @Override
  public UInt4Vector selectionVector() {
    return selectionVector;
  }
}
