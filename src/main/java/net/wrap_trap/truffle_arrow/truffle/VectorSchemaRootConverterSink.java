package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.ArrowUtils;
import org.apache.arrow.vector.VectorSchemaRoot;

public class VectorSchemaRootConverterSink extends RelRowSink {

  private int index = 0;
  private VectorSchemaRoot vectorSchemaRoot;

  public static RelRowSink createSink(FrameDescriptorPart framePart, CompileContext context, ThenRowSink next) {
    return new VectorSchemaRootConverterSink(ArrowUtils.createVectorSchemaRoot(framePart));
  }

  public VectorSchemaRootConverterSink(VectorSchemaRoot vectorSchemaRoot) {
    super(null);
    this.vectorSchemaRoot = vectorSchemaRoot;
  }

  @Override
  protected FrameDescriptorPart getFrameDescriptorPart() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SinkContext executeByRow(VirtualFrame frame, FrameDescriptorPart framePart, SinkContext context) throws UnexpectedResultException {
    ArrowUtils.setValues(frame, framePart, this.vectorSchemaRoot, this.index ++);
    return context;
  }

  @Override
  public SinkContext afterExecute(VirtualFrame frame, SinkContext context) {
    this.vectorSchemaRoot.getFieldVectors().stream().forEach(fieldVector -> fieldVector.setValueCount(this.index));
    this.vectorSchemaRoot.setRowCount(this.index);
    return context.setVectorSchemaRoots(new VectorSchemaRoot[]{this.vectorSchemaRoot});
  }
}
