package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.ArrowUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

public class VectorSchemaRootConverterSink extends RelRowSink {

  private static final Logger log = LoggerFactory.getLogger(VectorSchemaRootConverterSink.class);

  private VectorSchemaRoot[] vectorSchemaRoots;
  private int[] indexes;
  private int index = 0;

  public static RelRowSink createSink(FrameDescriptorPart framePart, CompileContext context, ThenRowSink next) {
    log.debug("createSink");

    BufferAllocator allocator = ArrowUtils.createAllocator("VectorSchemaRootConverterSink#createSink");
    VectorSchemaRoot vectorSchemaRoot = ArrowUtils.createVectorSchemaRoot(framePart, allocator);
    VectorSchemaRoot[] vectorSchemaRoots = new VectorSchemaRoot[]{vectorSchemaRoot};
    return new VectorSchemaRootConverterSink(vectorSchemaRoots, allocator);
  }

  public VectorSchemaRootConverterSink(VectorSchemaRoot[] vectorSchemaRoots, BufferAllocator allocator) {
    super(null);
    this.vectorSchemaRoots = vectorSchemaRoots;
    this.indexes = new int[vectorSchemaRoots.length];
  }

  @Override
  protected FrameDescriptorPart getFrameDescriptorPart() {
    throw new UnsupportedOperationException();
  }

  @Override
  public SinkContext executeByRow(VirtualFrame frame, FrameDescriptorPart framePart, SinkContext context) throws UnexpectedResultException {
    int bucket = this.index ++ % this.vectorSchemaRoots.length;
    ArrowUtils.setValues(frame, framePart, this.vectorSchemaRoots[bucket], this.indexes[bucket] ++);
    return context;
  }

  @Override
  public SinkContext afterExecute(VirtualFrame frame, SinkContext context) {
    log.debug("afterExecute");

    for (int i = 0; i < this.vectorSchemaRoots.length; i ++) {
      List<FieldVector> fieldVectors = vectorSchemaRoots[i].getFieldVectors();
      for (int j = 0; j < fieldVectors.size(); j ++) {
        fieldVectors.get(j).setValueCount(this.indexes[i]);
      }
      vectorSchemaRoots[i].setRowCount(this.indexes[i]);
    }
    return context.setVectorSchemaRoots(this.vectorSchemaRoots);
    // TODO dispose this.allocator
  }
}
