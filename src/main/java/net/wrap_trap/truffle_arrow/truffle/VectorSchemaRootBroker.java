package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.UnexpectedResultException;
import net.wrap_trap.truffle_arrow.ArrowUtils;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;


public class VectorSchemaRootBroker extends RelRowSink {

  private static final Logger log = LoggerFactory.getLogger(VectorSchemaRootBroker.class);

  private ArrowFileReader reader;
  private FrameDescriptorPart framePart;
  private Schema schema;
  private int[] fields;

  public static VectorSchemaRootBroker compile(
      FrameDescriptorPart framePart,
      File dir,
      Schema schema,
      List<? extends RexNode> projects,
      int[] fields,
      CompileContext compileContext,
      ThenRowSink then) {
    log.debug("dir: " + dir);
    if (projects != null && projects.size() > 0) {
      for (RexNode child : projects) {
        compile(framePart, child, compileContext);
      }
    } else {
      for (int field : fields) {
        compileContext.addInputRefSlotMap(field, field);
        framePart.addFrameSlot();
      }
    }
    RowSink sink = then.apply(framePart);
    return new VectorSchemaRootBroker(framePart, framePart.getRelDataType(), dir, schema, projects, fields, sink);
  }

  private static ExprBase compile(FrameDescriptorPart framePart, RexNode child, CompileContext compileContext) {
    return ScanCompileExpr.compile(framePart, child, compileContext);
  }

  private VectorSchemaRootBroker(
      FrameDescriptorPart framePart,
      RelDataType relType,
      File dir,
      Schema schema,
      List<? extends RexNode> projects,
      int[] fields, RowSink then) {
    super(then);
    this.framePart = framePart;
    this.schema = schema;
    this.fields = fields;
    this.then = then;
  }

  @Override
  protected FrameDescriptorPart getFrameDescriptorPart() {
    return this.getFrameDescriptorPart();
  }

  @Override
  public SinkContext executeVoid(VirtualFrame frame, VectorSchemaRoot[] alwaysNull, SinkContext initialContext) {
    log.debug("executeVoid");

    try (BufferAllocator allocator = ArrowUtils.createAllocator()) {
      return loadVectorSchemaRoots(initialContext, allocator, vectorSchemaRoots -> {
        Map<Integer, Integer> indexesMap = createFieldIndexesMap();

        SinkContext context = initialContext;
        for (VectorSchemaRoot vectorSchemaRoot : vectorSchemaRoots) {
          List<FieldVector> fieldVectors = vectorSchemaRoot.getFieldVectors();
          Map<Integer, FieldVector> selected = new HashMap<>();
          for (InputRefSlotMap inputRefSlotMap : context.getInputRefSlotMaps()) {
            int fieldVectorIndex = indexesMap.get(inputRefSlotMap.getIndex());
            selected.put(inputRefSlotMap.getSlot(), fieldVectors.get(fieldVectorIndex));
          }
          context = this.vectorEach(frame, this.framePart, selected, context);
          vectorSchemaRoot.close();
        }
        return context;
      });
    }
  }

  @Override
  public SinkContext executeByRow(VirtualFrame frame, FrameDescriptorPart framePart, SinkContext context) throws UnexpectedResultException {
    return this.then.executeByRow(frame, framePart, context);
  }

  private Map<Integer, Integer> createFieldIndexesMap() {
    Map<Integer, Integer> map = new HashMap<>();
    for (int i = 0; i < this.fields.length; i ++) {
      map.put(this.fields[i], i);
    }
    return map;
  }

  private SinkContext loadVectorSchemaRoots(SinkContext context, BufferAllocator allocator, Function<VectorSchemaRoot[], SinkContext> func) {
    return loadArrowFiles(context, allocator, list -> {
      assert list.size() > 0;
      int size = list.get(0).length;
      List<List<FieldVector>> container = new ArrayList<>();
      for (int i = 0; i < size; i ++) {
        container.add(new ArrayList<>());
      }

      for (VectorSchemaRoot[] roots: list) {
        if (roots.length != size) {
          throw new IllegalStateException(String.format(
            "Unexpected array size of VectorSchemaRoot[]. expect: %d, actual: %d", size, roots.length));
        }
        for (int j = 0; j < roots.length; j ++) {
          List<FieldVector> vectors = roots[j].getFieldVectors();
          assert vectors.size() == 1;
          container.get(j).add(vectors.get(0));
        }
      }

      VectorSchemaRoot[] ret = new VectorSchemaRoot[size];
      for (int i = 0; i < size; i ++) {
        ret[i] = new VectorSchemaRoot(container.get(i));
      }
      return func.apply(ret);
    });
  }

  private SinkContext loadArrowFiles(SinkContext context, BufferAllocator allocator, Function<List<VectorSchemaRoot[]>, SinkContext> func) {
    List<VectorSchemaRoot[]> params = Arrays.stream(this.fields).mapToObj(index -> {
      Field field = this.schema.getFields().get(index);
      File arrowFile = new File(context.getPartition(), field.getName().toUpperCase() + ".arrow");
      String arrowFilePath = arrowFile.getAbsolutePath();
      if (!arrowFile.exists()) {
        throw new IllegalStateException("Failed to read arrow file: " + arrowFilePath);
      }

      try {
        return load(arrowFilePath, allocator);
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }).collect(Collectors.toList());
    return func.apply(params);
  }

  public VectorSchemaRoot[] load(String path, BufferAllocator allocator) throws IOException {
    log.debug("path: " + path);

    byte[] bytes = Files.readAllBytes(FileSystems.getDefault().getPath(path));
    SeekableReadChannel channel = new SeekableReadChannel(new ByteArrayReadableSeekableByteChannel(bytes));
    this.reader = new ArrowFileReader(channel, allocator);
    List<VectorSchemaRoot> list = reader.getRecordBlocks().stream().map(block -> {
      try {
        if (!reader.loadRecordBatch(block)) {
          throw new IllegalStateException("Failed to load RecordBatch");
        }
        return reader.getVectorSchemaRoot();
      } catch (IOException e) {
        throw new IllegalStateException(e);
      }
    }).collect(Collectors.toList());
    VectorSchemaRoot[] vectorSchemaRoots = new VectorSchemaRoot[list.size()];
    list.toArray(vectorSchemaRoots);
    return vectorSchemaRoots;
  }
}
