package net.wrap_trap.truffle_arrow;

import com.google.common.collect.ImmutableList;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.FrameSlotKind;
import com.oracle.truffle.api.frame.VirtualFrame;
import net.wrap_trap.truffle_arrow.truffle.FrameDescriptorPart;
import net.wrap_trap.truffle_arrow.truffle.SqlNull;
import net.wrap_trap.truffle_arrow.type.ArrowTimeSec;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.SeekableReadChannel;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.ByteArrayReadableSeekableByteChannel;
import org.apache.arrow.vector.util.Text;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;


public class ArrowUtils {

  private static final RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);

  public static UInt4Vector createSelectionVector() {
    return new UInt4Vector("selectionVector", allocator);
  }

  public static VectorSchemaRoot[] load(String path) throws IOException {
    byte[] bytes = Files.readAllBytes(FileSystems.getDefault().getPath(path));
    SeekableReadChannel channel = new SeekableReadChannel(new ByteArrayReadableSeekableByteChannel(bytes));
    ArrowFileReader reader = new ArrowFileReader(channel, allocator);
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

  public static VectorSchemaRoot createVectorSchemaRoot(FrameDescriptorPart framePart) {
    ImmutableList.Builder<FieldVector> builder = ImmutableList.builder();

    framePart.getFrameSlots().stream().forEach(slot -> {
      FieldVector fieldVector;
      String name = slot.getIdentifier().toString();
      FrameSlotKind kind = framePart.frame().getFrameSlotKind(slot);
      switch (kind) {
        case Int:
          fieldVector =  new IntVector(name, allocator);
          break;
        case Long:
          fieldVector = new BigIntVector(name, allocator);
          break;
        case Double:
          fieldVector = new Float8Vector(name, allocator);
          break;
        case Object:
          fieldVector = new VarCharVector(name, allocator);
          break;
        default:
          throw new IllegalArgumentException("Unexpected FrameSlotKind: " + kind);
      }
      fieldVector.allocateNew();
      builder.add(fieldVector);
    });
    List<FieldVector> fieldVectors = builder.build();
    List<Field> fields = fieldVectors.stream().map(fieldVector -> fieldVector.getField()).collect(Collectors.toList());
    return new VectorSchemaRoot(fields, fieldVectors);
  }

  public static void setValues(VirtualFrame virtualFrame, FrameDescriptorPart framePart, VectorSchemaRoot vectorSchemaRoot, int index) {
    List<FrameSlot> slots = framePart.getFrameSlots();
    for (int i = 0; i < slots.size(); i ++) {
      FrameSlot slot = slots.get(i);
      FieldVector fieldVector = vectorSchemaRoot.getFieldVectors().get(i);
      ArrowFieldType type = ArrowFieldType.of(fieldVector.getField().getFieldType().getType());
      Object value = virtualFrame.getValue(slot);
      switch (type) {
        case INT:
        case DATE:
        case TIME:
          IntVector intVector = (IntVector) fieldVector;
          if (!(value instanceof SqlNull)) {
            intVector.set(index, (int) value);
          } else {
            intVector.setNull(index);
          }
          break;
        case LONG:
        case TIMESTAMP:
          BigIntVector bigIntVector = (BigIntVector) fieldVector;
          if (!(value instanceof SqlNull)) {
            bigIntVector.set(index, (long) value);
          } else {
            bigIntVector.setNull(index);
          }
          break;
        case DOUBLE:
          Float8Vector float8Vector = (Float8Vector) fieldVector;
          if (!(value instanceof SqlNull)) {
            float8Vector.set(index, (double) value);
          } else {
            float8Vector.setNull(index);
          }
          break;
        case STRING:
          VarCharVector varCharVector = (VarCharVector) fieldVector;
          if (!(value instanceof SqlNull)) {
            varCharVector.set(index, new Text((String) value));
          } else {
            varCharVector.setNull(index);
          }
          break;
        default:
          throw new IllegalArgumentException("Unexpected ArrowFieldType: " + type);
      }
    }
  }

  public static FrameSlotKind getFrameSlotKind(RelDataTypeField field) {
    String typeName = field.getType().getSqlTypeName().getName();
    switch (typeName) {
      case "INTEGER":
        return FrameSlotKind.Float.Int;
      case "BIGINT":
        return FrameSlotKind.Long;
      default:
        throw new IllegalArgumentException(typeName);
    }
  }

  public static Object getValue(Object o) {
    if (o == null || o == SqlNull.INSTANCE) {
      return SqlNull.INSTANCE;
    } else if (o instanceof Text) {
      return o.toString();
    } else if (o instanceof ArrowTimeSec) {
      return ((ArrowTimeSec) o).timeSec() * 1000;
    }
    return o;
  }
}
