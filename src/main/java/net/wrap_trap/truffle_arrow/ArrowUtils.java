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
    framePart.getRelDataType().getFieldList().stream().forEach(relDataTypeField -> {
      FieldVector fieldVector;
      String name = relDataTypeField.getName();
      String type = relDataTypeField.getType().getSqlTypeName().getName();
      switch (type) { // Time も Int になってしまう。RelDataTypeを見て、生成する FieldVector を細かく制御するべき？
        case "INTEGER":
          fieldVector = new IntVector(name, allocator);
          break;
        case "DATE":
          fieldVector = new DateDayVector(name, allocator);
          break;
        case "TIME":
          fieldVector = new TimeSecVector(name, allocator);
          break;
        case "BIGINT":
          fieldVector = new BigIntVector(name, allocator);
          break;
        case "TIMESTAMP":
          fieldVector = new TimeStampSecTZVector(name, allocator, "GMT");
          break;
        case "DOUBLE":
          fieldVector = new Float8Vector(name, allocator);
          break;
        case "VARCHAR":
          fieldVector = new VarCharVector(name, allocator);
          break;
        default:
          throw new IllegalArgumentException("Unexpected RelDataFieldType: " + type);
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
          IntVector intVector = (IntVector) fieldVector;
          if (!(value instanceof SqlNull)) {
            intVector.set(index, (int) value);
          } else {
            intVector.setNull(index);
          }
          break;
        case DATE:
          DateDayVector dateDayVector = (DateDayVector) fieldVector;
          if (!(value instanceof SqlNull)) {
            dateDayVector.set(index, (int) value);
          } else {
            dateDayVector.setNull(index);
          }
          break;
        case TIME:
          TimeSecVector timeSecVector = (TimeSecVector) fieldVector;
          if (!(value instanceof SqlNull)) {
            timeSecVector.set(index, ((ArrowTimeSec) value).timeSec());
          } else {
            timeSecVector.setNull(index);
          }
          break;
        case TIMESTAMP:
          TimeStampSecTZVector timezoneVector = (TimeStampSecTZVector) fieldVector;
          if (!(value instanceof SqlNull)) {
            timezoneVector.set(index, (long) value);
          } else {
            timezoneVector.setNull(index);
          }
          break;
        case LONG:
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
            varCharVector.set(index, (Text) value);
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
      case "DATE":
      case "TIME":
        return FrameSlotKind.Float.Int;
      case "BIGINT":
      case "TIMESTAMP":
        return FrameSlotKind.Long;
      case "DOUBLE":
        return FrameSlotKind.Double;
      case "VARCHAR":
        return FrameSlotKind.Object;
      default:
        throw new IllegalArgumentException(typeName);
    }
  }
}
