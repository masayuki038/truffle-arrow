package net.wrap_trap.truffle_arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.util.Text;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;


public class TestUtils {

  public static void generateTestFile(String path) throws IOException {
    RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    FieldVector intVector = createIntVector(10, allocator);
    FieldVector bigIntVector = createBigIntVector(10, allocator);
    FieldVector varCharVector = createVarCharVector(10, allocator);

    VectorSchemaRoot root = new VectorSchemaRoot(
      Arrays.asList(intVector.getField(), bigIntVector.getField(), varCharVector.getField()),
      Arrays.asList(intVector, bigIntVector, varCharVector),
      10);

    try (FileOutputStream out = new FileOutputStream(path)) {
      try (ArrowWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(out))) {
        writer.writeBatch();
      }
    }
  }

  private static FieldVector createIntVector(int size, BufferAllocator allocator) {
    IntVector vector = new IntVector("F_INT", allocator);
    vector.allocateNew();
    vector.setValueCount(size);
    for (int i = 0; i < size; i ++) {
      vector.set(i, i);
    }
    return vector;
  }

  private static FieldVector createBigIntVector(int size, BufferAllocator allocator) {
    BigIntVector vector = new BigIntVector("F_BIGINT", allocator);
    vector.allocateNew();
    vector.setValueCount(size);
    for (int i = 0; i < size; i ++) {
      vector.set(i, i);
    }
    return vector;
  }

  private static FieldVector createVarCharVector(int size, BufferAllocator allocator) {
    VarCharVector vector = new VarCharVector("F_VARCHAR", allocator);
    vector.allocateNew();
    vector.setValueCount(size);
    for (int i = 0; i < size; i ++) {
      vector.set(i, new Text("test" + i));
    }
    return vector;
  }

  public static void dumpMetadata(ResultSet rs) throws SQLException {
    ResultSetMetaData meta = rs.getMetaData();
    for (int i = 0; i < meta.getColumnCount(); i ++) {
      System.out.println(meta.getColumnName(i + 1) + "\t" + meta.getColumnTypeName(i + 1));
    }
  }

  public static void dumpResults(ResultSet rs) {
    System.out.println(getResults(rs));
  }

  public static List<String> getResults(ResultSet rs) {
    return new ResultSetSpliterator<>(rs, r -> getResultAsList(r)
       .stream().map(o -> o == null ? "" : o.toString()).collect(Collectors.joining("\t")))
       .stream().collect(Collectors.toList());
  }

  private static List<Object> getResultAsList(ResultSet rs) throws SQLException {
    List<Object> ret = new ArrayList<>();
    for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
      ret.add(rs.getObject(i + 1));
    }
    return ret;
  }
}
