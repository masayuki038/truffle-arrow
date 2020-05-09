package net.wrap_trap.truffle_arrow;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampSecTZVector;
import org.apache.arrow.vector.TimeSecVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;
import org.apache.arrow.vector.util.Text;

import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.sql.*;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;


public class TestUtils {

  public static void generateTestFile(String path) throws IOException {
    RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    FieldVector intVector = createIntVector(10, allocator);
    FieldVector bigIntVector = createBigIntVector(10, allocator);
    FieldVector varCharVector = createVarCharVector(10, allocator);
    FieldVector timestampVector = createTimestampVector(10, allocator);
    FieldVector timeVector = createTimeVector(10, allocator);
    FieldVector dateVector = createDateVector(10, allocator);
    FieldVector doubleVector = createDoubleVector(10, allocator);

    VectorSchemaRoot root = new VectorSchemaRoot(
      Arrays.asList(
        intVector.getField(),
        bigIntVector.getField(),
        varCharVector.getField(),
        timestampVector.getField(),
        timeVector.getField(),
        dateVector.getField(),
        doubleVector.getField()),
      Arrays.asList(
        intVector,
        bigIntVector,
        varCharVector,
        timestampVector,
        timeVector,
        dateVector,
        doubleVector),
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

  private static FieldVector createTimestampVector(int size, BufferAllocator allocator) {
    // TODO create TimeStampSecTZVector instead of TiemsStampMilliTZVector
    //  because timestamp literal of Calcite generate java.time.Instant that has only seconds
    TimeStampSecTZVector vector = new TimeStampSecTZVector("F_TIMESTAMP", allocator, "GMT");
    vector.allocateNew();
    vector.setValueCount(size);
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("GMT")));
    calendar.set(2020, 4, 4, 13, 48, 11);
    calendar.set(Calendar.MILLISECOND, 0);
    long offset = calendar.getTimeInMillis();
    for (int i = 0; i < size; i ++) {
      vector.set(i, offset + i * 60 * 60 * 1000);
    }
    return vector;
  }

  private static FieldVector createTimeVector(int size, BufferAllocator allocator) {
    TimeSecVector vector = new TimeSecVector("F_TIME", allocator);
    vector.allocateNew();
    vector.setValueCount(size);
    int offset = 4823; // 01:20:23
    for (int i = 0; i < size; i ++) {
      vector.set(i, offset + i * 60 * 60);
    }
    return vector;
  }

  private static FieldVector createDateVector(int size, BufferAllocator allocator) {
    DateDayVector vector = new DateDayVector("F_DATE", allocator);
    vector.allocateNew();
    vector.setValueCount(size);
    int offset = 18385; // 2020-05-03
    for (int i = 0; i < size; i ++) {
      vector.set(i, offset + i);
    }
    return vector;
  }

  private static FieldVector createDecimalVector(int size, BufferAllocator allocator) {
    DecimalVector vector = new DecimalVector("F_DECIMAL", allocator, 18, 8);
    vector.allocateNew();
    vector.setValueCount(size);
    BigDecimal offset = new BigDecimal("1234567890.12345678");
    for (int i = 0; i < size; i ++) {
      vector.set(i, offset.add(new BigDecimal("999.999")));
    }
    return vector;
  }

  private static FieldVector createFloatVector(int size, BufferAllocator allocator) {
    Float4Vector vector = new Float4Vector("F_FLOAT", allocator);
    vector.allocateNew();
    vector.setValueCount(size);
    float offset = 123.456f;
    for (int i = 0; i < size; i ++) {
      vector.set(i, offset + i);
    }
    return vector;
  }

  private static FieldVector createDoubleVector(int size, BufferAllocator allocator) {
    Float8Vector vector = new Float8Vector("F_DOUBLE", allocator);
    vector.allocateNew();
    vector.setValueCount(size);
    double offset = 123.456d;
    for (int i = 0; i < size; i ++) {
      vector.set(i, offset + i);
    }
    return vector;
  }

  public static void filterTest(String sql, int size, String expectedFirstRow) throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(sql);
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(size));
      assertThat(results.get(0), is(expectedFirstRow));
      assertThat(LastPlan.INSTANCE.includes(ArrowFilter.class), is(true));
    }
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
