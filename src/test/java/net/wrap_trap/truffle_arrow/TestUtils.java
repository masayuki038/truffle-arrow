package net.wrap_trap.truffle_arrow;

import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.channels.Channels;
import java.sql.*;
import java.time.ZoneId;
import java.util.*;
import java.util.stream.Collectors;

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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;


public class TestUtils {

  public static void generateTestFile(String path) throws IOException {
    generateTestFile(path, TestDataType.CASE1);
  }

  public static void generateTestFile(String path, TestDataType dataType) throws IOException {
    RootAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
    FieldVector intVector, bigIntVector, varCharVector, timestampVector, timeVector, dateVector, doubleVector;

    Calendar c20200504134811 = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("GMT")));
    c20200504134811.set(2020, 4, 4, 13, 48, 11);
    c20200504134811.set(Calendar.MILLISECOND, 0);

    Calendar c20200503000000 = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.of("GMT")));
    c20200503000000.set(2020, 4, 3, 0, 0, 0);
    c20200503000000.set(Calendar.MILLISECOND, 0);

    long timestampIntervalByHour = 60 * 60 * 1000L;
    long timestampIntervalByDay = 24 * 60 * 60 * 1000L;

    int timeOffset = 4823; // 01:20:23
    int timeInterval = 60 * 60;

    int dateOffset = 18385; // 2020-05-03

    switch(dataType) {
      case CASE1:
        intVector = createIntVector(10, 0, 1, -1, allocator);
        bigIntVector = createBigIntVector(10, 0, 1, -1, allocator);
        varCharVector = createVarCharVector("test", 10, 0, 1, -1, allocator);
        timestampVector = createTimestampVector(10, c20200504134811, timestampIntervalByHour, -1, allocator);
        timeVector = createTimeVector(10, timeOffset, timeInterval, -1, allocator);
        dateVector = createDateVector(10, dateOffset, 1, -1, allocator);
        doubleVector = createDoubleVector(10,123.456d, 1, -1, allocator);
        break;

      case CASE2: // null values
        intVector = createIntVector(10, 0, 1, 1, allocator);
        bigIntVector = createBigIntVector(10, 0, 1, 2, allocator);
        varCharVector = createVarCharVector("test", 10, 0, 1, 3, allocator);
        timestampVector = createTimestampVector(10, c20200504134811, timestampIntervalByHour, 4, allocator);
        timeVector = createTimeVector(10, timeOffset, timeInterval, 5, allocator);
        dateVector = createDateVector(10, dateOffset, 1, 6, allocator);
        doubleVector = createDoubleVector(10, 123.456d, 1, 7, allocator);
        break;

      case CASE3:
        intVector = createIntVector(10, 0, 1, -1, allocator);
        bigIntVector = createBigIntVector(10, 0, 1, -1, allocator);
        varCharVector = createVarCharVector("", 10, 0, 1, -1, allocator);
        timestampVector = createTimestampVector(10, c20200503000000, timestampIntervalByDay, -1, allocator);
        timeVector = createTimeVector(10, timeOffset, timeInterval, -1, allocator);
        dateVector = createDateVector(10, dateOffset, 1, -1, allocator);
        doubleVector = createDoubleVector(10, 0d, 1, -1, allocator);
        break;

      case CASE4:
        intVector = createIntVector(10, 0, 1, -1, allocator);
        bigIntVector = createBigIntVector(10, 10, -1, -1, allocator);
        varCharVector = createVarCharVector("", 10, 0, 1, -1, allocator);
        timestampVector = createTimestampVector(10, c20200503000000, timestampIntervalByDay, -1, allocator);
        timeVector = createTimeVector(10, timeOffset, timeInterval, -1, allocator);
        dateVector = createDateVector(10, dateOffset, 1, -1, allocator);
        doubleVector = createDoubleVector(10, 10d, -1, -1, allocator);
        break;

      case CASE5:
        intVector = createIntVector(10, 0, 1, -1, allocator);
        bigIntVector = createBigIntVector(5, 0, 1, -1, allocator);
        merge(bigIntVector, createBigIntVector(5, 0, 1, -1, allocator));
        varCharVector = createVarCharVector("test", 5, 0, 1, -1, allocator);
        merge(varCharVector, createVarCharVector("test", 5, 0, 1, -1, allocator));
        timestampVector = createTimestampVector(5, c20200504134811, timestampIntervalByDay, -1, allocator);
        merge(timestampVector, createTimestampVector(5, c20200504134811, timestampIntervalByDay, -1, allocator));
        timeVector = createTimeVector(5, timeOffset, timeInterval, -1, allocator);
        merge(timeVector, createTimeVector(5, timeOffset, timeInterval, -1, allocator));
        dateVector = createDateVector(5, dateOffset, 1, -1, allocator);
        merge(dateVector, createDateVector(5, dateOffset, 1, -1, allocator));
        doubleVector = createDoubleVector(5, 123.456d, 1, -1, allocator);
        merge(doubleVector, createDoubleVector(5, 123.456d, 1, -1, allocator));
        break;

      default:
        throw new IllegalArgumentException("Invalid Type:" + dataType);
    }

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

  private static FieldVector createIntVector(int size, int offset, int step, int nullIndex, BufferAllocator allocator) {
    IntVector vector = new IntVector("F_INT", allocator);
    vector.allocateNew();
    vector.setValueCount(size);
    for (int i = 0; i < size; i ++) {
      if (i == nullIndex) {
        vector.setNull(i);
      } else {
        vector.set(i, offset + (i * step));
      }
    }
    return vector;
  }

  private static void merge(FieldVector mergeTo, FieldVector mergeFrom) {
    int baseSize = mergeTo.getValueCount();
    mergeTo.setValueCount(baseSize + mergeFrom.getValueCount());
    for (int i = 0; i < mergeFrom.getValueCount(); i ++) {
      mergeTo.copyFrom(i, baseSize + i, mergeFrom);
    }
  }

  private static FieldVector createBigIntVector(int size, int offset, int step, int nullIndex, BufferAllocator allocator) {
    BigIntVector vector = new BigIntVector("F_BIGINT", allocator);
    vector.allocateNew();
    vector.setValueCount(size);
    for (int i = 0; i < size; i ++) {
      if (i == nullIndex) {
        vector.setNull(i);
      } else {
        vector.set(i, offset + (i * step));
      }
    }
    return vector;
  }

  private static FieldVector createVarCharVector(String prefix, int size, int offset, int step, int nullIndex, BufferAllocator allocator) {
    VarCharVector vector = new VarCharVector("F_VARCHAR", allocator);
    vector.allocateNew();
    vector.setValueCount(size);
    for (int i = 0; i < size; i ++) {
      if (i == nullIndex) {
        vector.setNull(i);
      } else {
        vector.set(i, new Text(prefix + (offset + (i * step))));
      }
    }
    return vector;
  }

  private static FieldVector createTimestampVector(int size, Calendar offset, long interval, int nullIndex, BufferAllocator allocator) {
    // TODO create TimeStampSecTZVector instead of TiemsStampMilliTZVector
    //  because timestamp literal of Calcite generate java.time.Instant that has only seconds
    TimeStampSecTZVector vector = new TimeStampSecTZVector("F_TIMESTAMP", allocator, "GMT");
    vector.allocateNew();
    vector.setValueCount(size);
    long offsetMillis = offset.getTimeInMillis();
    for (int i = 0; i < size; i ++) {
      if (i == nullIndex) {
        vector.setNull(i);
      } else {
        vector.set(i, offsetMillis + i * interval);
      }
    }
    return vector;
  }

  private static FieldVector createTimeVector(int size, int offset, int interval, int nullIndex, BufferAllocator allocator) {
    TimeSecVector vector = new TimeSecVector("F_TIME", allocator);
    vector.allocateNew();
    vector.setValueCount(size);

    for (int i = 0; i < size; i ++) {
      if (i == nullIndex) {
        vector.setNull(i);
      } else {
        vector.set(i, offset + i * interval);
      }
    }
    return vector;
  }

  private static FieldVector createDateVector(int size, int offset, int interval, int nullIndex, BufferAllocator allocator) {
    DateDayVector vector = new DateDayVector("F_DATE", allocator);
    vector.allocateNew();
    vector.setValueCount(size);

    for (int i = 0; i < size; i ++) {
      if (i == nullIndex) {
        vector.setNull(i);
      } else {
        vector.set(i, offset + (i * interval));
      }
    }
    return vector;
  }

  private static FieldVector createDecimalVector(int size, int nullIndex, BufferAllocator allocator) {
    DecimalVector vector = new DecimalVector("F_DECIMAL", allocator, 18, 8);
    vector.allocateNew();
    vector.setValueCount(size);
    BigDecimal offset = new BigDecimal("1234567890.12345678");
    for (int i = 0; i < size; i ++) {
      if (i == nullIndex) {
        vector.setNull(i);
      } else {
        vector.set(i, offset.add(new BigDecimal("999.999")));
      }
    }
    return vector;
  }

  private static FieldVector createFloatVector(int size, int nullIndex, BufferAllocator allocator) {
    Float4Vector vector = new Float4Vector("F_FLOAT", allocator);
    vector.allocateNew();
    vector.setValueCount(size);
    float offset = 123.456f;
    for (int i = 0; i < size; i ++) {
      if (i == nullIndex) {
        vector.setNull(i);
      } else {
        vector.set(i, offset + i);
      }
    }
    return vector;
  }

  private static FieldVector createDoubleVector(int size, double offset, int step, int nullIndex, BufferAllocator allocator) {
    Float8Vector vector = new Float8Vector("F_DOUBLE", allocator);
    vector.allocateNew();
    vector.setValueCount(size);
    for (int i = 0; i < size; i ++) {
      if (i == nullIndex) {
        vector.setNull(i);
      } else {
        vector.set(i, offset + (i * step));
      }
    }
    return vector;
  }

  public static void filterTest(String sql, int size) throws SQLException {
    filterTest(sql, size, null);
  }

  public static void filterTest(String sql, int size, String expectedFirstRow) throws SQLException {
    try (
      Connection conn = DriverManager.getConnection("jdbc:truffle:");
      PreparedStatement pstmt = conn.prepareStatement(sql);
      ResultSet rs = pstmt.executeQuery()
    ) {
      List<String> results = TestUtils.getResults(rs);
      assertThat(results.size(), is(size));
      if (expectedFirstRow != null) {
        assertThat(results.get(0), is(expectedFirstRow));
      }
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
       .stream().map(o -> o == null ? "_NULL_" : o.toString()).collect(Collectors.joining("\t")))
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
