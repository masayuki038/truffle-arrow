package net.wrap_trap.truffle_arrow;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.Types.MinorType;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

import java.util.HashMap;
import java.util.Map;

/**
 * Field Type for Apache Arrow
 */
enum ArrowFieldType {
  STRING(String.class),
  BOOLEAN(Primitive.BOOLEAN),
  BYTE(Primitive.BYTE),
  CHAR(Primitive.CHAR),
  SHORT(Primitive.SHORT),
  INT(Primitive.INT),
  LONG(Primitive.LONG),
  FLOAT(Primitive.FLOAT),
  DOUBLE(Primitive.DOUBLE),
  DECIMAL(java.math.BigDecimal.class, 18, 8),
  DATE(java.sql.Date.class),
  TIME(java.sql.Time.class),
  TIMESTAMP(java.sql.Timestamp.class),
  BYTEARRAY(byte[].class);

  private final Class clazz;
  private int precision;
  private int scale;
  private static final Map<MinorType, ArrowFieldType> MAP = new HashMap<>();

  static {
    MAP.put(MinorType.VARCHAR, STRING);
    MAP.put(MinorType.BIT, BOOLEAN);
    MAP.put(MinorType.INT, INT);
    MAP.put(MinorType.BIGINT, LONG);
    MAP.put(MinorType.FLOAT4, FLOAT);
    MAP.put(MinorType.FLOAT8, DOUBLE);
    MAP.put(MinorType.DECIMAL, DECIMAL);
    MAP.put(MinorType.DATEDAY, DATE);
    MAP.put(MinorType.TIMESEC, TIME);
    MAP.put(MinorType.TIMESTAMPSECTZ, TIMESTAMP);
    MAP.put(MinorType.TIMESTAMPMILLITZ, TIMESTAMP);
    MAP.put(MinorType.VARBINARY, BYTEARRAY);
  }

  ArrowFieldType(Primitive primitive) {
    this(primitive.boxClass);
  }

  ArrowFieldType(Class clazz) {
    this.clazz = clazz;
  }

  ArrowFieldType(Class clazz, int precision, int scale) {
    this.clazz = clazz;
    this.precision = precision;
    this.scale = scale;
  }

  public RelDataType toType(JavaTypeFactory typeFactory) {
    RelDataType javaType = typeFactory.createJavaType(clazz);
    RelDataType sqlType;
    if (precision > 0) {
      sqlType = typeFactory.createSqlType(javaType.getSqlTypeName(), precision, scale);
    } else {
      sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
    }
    return typeFactory.createTypeWithNullability(sqlType, true);
  }

  public static ArrowFieldType of(ArrowType arrowType) {
    MinorType minorType = Types.getMinorTypeForArrowType(arrowType);
    return MAP.get(minorType);
  }
}