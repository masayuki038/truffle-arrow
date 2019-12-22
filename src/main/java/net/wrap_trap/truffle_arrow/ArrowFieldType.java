package net.wrap_trap.truffle_arrow;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

import java.util.HashMap;
import java.util.Map;

/**
 * Field Type for Apache Arrow
 */
enum ArrowFieldType {
  STRING(String.class, "string"),
  BOOLEAN(Primitive.BOOLEAN),
  BYTE(Primitive.BYTE),
  CHAR(Primitive.CHAR),
  SHORT(Primitive.SHORT),
  INT(Primitive.INT),
  LONG(Primitive.LONG),
  FLOAT(Primitive.FLOAT),
  DOUBLE(Primitive.DOUBLE),
  DATE(java.sql.Date.class, "date"),
  TIME(java.sql.Time.class, "time"),
  TIMESTAMP(java.sql.Timestamp.class, "timestamp");

  private final Class clazz;
  private final String simpleName;

  private static final Map<Class<? extends ArrowType>, ArrowFieldType> MAP = new HashMap<>();

  static {
    MAP.put(ArrowType.Utf8.class, STRING);
    MAP.put(ArrowType.Bool.class, BOOLEAN);
    MAP.put(ArrowType.Int.class, LONG);
    MAP.put(ArrowType.FloatingPoint.class, FLOAT);
    MAP.put(ArrowType.Date.class, DATE);
    MAP.put(ArrowType.Time.class, TIME);
    MAP.put(ArrowType.Timestamp.class, TIMESTAMP);
  }

  ArrowFieldType(Primitive primitive) {
    this(primitive.boxClass, primitive.primitiveClass.getSimpleName());
  }

  ArrowFieldType(Class clazz, String simpleName) {
    this.clazz = clazz;
    this.simpleName = simpleName;
  }

  public RelDataType toType(JavaTypeFactory typeFactory) {
    RelDataType javaType = typeFactory.createJavaType(clazz);
    RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
    return typeFactory.createTypeWithNullability(sqlType, true);
  }

  public static ArrowFieldType of(ArrowType arrowType) {
    return MAP.get(arrowType.getClass());
  }
}