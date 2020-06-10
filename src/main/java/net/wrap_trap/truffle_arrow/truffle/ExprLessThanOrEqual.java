package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;
import org.apache.arrow.vector.util.Text;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

@NodeInfo(shortName = "<=")
abstract public class ExprLessThanOrEqual extends ExprBinary {

  @Specialization
  protected boolean le(int left, int right) {
    return left <= right;
  }

  @Specialization
  protected boolean le(long left, long right) {
    return left <= right;
  }

  @Specialization
  protected boolean le(long left, int right) {
    return left <= right;
  }

  @Specialization
  protected boolean le(int left, long right) {
    return left <= right;
  }

  @Specialization
  protected boolean le(double left, double right) {
    return left <= right;
  }

  @Specialization
  protected boolean le(double left, BigDecimal right) {
    return left <= right.doubleValue();
  }

  @Specialization
  protected boolean le(BigDecimal left, double right) {
    return left.doubleValue() <= right;
  }

  @Specialization
  protected SqlNull le(Object left, SqlNull right) {
    return SqlNull.INSTANCE;
  }

  @Specialization
  protected boolean le(Text left, String right) {
    return left.toString().compareTo(right) <= 0;
  }

  @Specialization
  protected boolean le(String left, Text right) {
    return left.compareTo(right.toString()) <= 0;
  }

  @Specialization
  protected boolean le(String left, String right) {
    return left.compareTo(right) <= 0;
  }

  @Specialization
  protected boolean le(Long left, Instant right) {
    return left.compareTo(right.toEpochMilli()) <= 0;
  }

  @Specialization
  protected boolean le(Instant left, Long right) {
    return Long.valueOf(left.toEpochMilli()).compareTo(right) <= 0;
  }

  @Specialization
  protected boolean le(Integer left, LocalTime right) {
    return left.compareTo(right.toSecondOfDay()) <= 0;
  }

  @Specialization
  protected boolean le(LocalTime left, Integer right) {
    return Integer.valueOf(left.toSecondOfDay()).compareTo(right) <= 0;
  }

  @Specialization
  protected boolean le(Integer left, LocalDate right) {
    return left.compareTo(Long.valueOf(right.toEpochDay()).intValue()) <= 0;
  }

  @Specialization
  protected boolean le(LocalDate left, Integer right) {
    return Integer.valueOf((int) left.toEpochDay()).compareTo(right) <= 0;
  }
}
