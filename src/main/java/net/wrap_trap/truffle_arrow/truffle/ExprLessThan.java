package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;
import org.apache.arrow.vector.util.Text;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

@NodeInfo(shortName = "<")
abstract public class ExprLessThan extends ExprBinary {

  @Specialization
  protected boolean lt(int left, int right) {
    return left < right;
  }

  @Specialization
  protected boolean lt(long left, long right) {
    return left < right;
  }

  @Specialization
  protected boolean lt(long left, int right) {
    return left < right;
  }

  @Specialization
  protected boolean lt(int left, long right) {
    return left < right;
  }

  @Specialization
  protected boolean lt(double left, double right) {
    return left < right;
  }

  @Specialization
  protected boolean lt(double left, BigDecimal right) {
    return left < right.doubleValue();
  }

  @Specialization
  protected boolean lt(BigDecimal left, double right) {
    return left.doubleValue() < right;
  }

  @Specialization
  protected SqlNull lt(Object left, SqlNull right) {
    return SqlNull.INSTANCE;
  }

  @Specialization
  protected boolean lt(Text left, String right) {
    return left.toString().compareTo(right) < 0;
  }

  @Specialization
  protected boolean lt(String left, Text right) {
    return left.compareTo(right.toString()) < 0;
  }

  @Specialization
  protected boolean lt(String left, String right) {
    return left.compareTo(right) < 0;
  }

  @Specialization
  protected boolean lt(Long left, Instant right) {
    return left.compareTo(right.toEpochMilli()) < 0;
  }

  @Specialization
  protected boolean lt(Instant left, Long right) {
    return Long.valueOf(left.toEpochMilli()).compareTo(right) < 0;
  }

  @Specialization
  protected boolean lt(Integer left, LocalTime right) {
    return left.compareTo(right.toSecondOfDay()) < 0;
  }

  @Specialization
  protected boolean lt(LocalTime left, Integer right) {
    return Integer.valueOf(left.toSecondOfDay()).compareTo(right) < 0;
  }

  @Specialization
  protected boolean lt(Integer left, LocalDate right) {
    return left.compareTo(Long.valueOf(right.toEpochDay()).intValue()) < 0;
  }

  @Specialization
  protected boolean lt(LocalDate left, Integer right) {
    return Integer.valueOf((int) left.toEpochDay()).compareTo(right) < 0;
  }
}