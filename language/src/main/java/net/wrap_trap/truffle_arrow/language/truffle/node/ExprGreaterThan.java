package net.wrap_trap.truffle_arrow.language.truffle.node;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.dsl.Specialization;
import net.wrap_trap.truffle_arrow.language.truffle.node.type.ArrowTimeSec;
import org.apache.arrow.vector.util.Text;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;

abstract public class ExprGreaterThan extends ExprBinary {

  @Specialization
  protected boolean gt(int left, int right) {
    return left > right;
  }

  @Specialization
  protected boolean gt(long left, long right) {
    return left > right;
  }

  @Specialization
  protected boolean gt(long left, int right) {
    return left > right;
  }

  @Specialization
  protected boolean gt(int left, long right) {
    return left > right;
  }

  @Specialization
  protected boolean gt(double left, double right) {
    return left > right;
  }

  @Specialization
  protected boolean gt(double left, BigDecimal right) {
    return left > right.doubleValue();
  }

  @Specialization
  protected boolean gt(BigDecimal left, double right) {
    return left.doubleValue() > right;
  }

  @Specialization
  protected boolean gt(Object left, SqlNull right) {
    return false;
  }

  @Specialization
  protected boolean gt(SqlNull left, Object right) {
    return false;
  }

  @Specialization
  protected boolean gt(Text left, Text right) {
    return left.toString().compareTo(right.toString()) > 0;
  }

  @Specialization
  protected boolean gt(Text left, String right) {
    return left.toString().compareTo(right) > 0;
  }

  @Specialization
  protected boolean gt(String left, Text right) {
    return left.compareTo(right.toString()) > 0;
  }

  @Specialization
  protected boolean gt(String left, String right) {
    return left.compareTo(right) > 0;
  }

  @Specialization
  protected boolean gt(Text left, Object right) {
    return gt(left.toString(), right);
  }

  @Specialization
  protected boolean gt(Object left, Text right) {
    return gt(left, right.toString());
  }

  @Specialization
  protected boolean gt(Long left, Instant right) {
    return left.compareTo(right.toEpochMilli()) > 0;
  }

  @Specialization
  protected boolean gt(Instant left, Long right) {
    return Long.valueOf(left.toEpochMilli()).compareTo(right) > 0;
  }

  @Specialization
  protected boolean gt(ArrowTimeSec left, LocalTime right) {
    return left.timeSec().compareTo(right.toSecondOfDay()) > 0;
  }

  @Specialization
  protected boolean gt(LocalTime left, ArrowTimeSec right) {
    return Integer.valueOf(left.toSecondOfDay()).compareTo(right.timeSec()) > 0;
  }

  @Specialization
  protected boolean gt(ArrowTimeSec left, ArrowTimeSec right) {
    return left.timeSec().compareTo(right.timeSec()) > 0;
  }

  @Specialization
  protected boolean gt(Integer left, LocalDate right) {
    return left.compareTo(Long.valueOf(right.toEpochDay()).intValue()) > 0;
  }

  @Specialization
  protected boolean gt(LocalDate left, Integer right) {
    return Integer.valueOf((int) left.toEpochDay()).compareTo(right) > 0;
  }

  @Specialization
  @CompilerDirectives.TruffleBoundary
  protected boolean gt(Object left, Object right) {
    return ((Comparable) left).compareTo(right) > 0;
  }
}
