package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;
import org.apache.arrow.vector.util.Text;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Objects;

@NodeInfo(shortName = "=")
abstract class ExprEquals extends ExprBinary {

  @Specialization
  protected boolean eq(boolean left, boolean right) {
    return left == right;
  }

  @Specialization
  protected boolean eq(int left, int right) {
    return left == right;
  }

  @Specialization
  protected boolean eq(long left, long right) {
    return left == right;
  }

  @Specialization
  protected boolean eq(double left, double right) {
    return left == right;
  }

  @Specialization
  protected boolean ge(Object left, SqlNull right) {
    return (left == SqlNull.INSTANCE);
  }

  @Specialization
  protected boolean ge(SqlNull left, Object right) {
    return (right == SqlNull.INSTANCE);
  }

  @Specialization
  protected boolean eq(Text left, Text right) {
    return Objects.equals(left.toString(), right.toString());
  }

  @Specialization
  protected boolean eq(Text left, String right) {
    return Objects.equals(left.toString(), right);
  }

  @Specialization
  protected boolean eq(String left, Text right) {
    return Objects.equals(left, right.toString());
  }

  @Specialization
  protected boolean eq(String left, String right) {
    return Objects.equals(left, right);
  }

  @Specialization
  protected boolean eq(Text left, Object right) {
    return eq(left.toString(), right);
  }

  @Specialization
  protected boolean eq(Object left, Text right) {
    return eq(left, right.toString());
  }

  @Specialization
  protected boolean eq(Long left, Instant right) {
    return Objects.equals(left, right.toEpochMilli());
  }

  @Specialization
  protected boolean eq(Instant left, Long right) {
    return Objects.equals(left.toEpochMilli(), right);
  }

  @Specialization
  protected boolean eq(Integer left, LocalTime right) {
    return Objects.equals(left, right.toSecondOfDay());
  }

  @Specialization
  protected boolean eq(LocalTime left, Integer right) {
    return Objects.equals(left.toSecondOfDay(), right);
  }

  @Specialization
  protected boolean eq(Integer left, LocalDate right) {
    return Objects.equals(left, Long.valueOf(right.toEpochDay()).intValue());
  }

  @Specialization
  protected boolean eq(LocalDate left, Integer right) {
    return Objects.equals(Long.valueOf(left.toEpochDay()).intValue(), right);
  }

  @Specialization
  @CompilerDirectives.TruffleBoundary
  protected boolean eq(Object left, Object right) {
    return ((Comparable) left).compareTo(right) == 0;
  }
}
