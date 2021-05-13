package net.wrap_trap.truffle_arrow.language.truffle.node;

import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.nodes.NodeInfo;

@NodeInfo(shortName = "-")
public abstract class ExprMinus extends ExprBinary {

  @Specialization
  protected long add(int left, int right) {
    return left - right;
  }

  @Specialization
  protected long add(long left, long right) {
    return left - right;
  }

  @Specialization
  protected long add(long left, int right) {
    return left - right;
  }

  @Specialization
  protected long add(int left, long right) {
    return left - right;
  }

  @Specialization
  protected double add(double left, double right) {
    return left - right;
  }

  @Specialization
  protected SqlNull leftNull(SqlNull left, Object right) {
    return SqlNull.INSTANCE;
  }

  @Specialization
  protected SqlNull rightNull(Object left, SqlNull right) {
    return SqlNull.INSTANCE;
  }
}
