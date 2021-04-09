package net.wrap_trap.truffle_arrow.truffle;

import com.oracle.truffle.api.dsl.NodeChild;
import com.oracle.truffle.api.dsl.Specialization;

@NodeChild("target")
abstract class ExprNot extends ExprBase {
  @Specialization
  boolean executeBoolean(boolean value) {
    return !value;
  }

  @Specialization
  SqlNull executeNull(Object any) {
    return SqlNull.INSTANCE;
  }
}