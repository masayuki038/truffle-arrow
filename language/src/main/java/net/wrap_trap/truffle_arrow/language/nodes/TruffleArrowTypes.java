package net.wrap_trap.truffle_arrow.language.nodes;

import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.dsl.ImplicitCast;
import com.oracle.truffle.api.dsl.TypeCheck;
import com.oracle.truffle.api.dsl.TypeSystem;

import java.math.BigInteger;

@TypeSystem({long.class, boolean.class})
public abstract class TruffleArrowTypes {


  @TypeCheck(SLNull.class)
  public static boolean isSLNull(Object value) {
    return value == SLNull.SINGLETON;
  }

  @TypeCast(SLNull.class)
  public static SLNull asSLNull(Object value) {
    assert isSLNull(value);
    return SLNull.SINGLETON;
  }

  @ImplicitCast
  @CompilerDirectives.TruffleBoundary
  public static SLBigNumber castBigNumber(long value) {
    return new SLBigNumber(BigInteger.valueOf(value));
  }
}
