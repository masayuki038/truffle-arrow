package net.wrap_trap.truffle_arrow.language.runtime;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.CompilerDirectives;
import com.oracle.truffle.api.RootCallTarget;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.NodeFactory;
import com.oracle.truffle.api.instrumentation.AllocationReporter;
import com.oracle.truffle.api.TruffleLanguage.Env;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.source.Source;
import net.wrap_trap.truffle_arrow.language.TruffleArrowLanguage;
import net.wrap_trap.truffle_arrow.language.builtins.BuiltinNode;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.List;

public class TruffleArrowContext {

  private final TruffleArrowLanguage language;
  private final Env env;
  private final BufferedReader input;
  private final PrintWriter output;
  private final AllocationReporter allocationReporter;

  public TruffleArrowContext(TruffleArrowLanguage language, TruffleLanguage.Env env) {
    this.env = env;
    this.input = new BufferedReader(new InputStreamReader(env.in()));
    this.output = new PrintWriter(env.out(), true);
    this.language = language;
    this.allocationReporter = env.lookup(AllocationReporter.class);
  }

  /**
   * Return the current Truffle environment.
   */
  public Env getEnv() {
    return env;
  }

  public BufferedReader getInput() {
    return input;
  }

  public PrintWriter getOutput() {
    return output;
  }

  /*
   * Methods for object creation / object property access.
   */
  public AllocationReporter getAllocationReporter() {
    return allocationReporter;
  }

  /*
   * Methods for language interoperability.
   */
  public static Object fromForeignValue(Object a) {
    if (a instanceof Long || a instanceof SLBigNumber || a instanceof String || a instanceof Boolean) {
      return a;
    } else if (a instanceof Character) {
      return fromForeignCharacter((Character) a);
    } else if (a instanceof Number) {
      return fromForeignNumber(a);
    } else if (a instanceof TruffleObject) {
      return a;
    } else if (a instanceof TruffleArrowContext) {
      return a;
    }
    throw shouldNotReachHere("Value is not a truffle value.");
  }

  @CompilerDirectives.TruffleBoundary
  private static long fromForeignNumber(Object a) {
    return ((Number) a).longValue();
  }

  @CompilerDirectives.TruffleBoundary
  private static String fromForeignCharacter(char c) {
    return String.valueOf(c);
  }

  public CallTarget parse(Source source) {
    return env.parsePublic(source);
  }

  /**
   * Returns an object that contains bindings that were exported across all used languages. To
   * read or write from this object the {@link TruffleObject interop} API can be used.
   */
  public TruffleObject getPolyglotBindings() {
    return (TruffleObject) env.getPolyglotBindings();
  }

  public static TruffleArrowContext getCurrent() {
    return TruffleArrowLanguage.getCurrentContext();
  }
}
