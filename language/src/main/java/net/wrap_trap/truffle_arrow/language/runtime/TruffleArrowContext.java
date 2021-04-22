package net.wrap_trap.truffle_arrow.language.runtime;

import com.oracle.truffle.api.RootCallTarget;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.dsl.NodeFactory;
import com.oracle.truffle.api.instrumentation.AllocationReporter;
import com.oracle.truffle.api.TruffleLanguage.Env;
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
  private final FunctionRegistry functionRegistry;
  private final AllocationReporter allocationReporter;

  public TruffleArrowContext(TruffleArrowLanguage language, TruffleLanguage.Env env, List<NodeFactory<? extends BuiltinNode>> externalBuiltins) {
    this.env = env;
    this.input = new BufferedReader(new InputStreamReader(env.in()));
    this.output = new PrintWriter(env.out(), true);
    this.language = language;
    this.allocationReporter = env.lookup(AllocationReporter.class);
    installBuiltins();
    for (NodeFactory<? extends BuiltinNode> builtin : externalBuiltins) {
      installBuiltin(builtin);
    }
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

  private void installBuiltins() {
    installBuiltin(SLReadlnBuiltinFactory.getInstance());
    installBuiltin(SLPrintlnBuiltinFactory.getInstance());
    installBuiltin(SLNanoTimeBuiltinFactory.getInstance());
    installBuiltin(SLDefineFunctionBuiltinFactory.getInstance());
    installBuiltin(SLStackTraceBuiltinFactory.getInstance());
    installBuiltin(SLHelloEqualsWorldBuiltinFactory.getInstance());
    installBuiltin(SLNewObjectBuiltinFactory.getInstance());
    installBuiltin(SLEvalBuiltinFactory.getInstance());
    installBuiltin(SLImportBuiltinFactory.getInstance());
    installBuiltin(SLGetSizeBuiltinFactory.getInstance());
    installBuiltin(SLHasSizeBuiltinFactory.getInstance());
    installBuiltin(SLIsExecutableBuiltinFactory.getInstance());
    installBuiltin(SLIsNullBuiltinFactory.getInstance());
    installBuiltin(SLWrapPrimitiveBuiltinFactory.getInstance());
    installBuiltin(SLTypeOfBuiltinFactory.getInstance());
    installBuiltin(SLIsInstanceBuiltinFactory.getInstance());
  }

  public void installBuiltin(NodeFactory<? extends BuiltinNode> factory) {
    /* Register the builtin function in our function registry. */
    RootCallTarget target = language.lookupBuiltin(factory);
    String rootName = target.getRootNode().getName();
    getFunctionRegistry().register(rootName, target);
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
    } else if (a instanceof SLContext) {
      return a;
    }
    throw shouldNotReachHere("Value is not a truffle value.");
  }

  @TruffleBoundary
  private static long fromForeignNumber(Object a) {
    return ((Number) a).longValue();
  }

  @TruffleBoundary
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

  public static SLContext getCurrent() {
    return SLLanguage.getCurrentContext();
  }
}
