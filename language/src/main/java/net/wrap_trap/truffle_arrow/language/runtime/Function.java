package net.wrap_trap.truffle_arrow.language.runtime;

import com.oracle.truffle.api.*;
import com.oracle.truffle.api.dsl.Cached;
import com.oracle.truffle.api.dsl.Fallback;
import com.oracle.truffle.api.dsl.ReportPolymorphism;
import com.oracle.truffle.api.dsl.Specialization;
import com.oracle.truffle.api.library.ExportMessage;
import com.oracle.truffle.api.nodes.DirectCallNode;
import com.oracle.truffle.api.nodes.IndirectCallNode;
import com.oracle.truffle.api.source.SourceSection;
import com.oracle.truffle.api.utilities.CyclicAssumption;
import com.oracle.truffle.api.utilities.TriState;
import net.wrap_trap.truffle_arrow.language.TruffleArrowLanguage;

import java.util.logging.Level;

public class Function {
  public static final int INLINE_CACHE_SIZE = 2;

  private static final TruffleLogger LOG = TruffleLogger.getLogger(TruffleArrowLanguage.ID, Function.class);

  /** The name of the function. */
  private final String name;

  /** The current implementation of this function. */
  private RootCallTarget callTarget;

  /**
   * Manages the assumption that the {@link #callTarget} is stable. We use the utility class
   * {@link CyclicAssumption}, which automatically creates a new {@link Assumption} when the old
   * one gets invalidated.
   */
  private final CyclicAssumption callTargetStable;

  protected Function(TruffleArrowLanguage language, String name) {
    this(language.getOrCreateUndefinedFunction(name));
  }

  protected Function(RootCallTarget callTarget) {
    this.name = callTarget.getRootNode().getName();
    this.callTargetStable = new CyclicAssumption(name);
    setCallTarget(callTarget);
  }

  public String getName() {
    return name;
  }

  protected void setCallTarget(RootCallTarget callTarget) {
    boolean wasNull = this.callTarget == null;
    this.callTarget = callTarget;
    /*
     * We have a new call target. Invalidate all code that speculated that the old call target
     * was stable.
     */
    LOG.log(Level.FINE, "Installed call target for: {0}", name);
    if (!wasNull) {
      callTargetStable.invalidate();
    }
  }

  public RootCallTarget getCallTarget() {
    return callTarget;
  }

  public Assumption getCallTargetStable() {
    return callTargetStable.getAssumption();
  }

  @Override
  public String toString() {
    return name;
  }

  @ExportMessage
  boolean hasLanguage() {
    return true;
  }

  @ExportMessage
  Class<? extends TruffleLanguage<?>> getLanguage() {
    return TruffleArrowLanguage.class;
  }

  @SuppressWarnings("static-method")
  @ExportMessage
  @CompilerDirectives.TruffleBoundary
  SourceSection getSourceLocation() {
    return getCallTarget().getRootNode().getSourceSection();
  }

  @SuppressWarnings("static-method")
  @ExportMessage
  boolean hasSourceLocation() {
    return true;
  }

  @ExportMessage
  boolean isExecutable() {
    return true;
  }

  @ExportMessage
  boolean hasMetaObject() {
    return true;
  }

  @ExportMessage
  Object getMetaObject() {
    return TruffleArrowType.FUNCTION;
  }

  @ExportMessage
  @SuppressWarnings("unused")
  static final class IsIdenticalOrUndefined {
    @Specialization
    static TriState doSLFunction(Function receiver, Function other) {
      return receiver == other ? TriState.TRUE : TriState.FALSE;
    }

    @Fallback
    static TriState doOther(Function receiver, Object other) {
      return TriState.UNDEFINED;
    }
  }

  @ExportMessage
  @CompilerDirectives.TruffleBoundary
  static int identityHashCode(Function receiver) {
    return System.identityHashCode(receiver);
  }

  @ExportMessage
  Object toDisplayString(@SuppressWarnings("unused") boolean allowSideEffects) {
    return name;
  }

  @ReportPolymorphism
  @ExportMessage
  abstract static class Execute {

    @Specialization(limit = "INLINE_CACHE_SIZE", //
      guards = "function.getCallTarget() == cachedTarget", //
      assumptions = "callTargetStable")
    @SuppressWarnings("unused")
    protected static Object doDirect(Function function, Object[] arguments,
                                     @Cached("function.getCallTargetStable()") Assumption callTargetStable,
                                     @Cached("function.getCallTarget()") RootCallTarget cachedTarget,
                                     @Cached("create(cachedTarget)") DirectCallNode callNode) {

      return callNode.call(arguments);
    }

    @Specialization(replaces = "doDirect")
    protected static Object doIndirect(Function function, Object[] arguments,
                                       @Cached IndirectCallNode callNode) {
      return callNode.call(function.getCallTarget(), arguments);
    }
  }
}
