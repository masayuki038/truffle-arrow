package net.wrap_trap.truffle_arrow.language;

import com.oracle.truffle.api.*;
import com.oracle.truffle.api.debug.DebuggerTags;
import com.oracle.truffle.api.dsl.NodeFactory;
import com.oracle.truffle.api.instrumentation.ProvidedTags;
import com.oracle.truffle.api.instrumentation.StandardTags;
import com.oracle.truffle.api.nodes.NodeInfo;
import com.oracle.truffle.api.object.Shape;
import com.oracle.truffle.api.source.Source;
import net.wrap_trap.truffle_arrow.language.builtins.BuiltinNode;
import net.wrap_trap.truffle_arrow.language.parser.TruffleArrowLanguageParser;
import net.wrap_trap.truffle_arrow.language.runtime.TruffleArrowContext;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@TruffleLanguage.Registration(id = TruffleArrowLanguage.ID, name = "truffle-arrow", defaultMimeType = TruffleArrowLanguage.MIME_TYPE, characterMimeTypes = TruffleArrowLanguage.MIME_TYPE, contextPolicy = TruffleLanguage.ContextPolicy.SHARED, fileTypeDetectors = TruffleArrowFileDetector.class)
@ProvidedTags({StandardTags.CallTag.class, StandardTags.StatementTag.class, StandardTags.RootTag.class, StandardTags.RootBodyTag.class, StandardTags.ExpressionTag.class, DebuggerTags.AlwaysHalt.class,
  StandardTags.ReadVariableTag.class, StandardTags.WriteVariableTag.class})
public class TruffleArrowLanguage extends TruffleLanguage<TruffleArrowContext> {
  public static volatile int counter;

  public static final String ID = "truffle-arrow";
  public static final String MIME_TYPE = "application/x-truffle-arrow";
  private static final Source BUILTIN_SOURCE = Source.newBuilder(TruffleArrowLanguage.ID, "", "SL builtin").build();

  private final Assumption singleContext = Truffle.getRuntime().createAssumption("Single TruffleArrow context.");

  private final Map<NodeFactory<? extends BuiltinNode>, RootCallTarget> builtinTargets = new ConcurrentHashMap<>();
  private final Map<String, RootCallTarget> undefinedFunctions = new ConcurrentHashMap<>();

  private final Shape rootShape;


  public TruffleArrowLanguage() {
    counter++;
    this.rootShape = Shape.newBuilder().layout(SLObject.class).build();
  }

  @Override
  protected TruffleArrowContext createContext(Env env) {
    return new TruffleArrowContext(this, env);
  }

  public static NodeInfo lookupNodeInfo(Class<?> clazz) {
    if (clazz == null) {
      return null;
    }
    NodeInfo info = clazz.getAnnotation(NodeInfo.class);
    if (info != null) {
      return info;
    } else {
      return lookupNodeInfo(clazz.getSuperclass());
    }
  }

  @Override
  protected CallTarget parse(ParsingRequest request) throws Exception {
    Map<String, RootCallTarget> targets = TruffleArrowLanguageParser.parse(this, request.getSource());
  }

}
