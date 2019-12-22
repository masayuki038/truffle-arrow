package net.wrap_trap.truffle_arrow;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.frame.VirtualFrame;
import com.oracle.truffle.api.nodes.ExecutableNode;
import com.oracle.truffle.api.source.Source;
import com.oracle.truffle.api.source.SourceSection;
import net.wrap_trap.truffle_arrow.truffle.RelRootNode;
import net.wrap_trap.truffle_arrow.truffle.RowSink;
import net.wrap_trap.truffle_arrow.truffle.RowSource;
import net.wrap_trap.truffle_arrow.truffle.ThenRowSink;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

@TruffleLanguage.Registration(id="ta", name = "TruffleArrow", version = "0.1", mimeType = TruffleArrowLanguage.MIME_TYPE)
public class TruffleArrowLanguage extends TruffleLanguage<TruffleArrowContext> {
  public static final String MIME_TYPE = "application/x-truffle-arrow-sql";

  public static final TruffleArrowLanguage INSTANCE = new TruffleArrowLanguage();

  private TruffleArrowLanguage() { }

  @Override
  protected TruffleArrowContext createContext(Env env) {
    return TruffleArrowContext.from(env);
  }

  @Override
  protected ExecutableNode parse(TruffleLanguage.InlineParsingRequest request) throws Exception {
    throw new UnsupportedOperationException();
  }

  @Override
  protected Object findExportedSymbol(TruffleArrowContext context, String globalName, boolean onlyExplicit) {
    return null;
  }

  @Override
  protected Object getLanguageGlobal(TruffleArrowContext context) {
    return context;
  }

  @Override
  protected boolean isObjectOfLanguage(Object object) {
    return false;
  }

  public CallTarget compileInteractiveQuery(RelRoot plan, Consumer<Object[]> then) {
    ThenRowSink sink = resultFrame -> new RowSink() {
      @Override
      public void executeVoid(VirtualFrame frame) {
        Object[] values = new Object[resultFrame.size()];

        for (int i = 0; i < resultFrame.size(); i++) {
          FrameSlot slot = resultFrame.findFrameSlot(i);
          Object truffleValue = frame.getValue(slot);
          RelDataType type = plan.validatedRowType.getFieldList().get(i).getType();
          Object resultSetValue = truffleValue; // TODO Convert type to match ResultSet interface

          values[i] = resultSetValue;
        }
        then.accept(values);
      }
    };
    return compile(plan, sink);
  }

  private CallTarget compile(RelRoot plan, ThenRowSink sink) {
    try {
      ArrowRel rel = (ArrowRel) plan.rel;
      RowSource compiled = rel.compile(sink);

      Source.SourceBuilder builder = Source.newBuilder("TruffleArrow", "", null);
      SourceSection sourceSection = builder.build().createUnavailableSection();
      RelRootNode root = new RelRootNode(INSTANCE, compiled);

      return Truffle.getRuntime().createCallTarget(root);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static void callWithRootContext(CallTarget main) {
    Objects.requireNonNull(main, "Program is null");
    TruffleArrowContext arrowContext = new TruffleArrowContext(
      System.in,
      System.out,
      System.err
    );
    main.call(arrowContext);
  }
}
