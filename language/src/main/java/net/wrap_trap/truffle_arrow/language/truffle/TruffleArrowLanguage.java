package net.wrap_trap.truffle_arrow.language.truffle;

import com.oracle.truffle.api.CallTarget;
import com.oracle.truffle.api.Truffle;
import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.nodes.Node;
import net.wrap_trap.truffle_arrow.language.parser.TruffleArrowParser;
import net.wrap_trap.truffle_arrow.language.parser.ast.AST;
import net.wrap_trap.truffle_arrow.language.truffle.node.Statements;
import org.jparsec.Parser;

import java.util.List;

@TruffleLanguage.Registration(id="ta", name = "TruffleArrow", version = "0.1", mimeType = TruffleArrowLanguage.MIME_TYPE)
public class TruffleArrowLanguage extends TruffleLanguage {
  public static final String MIME_TYPE = "application/x-truffle-arrow";

  @Override
  protected CallTarget parse(TruffleLanguage.ParsingRequest request) throws Exception {
    Parser<List<AST.ASTNode>> parser = TruffleArrowParser.createParser();
    List<AST.ASTNode> script = parser.parse(request.getSource().getReader());
    TruffleArrowTreeGenerator generator = new TruffleArrowTreeGenerator(this);
    FrameDescriptor frame = new FrameDescriptor();
    Statements statements = generator.visit(frame, script);
    TruffleArrowRootNode root = new TruffleArrowRootNode(this, frame, statements);
    return Truffle.getRuntime().createCallTarget(root);
  }

  @Override
  protected TruffleArrowContext createContext(Env env) {
    return new TruffleArrowContext();
  }

  @Override
  protected boolean isObjectOfLanguage(Object object) {
    return false;
  }
}
