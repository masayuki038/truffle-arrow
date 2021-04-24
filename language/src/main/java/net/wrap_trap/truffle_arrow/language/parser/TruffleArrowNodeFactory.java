package net.wrap_trap.truffle_arrow.language.parser;

import com.oracle.truffle.api.frame.FrameDescriptor;
import com.oracle.truffle.api.frame.FrameSlot;
import com.oracle.truffle.api.source.Source;
import net.wrap_trap.truffle_arrow.language.TruffleArrowLanguage;

import java.util.HashMap;
import java.util.Map;

public class TruffleArrowNodeFactory {
  static class LexicalScope {
    protected final LexicalScope outer;
    protected final Map<String, FrameSlot> locals;

    LexicalScope(LexicalScope outer) {
      this.outer = outer;
      this.locals = new HashMap<>();
      if (outer != null) {
        locals.putAll(outer.locals);
      }
    }
  }

  private final Source source;
  private FrameDescriptor frameDescriptor;
  private LexicalScope lexicalScope;
  private final TruffleArrowLanguage language;

  public TruffleArrowNodeFactory(TruffleArrowLanguage language, Source source) {
    this.language = language;
    this.source = source;
  }
}
