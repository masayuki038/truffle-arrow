package net.wrap_trap.truffle_arrow.language.parser;

import org.graalvm.polyglot.Context;
import org.junit.Test;

public class TruffleArrowLanguageTest {
  private final static String SAMPLE =
      "$a = 1;\n" +
      "$b = 2;\n" +
      "$a + $b;";

  @Test
  public void testScript() {
    Context ctx = Context.create("ta");
    ctx.eval("ta", SAMPLE);
  }
}
