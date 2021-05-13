package net.wrap_trap.truffle_arrow.language.parser;

import org.graalvm.polyglot.Context;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

public class TruffleArrowLanguageTest {
  private final static String SAMPLE =
      "$a = 1;\n" +
      "$b = 2;\n" +
      "return $a + $b;";

  @Test
  public void testScript() {
    Context ctx = Context.create("ta");
    assertThat(ctx.eval("ta", SAMPLE).asDouble(), is(3d));
  }
}
