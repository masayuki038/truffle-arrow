package net.wrap_trap.truffle_arrow;

import com.oracle.truffle.api.TruffleLanguage;
import java.io.*;

public class TruffleArrowContext {
  private BufferedReader in;
  private PrintWriter out;
  private PrintWriter err;

  static TruffleArrowContext from(TruffleLanguage.Env env) {
    return new TruffleArrowContext(env.in(), env.out(), env.err());
  }

  public TruffleArrowContext(InputStream in, OutputStream out, OutputStream err) {
    this.in = new BufferedReader(new InputStreamReader(in));
    this.out = new PrintWriter(out, true);
    this.err = new PrintWriter(err, true);
  }
}
