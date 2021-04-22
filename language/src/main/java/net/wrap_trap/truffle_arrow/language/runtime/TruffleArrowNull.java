package net.wrap_trap.truffle_arrow.language.runtime;

import com.oracle.truffle.api.TruffleLanguage;
import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;
import net.wrap_trap.truffle_arrow.language.TruffleArrowLanguage;

@ExportLibrary(InteropLibrary.class)
@SuppressWarnings("static-method")
public class TruffleArrowNull implements TruffleObject {

  public static final TruffleArrowNull SINGLETON = new TruffleArrowNull();
  private static final int IDENTITY_HASH = System.identityHashCode(SINGLETON);

  private TruffleArrowNull() { }

  @Override
  public String toString() {
    return "NULL";
  }

  @ExportMessage
  public boolean hasLanguage() {
    return true;
  }

  @ExportMessage
  Class<? extends TruffleLanguage<?>> getLanguage() {
    return TruffleArrowLanguage.class;
  }

}
