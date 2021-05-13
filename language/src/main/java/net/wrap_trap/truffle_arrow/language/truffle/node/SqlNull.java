package net.wrap_trap.truffle_arrow.language.truffle.node;

import com.oracle.truffle.api.interop.InteropLibrary;
import com.oracle.truffle.api.interop.TruffleObject;
import com.oracle.truffle.api.library.ExportLibrary;
import com.oracle.truffle.api.library.ExportMessage;

/**
 * Our representation of NULL
 */
@ExportLibrary(InteropLibrary.class)
public class SqlNull implements TruffleObject {
  public static final SqlNull INSTANCE = new SqlNull();

  private SqlNull() { }

  @ExportMessage
  public boolean isNull() {
    return true;
  }

  @Override
  public String toString() {
    return "NULL";
  }
}
