package net.wrap_trap.truffle_arrow;

import org.apache.calcite.avatica.AvaticaConnection;
import org.apache.calcite.avatica.DriverVersion;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.UnregisteredDriver;

import java.sql.DriverManager;
import java.sql.SQLException;

public class TruffleDriver extends UnregisteredDriver {

  public static final String DRIVER_PREFIX = "jdbc:truffle:";

  static {
    try {
      DriverManager.registerDriver(new TruffleDriver());
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  protected DriverVersion createDriverVersion() {
    return new DriverVersion("truffle-arrow JDBC Driver", "0.1", "Truffle", "0.1", true, 0, 1, 0, 1);
  }

  @Override
  protected String getConnectStringPrefix() {
    return DRIVER_PREFIX;
  }

  @Override
  public Meta createMeta(AvaticaConnection connection) {
    return new TruffleArrowMeta(connection);
  }
}
