package net.wrap_trap.truffle_arrow;

import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.jdbc.JdbcMeta;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.server.AbstractAvaticaHandler;
import org.apache.calcite.avatica.server.AvaticaProtobufHandler;
import org.apache.calcite.avatica.server.HttpServer;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.ServerConnector;

import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Server {

  private static int PORT = 3625;

  public static void main(String[] args) throws SQLException, InterruptedException, ClassNotFoundException {
    Class.forName("net.wrap_trap.truffle_arrow.TruffleDriver");

    LocalService service = new LocalService(createMeta());
    AbstractAvaticaHandler handler = new AvaticaProtobufHandler(service);
    HttpServer server = new HttpServer(PORT, handler) {
      @Override
      protected ServerConnector configureConnector(
        ServerConnector connector, int port) {
        HttpConnectionFactory factory = (HttpConnectionFactory)
                                          connector.getDefaultConnectionFactory();
        factory.getHttpConfiguration().setRequestHeaderSize(64 << 10);
        return super.configureConnector(connector, port);
      }
    };
    server.start();
    server.join();
  }

  private static Meta createMeta() throws SQLException {
    Properties props = new Properties();
    props.setProperty(JdbcMeta.ConnectionCacheSettings.EXPIRY_DURATION.key(), "10");
    props.setProperty(JdbcMeta.ConnectionCacheSettings.EXPIRY_UNIT.key(), TimeUnit.SECONDS.name());

    return new JdbcMeta(TruffleDriver.DRIVER_PREFIX, props);
  }
}
