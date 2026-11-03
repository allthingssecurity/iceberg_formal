package r5;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * jdbc:r5fault:<rest> delegates to jdbc:sqlite:<rest>. For the metadata-location CAS UPDATE
 * (identified by SQL text equality with JdbcUtil's own private constants, read by reflection), it
 * runs the real executeUpdate and THEN, while armed, throws SQLException with SQLState 08007.
 */
public final class FaultDriver implements Driver {
  public static final String PREFIX = "jdbc:r5fault:";
  public static final AtomicInteger ARMED = new AtomicInteger();
  public static final AtomicInteger INJECTED = new AtomicInteger();
  /** rows updated by every CAS UPDATE executed on the real database, in order. */
  public static final List<Integer> CAS_ROWS = new CopyOnWriteArrayList<>();
  /** Runs once, right after the first CAS UPDATE executed while set (fault or not), before any throw. */
  public static volatile Runnable HOOK = null;
  public static final AtomicInteger HOOK_RUNS = new AtomicInteger();

  private static final Set<String> CAS_SQL;

  static {
    try {
      Class<?> util = Class.forName("org.apache.iceberg.jdbc.JdbcUtil");
      Field v0 = util.getDeclaredField("V0_DO_COMMIT_SQL");
      Field v1 = util.getDeclaredField("V1_DO_COMMIT_TABLE_SQL");
      v0.setAccessible(true);
      v1.setAccessible(true);
      CAS_SQL = Set.of((String) v0.get(null), (String) v1.get(null));
      DriverManager.registerDriver(new FaultDriver());
    } catch (ReflectiveOperationException | SQLException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  public static void reset() {
    ARMED.set(0);
    INJECTED.set(0);
    CAS_ROWS.clear();
    HOOK = null;
    HOOK_RUNS.set(0);
  }

  @Override
  public Connection connect(String url, Properties info) throws SQLException {
    if (!acceptsURL(url)) {
      return null;
    }
    Connection real = DriverManager.getConnection("jdbc:sqlite:" + url.substring(PREFIX.length()), info);
    return (Connection)
        Proxy.newProxyInstance(
            FaultDriver.class.getClassLoader(),
            new Class<?>[] {Connection.class},
            (proxy, m, args) -> {
              Object r = invoke(real, m, args);
              if (m.getName().equals("prepareStatement") && args != null && args.length == 1
                  && CAS_SQL.contains((String) args[0])) {
                return casStatement((PreparedStatement) r);
              }
              return r;
            });
  }

  private static PreparedStatement casStatement(PreparedStatement real) {
    InvocationHandler h =
        (proxy, m, args) -> {
          if (m.getName().equals("executeUpdate") && (args == null || args.length == 0)) {
            int rows = real.executeUpdate();
            CAS_ROWS.add(rows);
            boolean inject = ARMED.getAndUpdate(a -> a > 0 ? a - 1 : 0) > 0;
            Runnable hook = HOOK;
            HOOK = null;
            if (hook != null) {
              HOOK_RUNS.incrementAndGet();
              hook.run();
            }
            if (inject) {
              INJECTED.incrementAndGet();
              throw new SQLException("r5 injected: transaction resolution unknown", "08007");
            }
            return rows;
          }
          return invoke(real, m, args);
        };
    return (PreparedStatement)
        Proxy.newProxyInstance(
            FaultDriver.class.getClassLoader(), new Class<?>[] {PreparedStatement.class}, h);
  }

  private static Object invoke(Object target, java.lang.reflect.Method m, Object[] args)
      throws Throwable {
    try {
      return m.invoke(target, args);
    } catch (InvocationTargetException e) {
      throw e.getCause();
    }
  }

  @Override
  public boolean acceptsURL(String url) {
    return url != null && url.startsWith(PREFIX);
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
    return new DriverPropertyInfo[0];
  }

  @Override
  public int getMajorVersion() {
    return 1;
  }

  @Override
  public int getMinorVersion() {
    return 0;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }
}
