package mn.bplogic.main;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class SqlStatics {
	private static SqlStatics sqlStaticsStorage = null;

	private Connection conn = null;

	private SqlStatics(){
	}

	// inner class for Exceptions
	public class SqlStaticsException extends Exception {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4453168243236928177L;

		SqlStaticsException(String msg, Exception cause){
			super(msg, cause);
		}
	}

	// private object based methods start here
	private Connection getConnection() throws SqlStaticsException{
		if (conn == null){
			try{
				try {
					conn = DriverManager.getConnection("jdbc:default:connection:");
				} catch (SQLException e) {
					try {
						Class.forName ("oracle.jdbc.OracleDriver");
					} catch (ClassNotFoundException e1) {
						throw new SqlStaticsException("No oracle driver installed in javapath", e1);
					}
					conn = DriverManager.getConnection
							("jdbc:oracle:thin:@//localhost:1521/XE", "XE", "XE");
				}
			} catch (SQLException e) {
				throw new SqlStaticsException("Connection failed", e);
			}
		}
		return conn;
	}

	// Public static methods start here
	public static Connection getSqlConnection() throws SqlStaticsException{
		if (sqlStaticsStorage == null){
			sqlStaticsStorage = new SqlStatics();
		}
		return sqlStaticsStorage.getConnection();
	}

	public static void executeSQL(String sql) throws SqlStaticsException{
		Connection conn = getSqlConnection();
		try {
			PreparedStatement prepareStatement = conn.prepareStatement(sql);
			prepareStatement.execute();
		} catch (SQLException e) {
			throw sqlStaticsStorage.new SqlStaticsException("Exception during SQL execution", e);
		}
	}
}
