import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class PhoenixUtils
{
    private PhoenixUtils() {}

    static
    {
        try
        {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public synchronized static Connection getConnection()
    {
        try
        {
            Connection conn =DriverManager.getConnection("jdbc:phoenix:centos1.Hadoop");
            conn.setAutoCommit(true);
            return conn;
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return null;
    }
}
