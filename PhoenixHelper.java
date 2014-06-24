import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.springframework.stereotype.Service;

@Service("phoenixHelper")
public class PhoenixHelper {  
	  
    static PhoenixHelper _HBaseHelper = null;  
    Connection connection = null;  
    Statement statement = null;  
    PreparedStatement preparedStatement = null;  
    String getExceptionInfoString = "";  
    String getDBConnectionString = "";  
  
    private PhoenixHelper() {}          
  
    public static PhoenixHelper getInstanceBaseHelper() {  
        if (_HBaseHelper == null)  
            synchronized (PhoenixHelper.class) {  
                if(_HBaseHelper==null)  
                    _HBaseHelper = new PhoenixHelper();  
            }             
        return _HBaseHelper;  
    }  
  
    public Object ExcuteNonQuery(String sql) {  
        int n = 0;  
        try {  
            connection =(Connection) PhoenixUtils.getConnection();
            statement = connection.createStatement();  
            n = statement.executeUpdate(sql);  
            connection.commit();  
        } catch (Exception e) {  
            Dispose();  
            e.printStackTrace(); 
        }   
        return n;  
    }  
  
    public Object ExcuteNonQuery(String sql, Object[] args) {  
        int n = 0;  
        try {  
            connection =(Connection) PhoenixUtils.getConnection();  
            preparedStatement = connection.prepareStatement(sql);  
            for (int i = 0; i < args.length; i++)  
                preparedStatement.setObject(i + 1, args[i]);  
            n = preparedStatement.executeUpdate();  
            connection.commit();  
        } catch (SQLException e) {  
            Dispose();
            e.printStackTrace(); 
        }  
        return n;  
    }  
  
    public ResultSet ExecuteQuery(String sql) {  
        ResultSet rsResultSet = null;  
        try {  
            connection =(Connection) PhoenixUtils.getConnection();
            statement = connection.createStatement();  
            rsResultSet = statement.executeQuery(sql);  
        } catch (Exception e) {  
            Dispose();  
            e.printStackTrace(); 
        }   
        return rsResultSet;  
    }  
  
    public ResultSet ExceteQuery(String sql, Object[] args) {  
        ResultSet rsResultSet = null;  
        try {  
            connection =(Connection) PhoenixUtils.getConnection();
            preparedStatement = connection.prepareStatement(sql);  
            for (int i = 0; i < args.length; i++)  
                preparedStatement.setObject(i + 1, args[i]);  
            rsResultSet = preparedStatement.executeQuery();  
  
        } catch (Exception e) {  
            Dispose();  
            e.printStackTrace(); 
        }   
        return rsResultSet;  
    }  
  
    public void Dispose() {  
        try {  
            if (connection != null)  
                connection.close();  
            if (statement != null)  
                statement.close();  
        } catch (Exception e) {  
            // TODO: handle exception  
            getExceptionInfoString = e.getMessage();  
        }  
    }  
} 

