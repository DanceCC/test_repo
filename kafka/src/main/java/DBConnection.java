import java.sql.*;
public class DBConnection {
    private static Connection con = null;
    // 驱动程序名
    private static String driverName = "com.mysql.jdbc.Driver";
    // 数据库用户名
    private static String userName = "root";
    // 密码
    private static String userPasswd = "123456";
    // 数据库名
    private static String dbName = "bigdata";

    public static Connection getConnection() {

        String url = "jdbc:mysql://localhost/" + dbName + "?user="
                + userName + "&password=" + userPasswd
                + "&useUnicode=true&characterEncoding=gbk";
        try {
            // 1.驱动
            Class.forName(driverName);
            // 2. 连接数据库 保持连接
            con = DriverManager.getConnection(url);
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return con;
    }
    public static void closeConnection() {
        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}