package mdb;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@ApplicationPath("app")
public class App extends Application {
    public static java.sql.Connection con;
    public static void main(String[] args) {
        String sqlRead, sqlWrite;
        sqlWrite = "INSERT INTO products(c) VALUES(CONCAT('Data - ', ROUND(RAND()*10000000,0)))";
        sqlRead = "SELECT @@hostname as hostName, c FROM products WHERE id = LAST_INSERT_ID()";
        try {
            connectToDatabase();
            Statement stmt = con.createStatement();
            ResultSet rs;
            System.out.println("Success!");
            while (true) {
                con.setTransactionIsolation(1);
                stmt.executeQuery(sqlWrite);
                rs = stmt.executeQuery(sqlRead);
                if (rs.next()) {
                    System.out.println("Record found for " + rs.getString("c") + " on host " + rs.getString("hostName"));
                } else {
                    System.out.println("Record NOT found!");
                }
                con.commit();
                rs.close();
                Thread.sleep(50);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                disconnectFromDatabase();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
    public static void connectToDatabase() throws SQLException {
        con = DriverManager.getConnection(
            "jdbc:mariadb:loadbalance://localhost:4601,localhost:4602/securedb",    
            "app_user", "P@ssw0rd"
        );
        con.setAutoCommit(false);
    }
    public static void disconnectFromDatabase() throws SQLException {
        if (con != null)
            con.close();
    }
}
