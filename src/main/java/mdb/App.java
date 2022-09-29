package mdb;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
public class App {
    public static java.sql.Connection con;
    public static void main(String[] args) {
        String sqlRead, sqlWrite;
        sqlWrite = "INSERT INTO products(c) VALUES(CONCAT('Data - ', ROUND(RAND()*10000000,0)))";
        sqlRead = "SELECT @@hostname as hostName, c FROM products WHERE id = LAST_INSERT_ID()";
        try {
            connectToDatabase();
            con.setAutoCommit(false);
            con.setTransactionIsolation(1);
            Statement stmt = con.createStatement();
            ResultSet rs;
            int transCount=0;
            while (true) {
                try {
                    stmt.executeQuery(sqlWrite);
                    rs = stmt.executeQuery(sqlRead);
                    if (rs.next()) 
                        System.out.println(transCount + ": Record found for " + rs.getString("c") + " on host " + rs.getString("hostName"));
                    
                    //Commit 30 writes at a time
                    if ((transCount++) % 30 == 0)
                        con.commit();
                    rs.close();
                    Thread.sleep(50);
                } catch (Exception e) {
                    e.printStackTrace();
                    con.rollback();
                }
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
            "jdbc:mariadb:sequential://localhost:4601,localhost:4602/securedb?transactionReplay&transactionReplaySize=1000",    
            "app_user", "P@ssw0rd"
        );
    }

    public static void disconnectFromDatabase() throws SQLException {
        if (con != null)
            con.close();
    }
}
