package mdb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
public class App {
    public static String sqlRead, sqlWrite;
    public static void main(String[] args) {
        executeBatches();
    }
                  
    public static void excuteSingleInserts() {
        sqlWrite = "INSERT INTO products(c) VALUES(CONCAT('Data - ', ROUND(RAND()*10000000,0)))";
        sqlRead = "SELECT @@hostname as hostName, c FROM products WHERE id = LAST_INSERT_ID()";
        Connection con;
        con = connectToDatabase();
        try {
            con.setAutoCommit(false);
            con.setTransactionIsolation(1);
            Statement stmt = con.createStatement();
            ResultSet rs;
            //int transCount = 0;
            while (true) {
                try {
                    stmt.executeQuery(sqlWrite);
                    rs = stmt.executeQuery(sqlRead);
                    //transCount++;
                    if (rs.next()) 
                        //System.out.println(transCount + ": Record found for " + rs.getString("c") + " on host " + rs.getString("hostName"));
                    
                    con.commit();
                    rs.close();
                    Thread.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                    con.rollback();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        disconnectFromDatabase(con);
    }

    public static void executeBatches() {
        sqlWrite = "INSERT INTO big_table(c) VALUES(CONCAT('Data - ', ROUND(RAND()*10000000,0)))";
        sqlRead = "SELECT @@hostname as hostName, c FROM big_table WHERE id = LAST_INSERT_ID()";
        Connection con;
        con = connectToDatabase();
        try {
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
                        System.out.println(transCount + ": Record found for \"" + rs.getString("c") + "\" on host " + rs.getString("hostName"));
                    
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
        }
        disconnectFromDatabase(con);
    }
    
    public static Connection connectToDatabase() {
        Connection con;
        try {
            //
            con = DriverManager.getConnection(
                "jdbc:mariadb:loadbalance//localhost:4601,localhost:4602/securedb?transactionReplay&transactionReplaySize=1000&socketTimeout=65535",    
                "app_user", "P@ssw0rd"
            );
            return con;           
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void disconnectFromDatabase(Connection con) {
        if (con != null)
            try {
                con.close();
                
            } catch (Exception e) {
                e.printStackTrace();
            }
    }
}