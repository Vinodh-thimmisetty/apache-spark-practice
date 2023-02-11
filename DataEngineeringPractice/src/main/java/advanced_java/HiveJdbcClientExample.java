package advanced_java;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class HiveJdbcClientExample {


    public static void main(String[] args) {
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            Connection conn = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");

            Statement stmt = conn.createStatement();
            String table_name = "employee";

            String sqlQuery = "select * from " + table_name;
            System.out.println("Executing query: " + sqlQuery);
            ResultSet rst = stmt.executeQuery(sqlQuery);
            while (rst.next()) {
                System.out.println(rst.getInt(1) + "\t" + rst.getString(2));
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
