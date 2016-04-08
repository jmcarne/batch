package batchconsult;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by CONSULTANT on 07/04/2016.
 */
public class MainBatch {
    //private static String[] connMainStr = {"jdbc:vertica://10.12.70.125:5433/main", "dbadmin", ""};
    //private static String[] connSuppStr = {"jdbc:vertica://10.12.70.125:5433/main", "dbadmin", ""};
    private static String[] connMainStr = {"jdbc:vertica://10.12.70.169:5433/main", "dbadmin", ""};
    private static String[] connSuppStr = {"jdbc:vertica://10.12.70.169:5433/main", "dbadmin", ""};


    //private static String[] connMainStr = {"jdbc:mysql://localhost/consmain", "root", "root"}; // 3306
    //private static String[] connSuppStr = {"jdbc:mysql://localhost/conssupp", "root", "root"};

    public static void main(String[] args) {
        System.out.println("Program has run successfully");
        // DB connections
        try {
            Class.forName("com.vertica.jdbc.Driver");
            //Class.forName("com.mysql.jdbc.Driver");

            //-------------------------------  TO DELETE
            Connection connSupp = DriverManager.getConnection(connSuppStr[0], connSuppStr[1], connSuppStr[2]);
            List<Subscription> subscriptions = new ArrayList<Subscription>(); //Get the subscriptions here!!!!

            try {
                Statement stmtSupp = connSupp.createStatement();
                ResultSet rsIns = null;
                rsIns = stmtSupp.executeQuery("SELECT USER, USR, TABLE_NAME, CONDITION_NOTIFICATION FROM CONDITIONS WHERE USR = 'MARIO'");

                while (rsIns.next()) {
                    System.out.println("row..");
                    Subscription s = new Subscription();
                    s.setUser(rsIns.getString(1));
                    s.setTable(rsIns.getString(2));
                    s.setCondition(rsIns.getString(3));
                    subscriptions.add(s);
                }
                System.out.println(subscriptions.size());
                System.out.println("..end");
            } catch (Exception e) {	}
            //-------------------------------

			/* Riattivare qui..
			Connection connMain = DriverManager.getConnection(connMainStr[0], connMainStr[1], connMainStr[2]);
			System.out.println("Connection establisced: ");
			Statement stmtMain = connMain.createStatement();
			ResultSet rsIns = null;
			rsIns = stmtMain.executeQuery("SELECT * FROM UPDATED_DATA");
	        //rsIns = stmtMain.executeQuery("SELECT * FROM TEST_TABLE");

	        while (rsIns.next()) {
	        	System.out.println("\nNew record:");
	        	System.out.println(rsIns.getString(1));
	        	System.out.println(rsIns.getString(2));
	        	System.out.println(rsIns.getString(3));
	        }*/
			/*
			Connection connSupp = DriverManager.getConnection(connSuppStr[0], connSuppStr[1], connSuppStr[2]);
			Statement stmtSupp = connSupp.createStatement();

			// Read from support DB
			//   insertion ID & conditions..
			//   check insertion id with the following query

			// Read from main DB
			//..
			ResultSet rsIns = null;
	        rsIns = stmtMain.executeQuery("SELECT * FROM table.. WHERE ID > xyz");

	        ResultSet rsUpd = null;
	        rsUpd = stmtMain.executeQuery("SELECT * FROM UPDATED_RECORDS");
	        ResultSet rsDel = null;
	        rsDel = stmtMain.executeQuery("SELECT * FROM DELETED_RECORDS"); // 2 tables

			// Verify

			// Send notification.. Where?
			String json = "";
			sendJson(json);

			// Update insertion ID
			// For each table..
			stmtSupp.executeUpdate("UPDATE INSERTION_ID SET LAST_ID = num WHERE TABLE = 'tablename'");

			// Delete UPDATED_RECORDS records
			stmtMain.executeUpdate("DELETE FROM UPDATED_RECORDS");

			// Delete DELETED_RECORDS records
			stmtMain.executeUpdate("DELETE FROM DELETED_RECORDS");
	        */
            // Closing connections
            //connMain.close();
            connSupp.close();
        } catch (Exception e) {
            // ..
            System.out.println("Exception: " + e.toString());
        }
    }

    private static void sendJson(String json) {

    }
}
