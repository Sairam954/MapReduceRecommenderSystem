package Recommender;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;
 
public class HiveJDBCClient {
  private static String driverName = "org.apache.hive.jdbc.HiveDriver";
 
  /**
   * @param args
   * @throws SQLException
   */
  public static void main(String[] args) throws SQLException {
      try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      System.exit(1);
    }
    //replace "hive" here with the name of the user the queries should run as
    Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "hive", "");
    Statement stmt = con.createStatement();
    String tableName = "netflixtabledev";
    stmt.execute("drop table if exists " + tableName);
    stmt.execute("CREATE TABLE "+tableName+" (userid int, movieid int, rating int) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'");
    // show tables
    String sql = "show tables '" + tableName + "'";
    System.out.println("Running: " + sql);
    ResultSet res = stmt.executeQuery(sql);
    if (res.next()) {
      System.out.println(res.getString(1));
    }
       // describe table
    sql = "describe " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
    while (res.next()) {
      System.out.println(res.getString(1) + "\t" + res.getString(2));
    }
 
    // load data into table
    // NOTE: filepath has to be local to the hive server
    // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
//    String filepath = "/home/cloudera/workspace/cs626_proj_v2/input/mv_0000001.txt";
    String filepath = args[0];
    
    sql = "load data local inpath '" + filepath + "' into table " + tableName;
    System.out.println("Running: " + sql);
    stmt.execute(sql);
 
    // select * query
    sql = "select * from " + tableName;
    System.out.println("Running: " + sql);
    res = stmt.executeQuery(sql);
//    while (res.next()) {
//      System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
//    }
 
    // regular hive query
    sql = "select userid,concat_ws(',',collect_list(concat_ws(':',cast(movieid as string),cast(rating as string)))) as movierating from "+tableName+" group by userid";
    System.out.println("Running: " + sql);
      
    res = stmt.executeQuery(sql);
    File myObj = new File(args[1]);
   
    try {
    	boolean result = Files.deleteIfExists(myObj.toPath());
		if (myObj.createNewFile()) {
		    System.out.println("File created: " + myObj.getName());
		  } else {
		    System.out.println("File already exists.");
		  }
	} catch (IOException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
//    String fileName= "./movieratingbyuser/part-r-00000.txt";
    String fileName= args[1];
	FileOutputStream outputStream;
	try {
		outputStream = new FileOutputStream(fileName);
		 
	    while (res.next()) {
	      //System.out.println(res.getString(1)+" "+res.getString(2));
	      String output =  res.getString(1)+"\t"+res.getString(2)+"\n";
	      byte[] strToBytes = output.getBytes();
	      outputStream.write(strToBytes);
	    }
	    outputStream.close();
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
   
  }
}