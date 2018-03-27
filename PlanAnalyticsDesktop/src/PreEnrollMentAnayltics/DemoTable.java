package PreEnrollMentAnayltics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Vector;

public class DemoTable {
	
	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://96.88.82.250:3306/plan_recommend_dev_qa";
    private static final String USER = "root1";
    private static final String PASSWORD = "Dzee2015$";
    private String INSTRUCTIONS = new String();
    public String dzee_client,employerGroup, enrollment_year;
    static private Connection connection = null;
    private ResultSet healthGradeCountResultSet,healthGradeDistributionResultSet,deleteSqlResultSet;
    ArrayList<Float> healthMatrix = new ArrayList<Float>();
    
    String healthGradeCount=" select health_profile,count(m.email_id) as count"
                            +" from members m, employee e,employer em"
                            +" where"
                            +" em.dzee_client=?"
                            +" and em.employer_group=?"
                            +" and e.plan_enrollment_year=?"
                            +" and e.employer_id = em.email_id"
                            +" and e.email_id= m.email_id"
                            +" and e.deleted = 0"
                            +" group by health_profile"
                            +" order by health_profile";
    
    
    String healthGradeDistribution = "Select * from healthGrade_distribution ";
    
    String deleteSql = "delete from health__matrix ";
    
    String insertSql = "insert into health__matrix(dzee_client,employer_group,plan_enrollment_year,health_profile,25K,50K,100K,250K) "
                       + " values (?,?,?,?,?,?,?,?)";

    @SuppressWarnings("static-access")
	public static void main(String[] args) throws Exception{

        System.out.println("Main entered");
        DemoTable   demo = new DemoTable ();
        System.out.println("Enter the Dzee client,Employer group and Enrollment year ");
        demo.dzee_client = args[0];
        demo.employerGroup = args[1];
        demo.enrollment_year = args[2];
        try {
        	
        	demo.databaseConnect(demo.JDBC_DRIVER,demo.URL,demo.USER,demo.PASSWORD);
        	demo.deleteSqlResultSet = demo.selectFromTable(demo.deleteSql);
        	demo.healthGradeCountResultSet = demo.creatingStatement(demo.healthGradeCount, demo.dzee_client, demo.employerGroup, demo.enrollment_year);
        	demo.healthGradeDistributionResultSet=demo.selectFromTable(demo.healthGradeDistribution);
        	demo.getValues(demo);
           }catch (SQLException se) {      //Handle errors for JDBC
            se.printStackTrace();
            //demo.connection.rollback();
        }finally {
     	 if (connection != null) {
     		// connection.setAutoCommit(true);
             connection.close();
             System.out.println("Connection Closed");
              
        }
     }
 }
    
   
	public void databaseConnect(String JDBC_DRIVER,String URL,String USER,String PASSWORD) {
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException var9) {
            System.out.println("Where is your MySQL JDBC Driver?");
            var9.printStackTrace();
            return;
        }

        System.out.println("MySQL JDBC Driver Registered!");
        try {
            connection = getConnection(URL,USER,PASSWORD);
            } catch (SQLException var8) {
            var8.printStackTrace();
        } finally {
            if (connection != null) {
                System.out.println("Database Connection successful!");
            } else {
                System.out.println("Failed to make connection!"
                		+ "");
            }
        }
    }

 // connecting to database
    public Connection getConnection(String URL,String USER,String PASSWORD) throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }
    
  //executing the sql query
    public ResultSet creatingStatement(String query, String param1, String param2, String param3) throws SQLException {
        ResultSet rs = null;
        PreparedStatement stmt = null;

        //System.out.println("Creating statement...");
        try {

            stmt = connection.prepareStatement(query);
            stmt.setString(1, param1);
            stmt.setString(2, param2);
            stmt.setString(3, param3);
         // rs = stmt.executeQuery();
            stmt.execute();
            rs = stmt.getResultSet();
         } catch (SQLException se) {      //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {  //Handle errors for Class.forName
            e.printStackTrace();
        }
        return rs;
    }
    
    public ResultSet selectFromTable(String querySelect){
    	ResultSet rs = null;
        Statement stmtSelect = null;
        try {
            stmtSelect = connection.createStatement();
            stmtSelect.execute(querySelect);
            rs=stmtSelect.getResultSet();
        } catch (SQLException se) {      //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {  //Handle errors for Class.forName
            e.printStackTrace();
        }
        return rs;
    }//close method
    
    //insert into table
    public void insertIntoTable(ArrayList x,DemoTable demo){
    	 
    	int i=0;
  		try {
  			
            	// create the MySql insert prepared statement
    			PreparedStatement preparedStmt = connection.prepareStatement(insertSql);
                preparedStmt.setString(1, demo.dzee_client);
                preparedStmt.setString(2, demo.employerGroup);
                preparedStmt.setString(3, demo.enrollment_year);
                preparedStmt.setInt(4, demo.healthGradeCountResultSet.getInt("health_profile"));
                preparedStmt.setFloat(5,(float)healthMatrix.get(0));
                preparedStmt.setFloat(6, (float)healthMatrix.get(1));
                preparedStmt.setFloat(7, (float) healthMatrix.get(2));
                preparedStmt.setFloat(8, (float)healthMatrix.get(3));
                preparedStmt.execute();
    		 } catch (Exception e) {  //Handle errors for Class.forName
              e.printStackTrace();
            }
    	  
    	}
    
    private void getValues(DemoTable demo) throws SQLException {
    	
    	int j =2;
    	ResultSetMetaData metaData = demo.healthGradeDistributionResultSet.getMetaData();
    	int count = metaData.getColumnCount();
    	System.out.println(count);
    	try {
			demo.healthGradeCountResultSet.beforeFirst();
			demo.healthGradeDistributionResultSet.first();
			while (demo.healthGradeCountResultSet.next()){
               System.out.println(demo.healthGradeDistributionResultSet.getInt("health_profile"));
	            if(demo.healthGradeCountResultSet.getInt("health_profile") == demo.healthGradeDistributionResultSet.getInt("health_profile")){
	             do{	
	        	  float healthGradeValue = Math.round((float) ((demo.healthGradeCountResultSet.getInt("count")* demo.healthGradeDistributionResultSet.getInt(j))/100.0)); 
	        	  String label= metaData.getColumnLabel(j);
	        	  
	        	  int income =(Integer.parseInt(label))*1000;
	        	  System.out.println(healthGradeValue);
	        	  healthMatrix.add(healthGradeValue*income);
	        	  j++;  
	             }while(j <=count);
	            }
	            else{
	            demo.healthGradeDistributionResultSet.next();}
	           
                demo.insertIntoTable(healthMatrix,demo);
                j=2;
                healthMatrix.clear();
                demo.healthGradeDistributionResultSet.next();
             }
		   }catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		  }
    		
	}//close method

    
   
}
