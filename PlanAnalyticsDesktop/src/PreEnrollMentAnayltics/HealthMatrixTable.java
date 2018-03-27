package PreEnrollMentAnayltics;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

public class HealthMatrixTable {

	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://96.88.82.250:3306/plan_recommend_dev_qa";
    private static final String USER = "root1";
    private static final String PASSWORD = "Dzee2015$";
    private String INSTRUCTIONS = new String();
    public String dzee_client,employerGroup, enrollment_year;
    static private Connection connection = null;
    private ResultSet riskAnaylsisResultSet;
    ArrayList<Float> healthMatrix = new ArrayList<Float>();
    
    String queryDrop = "DROP PROCEDURE IF EXISTS Risk_Analysis";
    
    String riskAnalysisSql="CREATE PROCEDURE `Risk_Analysis`(IN dzee_client varchar(25), IN employerGroup varchar(25),IN enrollment_year varchar(25))"
    +" BEGIN"
    +" delete from stop_loss_risk_analysis;"
    +" CREATE TEMPORARY table IF NOT EXISTS tempdata AS"
    +" (select dzee_client,employer_group,plan_enrollment_year,health_profile,count(m.email_id) as count"
    +" from members m, employee e,employer em"
    +" where" 
    +" em.dzee_client=dzee_client"
    +" and em.employer_group=employerGroup"
    +" and e.plan_enrollment_year=enrollment_year"
    +" and e.employer_id = em.email_id"
    +" and e.email_id= m.email_id"
    +" and e.deleted = 0"
    +" group by health_profile"
    +" order by health_profile);"

    +" insert into stop_loss_risk_analysis" 
    +" (select dzee_client,employer_group,plan_enrollment_year,"
    +" s.health_profile,(round((s.25K * e.count)/100))*25000,(round((s.50K * e.count)/100))*50000,"
    +" (round((s.100K * e.count)/100))*100000,(round((s.250K * e.count)/100))*250000"
    +" from stop_loss_risk_assumption s,tempdata e"
    +" where e.health_profile = s.health_profile"
    +" order by health_profile);"
    +" END";
	
    
    public static void main(String[] args) throws Exception{

        System.out.println("Main entered");
        HealthMatrixTable   demo = new HealthMatrixTable ();
        System.out.println("Enter the Dzee client,Employer group and Enrollment year ");
        demo.dzee_client = args[0];
        demo.employerGroup = args[1];
        demo.enrollment_year = args[2];
        
        try {
        	
        	demo.databaseConnect(demo.JDBC_DRIVER,demo.URL,demo.USER,demo.PASSWORD);
        	demo.riskAnaylsisResultSet = demo.createRiskAnalysisTable(demo.riskAnalysisSql, demo.dzee_client, demo.employerGroup, demo.enrollment_year);
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
    
      
    public void createRiskAnalysisProcedure(){
		 Statement statement;
		try {
			statement = connection.createStatement();
			statement.execute(queryDrop);
			System.out.println(riskAnalysisSql);
			statement.execute(riskAnalysisSql);
			
		} catch (SQLException e) {
			e.printStackTrace();
		}	
	}

   public  ResultSet createRiskAnalysisTable(String query,String dzee_client, String employerGroup, String enrollment_year) throws SQLException{
   	
   	ResultSet rs = null;
   	createRiskAnalysisProcedure();
   	
   	CallableStatement statement = connection.prepareCall("{call Risk_Analysis(?, ?, ?)}");
   	 try{ 	 
          statement.setString(1, dzee_client);	 
   	      statement.setString(2, employerGroup);
          statement.setString(3, enrollment_year);
     	  statement.executeUpdate();
     	  rs=statement.getResultSet();
     	   /*while(rs.next()){
     		   System.out.println(rs.getString("email_id")+""+rs.getString("member_id"));
     	    }*/
        //statement.close();  	   
   	  } catch (SQLException e) { e.printStackTrace();	}  	
		 return rs;
      }  
	
}
