package PreEnrollMentAnayltics;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class loadCSV {
	
	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://96.88.82.250:3306/plan_recommend_dev";
    private static final String USER = "root1";
    private static final String PASSWORD = "Dzee2015$";
    private String INSTRUCTIONS = new String();
    private static Connection connection = null;
    public String dzee_client,employerGroup, enrollment_year;
   // public final String fileName = "F:/PlanEnrollEclipse/PlanEnrollmentAnayltics/src/properties_pk/preEnrollmentStandard.csv";
    
  /*  public String deleteFromPreEnrollmentStandard= "Delete from pre_enrollment_analytics_standard where dzee_client=? and employer=? "
    		                                      + "and enrollment_year=? and analytic_type=1; ";
    
    public String loadPreEnrollmentStandardSql ="LOAD DATA local INFILE '"+fileName+"'"
    		                                    +" IGNORE "
                                                +" INTO TABLE pre_enrollment_analytics_standard "
                                                +" FIELDS TERMINATED BY '|' "
                                                +" ignore 1 lines; " ;*/
                                                
   
   
    public void loadCSV(String dzee_client,String employerGroup, String enrollment_year,String deleteSql,String loadCSVSql){
    	ResultSet rs = null;
        PreparedStatement stmt = null;
        try {
            stmt = connection.prepareStatement(deleteSql);
            stmt.setString(1, dzee_client);
            stmt.setString(2, employerGroup);
            stmt.setString(3, enrollment_year);
            stmt.execute();
            stmt = connection.prepareStatement(loadCSVSql);
            //System.out.println(stmt);
            rs = stmt.executeQuery();           
         } catch (SQLException se) {      //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {  //Handle errors for Class.forName
            e.printStackTrace();
        }
        
      } 
}
    
