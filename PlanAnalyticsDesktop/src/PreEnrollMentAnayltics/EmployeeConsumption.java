package PreEnrollMentAnayltics;



import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;



public class EmployeeConsumption {
	
	private String queryDrop = "DROP PROCEDURE IF EXISTS employee_consumption";
	
	private String memberDetailsql="(select m.email_id , 0 as family_consumption, m.member_id, b.total_consumption as individual_consumption,e.plan_enrollment_year"
			+" from benefit_consumption b, members m, employee e, active_states ac, employer emp"
			+" where"
			+" emp.dzee_client= ? "
			+" and emp.employer_group= ? "
			+" and e.plan_enrollment_year= ? " 
			+" and emp.email_id = e.employer_id and m.email_id = e.email_id"
			+" and m.gender = b.gender and m.health_profile = b.health_profile "
			+" and b.service_year = e.plan_enrollment_year and b.location = ac.states" 
			+" and m.age between b.age_from and b.age_to and m.location = ac.states"
			+" group by email_id, member_id,individual_consumption,e.plan_enrollment_year);";
 			
    
	public void createEmployeeConsumptionStoreProcedure(Connection connection){
		 Statement statement;
		try {
			statement = connection.createStatement();
			statement.execute(queryDrop);
			System.out.println(memberDetailsql);
			statement.execute(memberDetailsql);
			
		} catch (SQLException e) {
			e.printStackTrace();
		}	
	}

    public  ResultSet createEmployeeConsumptionTable(String dzee_client, String employerGroup, String enrollment_year,Connection connection) throws SQLException{
    	
    	ResultSet rs = null;
    	createEmployeeConsumptionStoreProcedure(connection);
    	CallableStatement statement = connection.prepareCall("{call employee_consumption(?, ?, ?)}");
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
