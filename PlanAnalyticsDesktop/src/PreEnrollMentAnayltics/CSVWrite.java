package PreEnrollMentAnayltics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;

public class CSVWrite {
	
	private static final String COMMA_DELIMITER = "|";
	private static final String NEW_LINE_SEPARATOR = "\n";
	 
	//CSV file header
	 private final String FILE_HEADER = "dzee_client,employer, enrollment_year,employee_email_id,"
	 		+ "plan_id,total_cost_based_plan_rank,employer_total_cost_based_plan_rank,"
	 		+ "oop,monthly_premium,annual_healthcare_premium_plus_OOP,"
	 		+ "total_family_service_cost,annual_supplement_plan_premium,"
	 		+ "hsa_contribution,fsa_contribution,tax_adjusted_OOP,analytic_type, "
            + "employer_premium_contribution, employer_oop_contribution ";
	 
	
	 
	  public static File f;
	  public static FileWriter fw = null;	
	  public static BufferedWriter writer;
	  public String outf;
	 
	 public void writeHeading(String filePath) throws IOException{
		
		//System.out.println("Write heading into CSV file:");	              
      	try{
	     
		 f = new File(filePath);	
		 fw = new FileWriter(f);	
		 writer = new BufferedWriter(fw);
		 writer.write(FILE_HEADER);
		 writer.write(NEW_LINE_SEPARATOR);	  
		 writer.close();
         System.out.println("Header writing Success");
      	 }catch(Exception e){
		    System.out.println(e+ "Exception ");    
		 }       	
	}
	
	public void writeToCsv(Vector<PlanDetail> planDetailVector,String filePath) throws IOException{
		
		
		fw = new FileWriter(f,true);
	    writer = new BufferedWriter(fw);
	   	for(int i = 0; i< planDetailVector.size(); i++){
				
				writer.write(String.valueOf(planDetailVector.get(i).getdzee_client()));
				writer.write(COMMA_DELIMITER);
				writer.write(String.valueOf(planDetailVector.get(i).getemployer_group()));
				writer.write(COMMA_DELIMITER);
			    writer.write(String.valueOf(planDetailVector.get(i).getEnrollment_Year()));
			    writer.write(COMMA_DELIMITER);
			    //System.out.println(planDetailVector.get(i).getEmployee_Email_ID());
			    writer.write(String.valueOf(planDetailVector.get(i).getEmployee_Email_ID()));
			    writer.write(COMMA_DELIMITER);
			    //System.out.println(planDetailVector.get(i).gethealthcare_plan_id());
			    writer.write(String.valueOf(planDetailVector.get(i).gethealthcare_plan_id()));
			    writer.write(COMMA_DELIMITER);
			    writer.write(String.valueOf(planDetailVector.get(i).getEmployee_Rank()));//employee rank
			    writer.write(COMMA_DELIMITER);
			    writer.write(String.valueOf(planDetailVector.get(i).getpremium_Rank()));//premium rank
			    writer.write(COMMA_DELIMITER);
			    writer.write(String.valueOf(planDetailVector.get(i).getOOP()));//OOP
			    writer.write(COMMA_DELIMITER);
			    writer.write(String.valueOf(planDetailVector.get(i).getPremium()));//premium
			    writer.write(COMMA_DELIMITER);
			    writer.write(String.valueOf(planDetailVector.get(i).getpremium_OOP()));//premium+OOP
			    writer.write(COMMA_DELIMITER);
			    writer.write(String.valueOf(planDetailVector.get(i).getTotal_Consumption()));////total consumption
			    writer.write(COMMA_DELIMITER);
			    writer.write(String.valueOf(0));//Annual supplement plan premium
			    writer.write(COMMA_DELIMITER);
			    writer.write(String.valueOf(planDetailVector.get(i).getHSAContribution()));//HSAContribution
			    writer.write(COMMA_DELIMITER);
			    writer.write(String.valueOf(planDetailVector.get(i).getFSAContribution()));//FSAContribution
			    writer.write(COMMA_DELIMITER);
			    writer.write(String.valueOf(planDetailVector.get(i).getTaxAdjustedOOP()));//TaxAdjustedOOP
			    writer.write(COMMA_DELIMITER);
			    writer.write(String.valueOf(1));//Analytic_Type
			    writer.write(COMMA_DELIMITER);
			    writer.write(String.valueOf(planDetailVector.get(i).getEmployerPremiumContribution()));//EmployerPremiumContribution
			    writer.write(COMMA_DELIMITER);
			    writer.write(String.valueOf(1));//EmployerOOPContribution
			    writer.write(NEW_LINE_SEPARATOR);
	      }
			writer.close();   
  }//method close
 
}//class close
