package PreEnrollMentAnayltics;


import java.sql.PreparedStatement;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Vector;

import com.mysql.jdbc.Util;

import PreEnrollMentAnayltics.EmployeeConsumption;
import PreEnrollMentAnayltics.CSVWrite;

public class PlanAnalytics {
	
	private final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private final String URL = "jdbc:mysql://dzanalytics.dzeecloud.com:3306/plan_recommend";
    private final String USER = "root1";
    private final String PASSWORD = "Dzee2015$";
    static Connection connection = null;
    private ResultSet planResultSet, memResultSet,globalResultSet,employerPremiumContribution,hSAFSAResultSet,employeeConsumptionResultSet;
    public String dzee_client,employerGroup, enrollment_year;
    static ArrayList<Integer> list = new ArrayList<Integer>();
    int taxArray[][] = new int[2][5];
    ArrayList<planInfo> premiumList = new ArrayList<planInfo>();
    ArrayList<planInfo> premium_OOP_List = new ArrayList<planInfo>();
    Vector<PlanDetail> planDetailVector = new Vector<PlanDetail>();
    static CSVWrite filewrite = new CSVWrite();
    int noEmpRecords=0;
    int noOfPlanRecords = 0;
    public String filePath,fileName; 
    public final static String outdir = System.getProperty("user.dir")+ "/properties_pk";
	public static final String outstr = "preEnrollmentStandard";
    public final String outfilePath = "/Users/ratnakar/Documents/DZEE/PreEnrollmentAnalytics/src/properties_pk/";
  

    class planInfo implements Comparable<planInfo>{
        String healthcare_plan_id;
        double premium_OOP;
        int planRank;

        public planInfo(){}

        public planInfo(double premium,String plan_id) {
            this.premium_OOP = premium;
            this.healthcare_plan_id = plan_id;
        }

        public void setPlanRank(planInfo planinfo, int rank) {
            planinfo.planRank = rank;
        }
       @Override
        public int compareTo(planInfo pl) {
           if (premium_OOP == pl.premium_OOP)
               return 0;
           else if (premium_OOP > pl.premium_OOP)
               return 1;
           else
               return -1;
        }
    }
         String memSql=" select emp.email_id, m.member_id,gender,relation,health_profile,age,tobacco_usage,annual_income,empcon.individual_consumption,emp.plan_enrollment_year"
                +" from employee emp, members m, employee_consumptiontemp empcon, employer emplr"
                +" where "
                +" m.member_id = empcon.member_id "
                +" and m.email_id = emp.email_id "
                +" and empcon.email_id= m.email_id"
                +" and emp.plan_enrollment_year= empcon.plan_enrollment_year"
                +" and emp.employer_id = emplr.email_id"
                +" and ((m.member_id < 1) or (m.member_id = 1 and m.relation = 'Spouse' and m.age < 65) or (m.member_id > 0 and m.relation = 'Child' and m.age <= 26))"
                +" and emplr.dzee_client = ? "
                +" and emplr.employer_group= ? "
                +" and emp.plan_enrollment_year= ? "
                +" and emp.deleted=false "
    			+" and emplr.deleted=false "
    			+" order by emp.email_id,m.member_id ";

         String planSql = "select dzee_client,employer_group , healthcare_plan_id, minexpense,total_copay,"
                + " individual_in_network_deductible_limit, family_in_network_deductible_limit, "
                + " individual_in_network_oop_limit, family_in_network_oop_limit, plan_level_co_insurance, detailed_premium, opd.enrollment_year "
                + " from organization_plans_data opd, financial_plan_attributes fp "
                + " where "
                + " fp.plan_id = opd.healthcare_plan_id "
                + " and opd.enrollment_year = fp.service_year"
                + " and opd.is_deleted <> 1"
                + " and dzee_client = ? "
                + " and opd.employer_group= ? "
                + " and enrollment_year = ? "
                + " and opd.is_deleted=false "
    			+ " and opd.dzee_approved=true";

        //updated planCopaySql on 03/20/2018 
        String planCopaySql = "SELECT total_co_pay "
        		+ " FROM plan_copay pc, "
        		+ " organization_plans_data opd,active_states ac"
                + " where "
                + " plan_id = ? "
                + " and gender = ?"
                + " and health_profile = ? "
                + " and ? between min_age_range and max_age_range"
                + " and service_year = ?"
                + " and opd.healthcare_plan_id = pc.plan_id"
                + " and (ac.states = opd.state) Or (opd.state = 'All' and ac.states='USA')"
                + " and ac.states = pc.location"
                + " and opd.employer_group =? "
                + " and opd.dzee_client = ? ";


        String detailPremiumSql = "select emp.email_id, org.healthcare_plan_id,sum(ag.premium) as premiums,org.enrollment_year "
                + " from organization_plans_data org,employee emp,age_wise_premiums ag,members m "
                + " where org.healthcare_plan_id = ag.healthcare_plan_id "
                + " and m.email_id=emp.email_id "
                + " and ((m.member_id < 1) or (m.member_id = 1 and m.relation = 'Spouse' and m.age < 65) or (m.member_id > 0 and m.relation = 'Child' and m.age <= 26))"
                + " and m.age between ag.age_from and ag.age_to "
                + " and org.enrollment_year = emp.plan_enrollment_year"
                + " and org.healthcare_plan_id = ag.healthcare_plan_id"
                + " and org.employer_group = ag.employer_group"
                + " and org.dzee_client = ag.dzee_client"
                + " and org.dzee_client= ?"
                + " and org.employer_group= ?"
                + " and org.healthcare_plan_id = ? "
                + " and emp.email_id= ? "
                + " and org.enrollment_year = ? "
                + " and org.detailed_premium=1 ";

         String premiumSql = "select temp.email_id,healthcare_plan_id,enrollment_year,"
                 +" ( "
                 +" CASE" 
                 +" WHEN(temp.spouse ='0' && temp.number_of_children='0') THEN opd.primary_healthcare_plan_premium "
                 +" WHEN(temp.spouse ='0' && temp.number_of_children= '1') THEN opd.primary_one_child_healthcare_plan_premium" 
                 +" WHEN(temp.spouse ='0' && temp.number_of_children= '2') THEN opd. primary_two_children_healthcare_plan_premium "
                 +" WHEN(temp.spouse ='0' && temp.number_of_children>= '3') THEN opd.primary_three_and_more_children_healthcare_plan_premium "
                 +" WHEN(temp.spouse ='1' && temp.number_of_children= '0') THEN opd.couple_healthcare_plan_premium "
                 +" WHEN(temp.spouse ='1' && number_of_children = '1') THEN opd.couple_one_child_healthcare_plan_premium"
                 +" WHEN(temp.spouse ='1' && temp.number_of_children= '2') THEN opd.couple_two_children_healthcare_plan_premium" 
                 +" WHEN(temp.spouse ='1' && temp.number_of_children>= '3') THEN opd.couple_three_children_and_more_healthcare_plan_premium "
                 +" ELSE 1 "
                 +" END)as premiums "
                 +" from "
                 +" (select m.email_id, m.member_id,m.tobacco_usage,m.relation,m.age, sum(spouse) as spouse, sum(number_of_children) as number_of_children"
                 +" from "
                 +" (select  email_id, "
                 +" if(m.relation='Spouse', 1,0) spouse,"
                 +" if(m.relation='Child',1,0) number_of_children "
                 +" from members m"
                 +" where"
                 +" ((m.member_id < 1) or (m.member_id = 1 and m.relation = 'Spouse' and m.age < 65) or (m.member_id > 0 and m.relation = 'Child' and m.age <= 26))"
                 +" and m.email_id= ? ) as tempdata , members m"
                 +" where m.email_id = tempdata.email_id and member_id = 0 group by email_id) as temp ,organization_plans_data opd"
                 +" where"
                 +" opd.dzee_client = ?"
                 +" and opd.employer_group= ?"
                 +" and opd.enrollment_year= ? "
                 +" and opd.healthcare_plan_id = ? "
                 +" and opd.is_deleted=false" 
                 +" and opd.dzee_approved=true" 
                 +" order by temp.email_id,temp.member_id";

       String enrollmentSql = "insert into pre_enrollment_analytics_standard (dzee_client,employer,enrollment_year,employee_email_id,plan_id,total_cost_based_plan_rank,employer_total_cost_based_plan_rank, "
                + "oop,monthly_premium,annual_healthcare_premium_plus_OOP,total_family_service_cost,annual_supplement_plan_premium,hsa_contribution,fsa_contribution,tax_adjusted_OOP,analytic_type, "
                + "employer_premium_contribution, employer_oop_contribution) "
                + " values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

       String queryDelete = "Delete from pre_enrollment_analytics_standard where dzee_client=? and employer=? and enrollment_year=? and analytic_type=1";
       
       String employerContriSql = " SELECT employer_id,enrollment_year,contribution_type,fsa_single,fsa_family,hsa_single,hsa_family,premium_single,premium_family "
       		                     + " from employer emplyr,employer_contribution empContri "
       		                     + " where "
       		                     + " emplyr.email_id = empContri.employer_id"
    		                     + " and emplyr.dzee_client = ?"
    		                     + " and emplyr.employer_group = ?"
    		                     + " and empContri.enrollment_year  = ? "
    		                     + " and emplyr.deleted=false";        
              
       String organisationPlanSql = " SELECT * FROM organization_plans_data "
    		                        + " where "
                                    + " dzee_client = ?"
                                    + " and employer_group = ?"
                                    + " and enrollment_year = ? "
                                    + " and dzee_approved=true "
                                	+ " and is_deleted=false";   
       
       String taxBracketSql = " SELECT * FROM tax_brackets ";
       
       String globalAssumptionsSql = "SELECT * FROM global_assumptions "; 
 
       //Updated employee_consumptiontempSql…on 03/18/2018 Removed ac.states='Colorado’condition
       String employee_consumptiontempSql ="CREATE TEMPORARY table IF NOT EXISTS employee_consumptiontemp AS (select m.email_id ,m.member_id, b.total_consumption as individual_consumption,e.plan_enrollment_year"
       	    +" from benefit_consumption b, members m, employee e, active_states ac, employer emp"
       		+" where"
       		+" emp.dzee_client= ?"
       		+" and emp.employer_group= ?"
       		+" and e.plan_enrollment_year= ?"
       		+" and emp.email_id = e.employer_id and m.email_id = e.email_id"
       		+" and m.gender = b.gender and m.health_profile = b.health_profile"
       		+" and b.service_year = e.plan_enrollment_year"
       		+" and m.age between b.age_from and b.age_to"
       		+" and b.location=ac.states"
       		+" and emp.deleted=false "
			+" and e.deleted=false "
       		+" group by email_id, member_id,individual_consumption,e.plan_enrollment_year);";      
                
       public String loadPreEnrollmentStandardSql ="LOAD DATA local INFILE '"+"/Users/ratnakar/Documents/DZEE/PreEnrollmentAnalytics/src/properties_pk/"+outstr+'_'+employerGroup+"'"
       		                                       +" IGNORE "
                                                   +" INTO TABLE pre_enrollment_analytics_standard "
                                                   +" FIELDS TERMINATED BY '|' "
                                                   +" ignore 1 lines; " ;
       		                  

       public static void main(String[] args) throws Exception {


        	PlanAnalytics empObj = new PlanAnalytics();
        	System.out.println("OUT DIR"+outdir);
        	System.out.println("Enter the Dzee client,Employer group and Enrollment year ");
            empObj.dzee_client = args[0];
            empObj.employerGroup = args[1];
            empObj.enrollment_year = args[2];
                    
            try {
            	empObj.filePath = empObj.outdir +"/" + empObj.outstr + "_" + empObj.employerGroup + ".csv";
            	filewrite.writeHeading(empObj.filePath);
            	empObj.fileName = empObj.filePath.substring(empObj.filePath.lastIndexOf("/")+1);
            	System.out.println(empObj.fileName);
            	empObj.databaseConnect(empObj.JDBC_DRIVER,empObj.URL,empObj.USER,empObj.PASSWORD);
                empObj.employeeConsumptionResultSet = empObj.creatingStatement(empObj.employee_consumptiontempSql, empObj.dzee_client, empObj.employerGroup, empObj.enrollment_year);
                empObj.memResultSet = empObj.creatingStatement(empObj.memSql, empObj.dzee_client, empObj.employerGroup, empObj.enrollment_year);
                empObj.planResultSet = empObj.creatingStatement(empObj.planSql, empObj.dzee_client,empObj.employerGroup, empObj.enrollment_year);
                empObj.deleteFromPlanEnrollmentAnalytics(empObj.queryDelete,empObj.dzee_client,empObj.employerGroup, empObj.enrollment_year);
                empObj.globalResultSet = empObj.executeSqlQuery(empObj.globalAssumptionsSql);
                empObj.hSAFSAResultSet = empObj.creatingStatement(empObj.organisationPlanSql,empObj.dzee_client,empObj.employerGroup, empObj.enrollment_year);
                empObj.employerPremiumContribution = empObj.creatingStatement(empObj.employerContriSql,empObj.dzee_client,empObj.employerGroup,empObj.enrollment_year);
                empObj.taxArray = empObj.fetchfromTaxBrackets(empObj.taxBracketSql);
                empObj.noEmpRecords = empObj.getCountofRecords(empObj.memResultSet);
                System.out.println("No of Employees:"+empObj.noEmpRecords);
                empObj.noOfPlanRecords = empObj.getCountofRecords(empObj.planResultSet);
                System.out.println("No of plans:"+empObj.noOfPlanRecords);
                if(empObj.noEmpRecords == 0 || empObj.noOfPlanRecords ==0 ){
                	throw new Exception ("No plans available for the employee or no employee's assigned for the plan");          	             			
                }
                list = empObj.createIndex(empObj.memResultSet);
                empObj.getEmployeeExpenses(empObj,list);
               }catch (SQLException se) {      //Handle errors for JDBC
                   se.printStackTrace();
                   connection.rollback();
               }finally {
            	 if (empObj.connection != null) {
            	//empObj.deleteCSV(empObj.filePath);
            		connection.setAutoCommit(true);
                    empObj.connection.close();
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
                    System.out.println("Failed to make connection!");
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
  
 //executing global assumptions query and tax bracket query
        public ResultSet executeSqlQuery(String query ){
        	ResultSet rs = null;
            PreparedStatement stmt = null;
            
            try {
                stmt = connection.prepareStatement(query);
                rs = stmt.executeQuery();
                } catch (SQLException se) {      //Handle errors for JDBC
                se.printStackTrace();
                } catch (Exception e) {  //Handle errors for Class.forName
                e.printStackTrace();
            }
            return rs;
        }//method close
       
   	        
// get the number of records in the plan result set and member result set
      public int getCountofRecords(ResultSet rs) throws SQLException {
            int numberOfRecords = 0;
            boolean b = rs.last();
            if (b) {
                numberOfRecords = rs.getRow();
                System.out.println(numberOfRecords);
            }
            return numberOfRecords;
        }//method close

 //creating array index for member = 0 
      public ArrayList<Integer> createIndex(ResultSet memResultSet) throws SQLException {
            memResultSet.beforeFirst();
            ArrayList<Integer> list = new ArrayList<Integer>();
            while (memResultSet.next()) {
            	 if (Integer.parseInt(memResultSet.getString("member_id")) == 0) {
                    list.add(memResultSet.getRow());
                }
            }
            return list;
        }//close method

 //delete from planEnrollment table before inserting
     public void deleteFromPlanEnrollmentAnalytics(String queryDrop,String dzee_client,String employerGroup, String enrollment_year) {
          ResultSet rs = null;
        PreparedStatement stmt = null;

        //System.out.println("Creating statement...");
        try {

            stmt = connection.prepareStatement(queryDrop);
            stmt.setString(1, dzee_client);
            stmt.setString(2, employerGroup);
            stmt.setString(3, enrollment_year);
            stmt.execute();
        } catch (SQLException se) {      //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {  //Handle errors for Class.forName
            e.printStackTrace();
        }    
        
    }//close method

 //insert into the table plan enrollment analytics
     public void insertPlanEnrollmentAnalytics(Vector<PlanDetail> planDetailVector, ArrayList<planInfo> premiumList,ArrayList<planInfo> premium_OOP_List) throws IOException{

        int TotalCostbasedPlanRank = 0;
        int PremiumbasedPlanRank = 0;
        
        for (int i = 0; i < planDetailVector.size(); i++) {
            for (int j = 0; j < premiumList.size(); j++) {
              if (premiumList.get(j).healthcare_plan_id.equals(planDetailVector.get(i).gethealthcare_plan_id()))
                {
            	  PremiumbasedPlanRank = premiumList.get(j).planRank;
                }
            }
            for (int k = 0; k < premium_OOP_List.size(); k++) {
             if (premium_OOP_List.get(k).healthcare_plan_id.equals(planDetailVector.get(i).gethealthcare_plan_id()))
                {
            	 TotalCostbasedPlanRank = premium_OOP_List.get(k).planRank;
                }
            }
            planDetailVector.get(i).setPlanRanks(TotalCostbasedPlanRank ,PremiumbasedPlanRank);                 
        
        }
        filewrite.writeToCsv(planDetailVector,filePath);//write to CSV
        
    }//method close

 
// calculate the net expenses for each plan 
       public void getEmployeeExpenses(PlanAnalytics empObj, ArrayList<Integer> list) throws SQLException, IOException {
            double DmonthlyPremium = 0.0;
            int RmonthlyPremium=0;
            double AnnualHealthcarePremiumPlusOOP =0.0;
            int OOP = 0;
            int recommendedhsaFsaConribution[][] = new int[2][1];
            double employerPremiumContri=0.0;
            int taxAdjustedOOP;
            int employerCost=0;
            int annualEmployerPremiumContribution=0;
            int employerHSAFSAContribution=0; 
                       	
            try{
             
              connection.setAutoCommit(false);	
             // for each member loop    
             for (int i = 0; i <  list.size(); i++) {
                empObj.memResultSet.absolute((Integer) list.get(i));
                String email = empObj.memResultSet.getString("email_id");
                //call to calculate employer premium contribution
                empObj.planResultSet.beforeFirst();
                while (empObj.planResultSet.next()) { // for each plan for that employer group
                    //String plan_id = empObj.planResultSet.getString("healthcare_plan_id");
                    String plan_id = empObj.planResultSet.getString(3);
                    //System.out.println("plan_id"+plan_id);
                    //call calculate OOP for for each member for each plan
                    OOP = empObj.calculateExpenses(planResultSet, memResultSet, (Integer)list.get(i), email);
                    //System.out.println("OOP"+OOP);
                    //empObj.memResultSet.absolute((Integer)list.get(i));
                    DmonthlyPremium =  getPremium(dzee_client,employerGroup,enrollment_year,plan_id, email, (Integer.parseInt(empObj.planResultSet.getString("detailed_premium"))));
                    RmonthlyPremium = (int) Math.round(DmonthlyPremium);
                    employerPremiumContri = empObj.calculateEmployerPremiumContribution(email,memResultSet,planResultSet,employerPremiumContribution,(Integer)list.get(i),RmonthlyPremium);
                    employerHSAFSAContribution=getEmployerHSAFSAContibution(memResultSet,planResultSet,hSAFSAResultSet,employerPremiumContribution,(Integer) list.get(i), email);
                    recommendedhsaFsaConribution = CalculateRecommendedHsaFsaContibution(OOP,planResultSet, memResultSet,hSAFSAResultSet,employerPremiumContribution,globalResultSet,employerHSAFSAContribution,(Integer) list.get(i), email);
                    taxAdjustedOOP = calculateTaxAdjustedOOP(memResultSet,planResultSet,hSAFSAResultSet,employerHSAFSAContribution,(Integer)list.get(i),recommendedhsaFsaConribution,taxArray,OOP);
                    AnnualHealthcarePremiumPlusOOP = (DmonthlyPremium- employerPremiumContri)*12 +taxAdjustedOOP;
                    //AnnualHealthcarePremiumPlusOOP = RmonthlyPremium*12+OOP;
                    annualEmployerPremiumContribution= (int) (employerPremiumContri * 12);
                    empObj.addToList(plan_id, employerPremiumContri, AnnualHealthcarePremiumPlusOOP,empObj);
                    int family_consumption = empObj.getFamilyConsumption(memResultSet,(Integer) list.get(i), email);
                    empObj.memResultSet.absolute((Integer)list.get(i));
                    planDetailVector.addElement(new PlanDetail(dzee_client,employerGroup,Integer.parseInt(enrollment_year),email,plan_id,OOP,DmonthlyPremium,
                    AnnualHealthcarePremiumPlusOOP,family_consumption,recommendedhsaFsaConribution[0][0],
                    recommendedhsaFsaConribution[1][0],taxAdjustedOOP,employerPremiumContri));                    
                }
                //printList(email,empObj.premiumList,empObj.premium_OOP_List);
                empObj.planRank(empObj.premiumList,empObj.premium_OOP_List);
                empObj.insertPlanEnrollmentAnalytics(planDetailVector,empObj.premiumList,empObj.premium_OOP_List);
                empObj.clearAll(premiumList,premium_OOP_List,planDetailVector);
            }
             empObj.loadCSV(empObj.fileName);
             connection.commit();
            } catch (SQLException se) {      //Handle errors for JDBC
            	se.printStackTrace();
               /* try {
					connection.rollback();
                } catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
            
            }
            
        }//method close
       
       public int getFamilyConsumption(ResultSet memResultSet,int rowNum, String email) throws NumberFormatException, SQLException{
    	   int memberCount=0;
    	   int family_consumption=0;
    	   memResultSet.absolute(rowNum);
    	   do{
    	   //System.out.println(memResultSet.getString("email_id"));
    		   if(memResultSet.getString("email_id").equals(email)){
    			   if(((memResultSet.getString("relation").equalsIgnoreCase("Spouse")) && (Integer.parseInt(memResultSet.getString("age")) >= 65 )||
    				   ((memResultSet.getString("relation").equalsIgnoreCase("Child")) && (Integer.parseInt(memResultSet.getString("age")) >26 ))))
    		       {
    				continue;   
    			    }
    			   else{
    			   memberCount++;
    			   family_consumption += Integer.parseInt(memResultSet.getString("individual_consumption"));
    			   }
    		   }  			   
    	  }while(memResultSet.next()); 	       
    	   return family_consumption;  	   
       }
 
 //Calculate suggested HSA/FSA Contribution 
        public int[][] CalculateRecommendedHsaFsaContibution(int OOP, ResultSet planResultSet, ResultSet memResultSet,
        		        ResultSet hSAFSAResultSet,ResultSet employerPremiumContribution,ResultSet globalResultSet,int employerHSAFSAContribution,int rowNum, String email) throws SQLException {
		// TODO Auto-generated method stub
         String hsaFsaflag = "";
         String plan_id = planResultSet.getString(3);
         int memberCount = 0;
         int HSA_FSAContributionArray[][] = new int[2][1];
         int FSA_Max_limit_Family=0;
         int FSA_Max_limit_Individual=0;
         int HSA_Max_limit_Family=0;
         int HSA_Max_limit_Individual=0;
         int carryForward =0;
         int age =0;
         hSAFSAResultSet.beforeFirst();
         while(hSAFSAResultSet.next()){
        	 if(plan_id.equals(hSAFSAResultSet.getString(3))){
        		 hsaFsaflag = hSAFSAResultSet.getString(33);
        		 break;
          	 }
         }
         globalResultSet.first();
         while(globalResultSet.next()){
              if(globalResultSet.getString(1).equals("FSA Max limit - Family(Employer+Employee)"))
                 FSA_Max_limit_Family = Integer.parseInt(globalResultSet.getString(2));
              if(globalResultSet.getString(1).equals("FSA Max Limit - Individual(Employer+Employee)"))
            	  FSA_Max_limit_Individual =  Integer.parseInt(globalResultSet.getString(2));
              if(globalResultSet.getString(1).equals("HSA Max limit - Family(Employer+Employee)"))
            	  HSA_Max_limit_Family =  Integer.parseInt(globalResultSet.getString(2));
              if(globalResultSet.getString(1).equals("HSA Max limit - Individual (Employer+Employee)"))
                 HSA_Max_limit_Individual =Integer.parseInt(globalResultSet.getString(2));
             }
        
         memResultSet.absolute(rowNum);
         do{
             //compare email ids of primary and members to identify the number of members in the family
            if (memResultSet.getString("email_id").equals(email)) {
                 memberCount++;
                 if(memResultSet.getInt("member_id")== 0){
            	 age = memResultSet.getInt("Age");
               }
             }         
           }while(memResultSet.next()); 
         
            if(hsaFsaflag.equalsIgnoreCase("F")){
            	HSA_FSAContributionArray[0][0]= 0;
               if(memberCount > 1 ){
            	   if(OOP >= FSA_Max_limit_Family){
            		   HSA_FSAContributionArray[1][0] = FSA_Max_limit_Family - employerHSAFSAContribution;
            		 
            	     }else{
            	    	HSA_FSAContributionArray[1][0] = OOP - employerHSAFSAContribution;

            	     }
            	   carryForward = FSA_Max_limit_Family- OOP;
            	   
            	   if(carryForward >= 500){
            	     if(HSA_FSAContributionArray[1][0] + 500 <= FSA_Max_limit_Family){
            	        HSA_FSAContributionArray[1][0] = HSA_FSAContributionArray[1][0]+500;
                     }else{
                       HSA_FSAContributionArray[1][0] = FSA_Max_limit_Family;
                     }
                   }else if(carryForward < 500 & carryForward >= 0) {
                	   
                	   if(HSA_FSAContributionArray[1][0] + carryForward <= FSA_Max_limit_Family){
               	          HSA_FSAContributionArray[1][0] = HSA_FSAContributionArray[1][0]+carryForward;
                        }else{
                          HSA_FSAContributionArray[1][0] = FSA_Max_limit_Family;
                        }
                      }
                      else if(carryForward < 0) {
                   	     if(HSA_FSAContributionArray[1][0] + 0 <= FSA_Max_limit_Family){
               	            HSA_FSAContributionArray[1][0] = HSA_FSAContributionArray[1][0]+ 0;
                        }else{
                            HSA_FSAContributionArray[1][0] = FSA_Max_limit_Family;
                        }
                      } 
                   }
                	   
            	 else{
            	   if(OOP >= FSA_Max_limit_Individual){
            		   HSA_FSAContributionArray[1][0] = FSA_Max_limit_Individual - employerHSAFSAContribution;
                	 } else{
                	   HSA_FSAContributionArray[1][0] = OOP - employerHSAFSAContribution;
                    } 
            	   
            	   carryForward = FSA_Max_limit_Individual- OOP;
            	   
            	   if(carryForward >= 500){
              	     if(HSA_FSAContributionArray[1][0] + 500 <= FSA_Max_limit_Individual){
              	        HSA_FSAContributionArray[1][0] = HSA_FSAContributionArray[1][0]+500;
                       }else{
                         HSA_FSAContributionArray[1][0] = FSA_Max_limit_Individual;
                       }
                     }else if(carryForward < 500 & carryForward >= 0){
                  	     if(HSA_FSAContributionArray[1][0] + carryForward <= FSA_Max_limit_Individual){
                 	          HSA_FSAContributionArray[1][0] = HSA_FSAContributionArray[1][0]+carryForward;
                          }else{
                            HSA_FSAContributionArray[1][0] = FSA_Max_limit_Individual;
                          }
                        }
                        else if(carryForward < 0){
                     	     if(HSA_FSAContributionArray[1][0] + 0 <= FSA_Max_limit_Individual){
                 	            HSA_FSAContributionArray[1][0] = HSA_FSAContributionArray[1][0]+ 0;
                          }else{
                              HSA_FSAContributionArray[1][0] = FSA_Max_limit_Individual;
                          }
                        } 
                     }
            }
              else if (hsaFsaflag.equalsIgnoreCase("H")){
            	 HSA_FSAContributionArray[1][0]= 0;
            	 if(memberCount > 1 ){
            		 
            		 HSA_FSAContributionArray[0][0] = HSA_Max_limit_Family - employerHSAFSAContribution;
             	       }               
             	 else{
             	    HSA_FSAContributionArray[0][0] = HSA_Max_limit_Individual- employerHSAFSAContribution;
                 	 } 
            	 if(age >= 55){
            		 HSA_FSAContributionArray[0][0] = HSA_FSAContributionArray[0][0] + 1000; 
            	    }
             	  }
            else {
            HSA_FSAContributionArray[0][0] =0;
            HSA_FSAContributionArray[1][0]= 0;
            }	
            return HSA_FSAContributionArray;
	} //method close
      
        
 //Calculate HSA/FSA employer contribution
        
    public int getEmployerHSAFSAContibution(ResultSet memResultSet,ResultSet planResultSet,ResultSet hSAFSAResultSet,
    		ResultSet employerPremiumContribution,int rowNum,String email) throws SQLException{
    	
    	String hsaFsaflag = "";
        String plan_id = planResultSet.getString(3);
        int memberCount = 0;
        int employerHSAFSAContribution=0;
        
        hSAFSAResultSet.beforeFirst();
    	while(hSAFSAResultSet.next()){
       	 if(plan_id.equals(hSAFSAResultSet.getString(3))){
       		 hsaFsaflag = hSAFSAResultSet.getString(33);
       		 break;
         	 }
        }
    	 memResultSet.absolute(rowNum);
         do{
            //compare email ids of primary and members to identify the number of members in the family
            if (memResultSet.getString("email_id").equals(email)) {
            memberCount++;        
            } 
         }while(memResultSet.next());
         
         employerPremiumContribution.first();
         if(hsaFsaflag.equalsIgnoreCase("F")){
        	 if(memberCount > 1 ){
        		 employerHSAFSAContribution= Integer.parseInt(employerPremiumContribution.getString(5));
        	 }
        	  else {
        		 employerHSAFSAContribution= Integer.parseInt(employerPremiumContribution.getString(4));  
             }
         }
         else if(hsaFsaflag.equalsIgnoreCase("H")){
        	 if(memberCount > 1 ){
        		 employerHSAFSAContribution= Integer.parseInt(employerPremiumContribution.getString(7));
        	 }
        	  else {
        		 employerHSAFSAContribution= Integer.parseInt(employerPremiumContribution.getString(6));  
        	  }    
           } else{
        	   employerHSAFSAContribution =0;
           }            
           return employerHSAFSAContribution;	   
    }     
        
        
//Calculate taxAdjusted OOP
     public int calculateTaxAdjustedOOP(ResultSet memResultSet, ResultSet planResultSet, ResultSet hSAFSAResultSet,
    		 int employerHSAFSAContribution,int rowNum,int[][] recommendedhsaFsaConribution,int[][] taxArray,int OOP) throws SQLException{ 
    	 
    	 int taxAdjustedHSAFSA;
    	 int taxAdjustedOOP;
    	 int annualIncome;
    	 int taxRate = 0;
    	 String hsaFsaflag = "";
         String plan_id = planResultSet.getString(3);
         hSAFSAResultSet.beforeFirst();
         while(hSAFSAResultSet.next()){
        	 if(plan_id.equals(hSAFSAResultSet.getString(3))){
        		 hsaFsaflag = hSAFSAResultSet.getString(33);
        		 break;
          	 }
         }
    	 memResultSet.absolute(rowNum);
    	 annualIncome = Integer.parseInt(memResultSet.getString("annual_income"));
    	 for(int i = 0; i< taxArray[0].length; i++){
       		 if(taxArray[0][i] == annualIncome){
       			taxRate = taxArray[1][i];
       			break;
       		 }
    	  }
    	 int hsaRecomm = recommendedhsaFsaConribution[0][0];
    	 int fsaRecomm = recommendedhsaFsaConribution[1][0];
    	   		 
    	 if (hsaFsaflag.equalsIgnoreCase("F")){
    		 taxAdjustedHSAFSA = (int) ((fsaRecomm) * taxRate )/100;
    	 }
    	 else if(hsaFsaflag.equalsIgnoreCase("H")){
    		 taxAdjustedHSAFSA = (int)((hsaRecomm) * taxRate)/100;
    	 }		 
    	 else{	 
    		 taxAdjustedHSAFSA = 0;  		 
    	 }
    	 if(taxAdjustedHSAFSA > 0){
    	 taxAdjustedOOP = OOP - taxAdjustedHSAFSA-employerHSAFSAContribution;    	 	   	 
    	 }
    	 else{
    	 taxAdjustedOOP = OOP - employerHSAFSAContribution; 
    	 }
    	 if (taxAdjustedOOP < 0){
    		 taxAdjustedOOP = 0; 
    	 }
    	 return taxAdjustedOOP;
     }//method close
    	             
 
 //fetch tax rate from tax bracket table and store in array
	  public int[][] fetchfromTaxBrackets(String taxBracketSql) throws SQLException{
		 
		 ResultSet taxResultSet;
		 taxResultSet = executeSqlQuery(taxBracketSql);
	    //taxRate.first();
          for(int i = 0; i < 5 && taxResultSet.next(); i++){	
				 taxArray[0][i] = taxResultSet.getInt(1);
				 taxArray[1][i] = Integer.parseInt(taxResultSet.getString(2));	
				  		
	     }
		 return taxArray;		 
    }//method close

//calculate employer premium contribution
      public int calculateEmployerPremiumContribution(String email,ResultSet memResultSet,ResultSet planResultSet,ResultSet employerPremiumContribution,Integer rowNum,int monthlyPremium) throws SQLException {
        // TODO Auto-generated method stub
        	int memberCount = 0;
        	int employerContriPercent = 0;
        	
        	memResultSet.absolute(rowNum);
          	do{

             //compare email ids of primary and members to identify the number of members in the family
              if (memResultSet.getString("email_id").equals(email)) {
            	   memberCount++;
               }         
            }while(memResultSet.next()); 
            //System.out.println(memberCount);
            employerPremiumContribution.first();
        	if (employerPremiumContribution.getString("contribution_type").equalsIgnoreCase("variable")){
        	  if(memberCount >1 ){
        	   employerContriPercent = monthlyPremium * Integer.parseInt(employerPremiumContribution.getString(9))/100;
               return employerContriPercent;
              }
        	  else{  
               employerContriPercent = monthlyPremium * Integer.parseInt(employerPremiumContribution.getString("premium_single"))/100;
               return employerContriPercent;
              } 
        	}
        	else {
         	   if(memberCount >1 ) { 
         	     return(Integer.parseInt(employerPremiumContribution.getString(9)));
         	   }
         	   else{  
                 return(Integer.parseInt(employerPremiumContribution.getString("premium_single")));
         	   }
         	}
	}//method close

       
//calculate expenses for each member of the family
       public int calculateExpenses(ResultSet planResultSet, ResultSet memResultSet, Integer rowNum, String email) throws SQLException {

           int eff_ded = 0;
            int family_ded = 0;
            int planCopay = 0;
            int family_oop = 0;
            int mem_oop = 0;
            int OOP_limit =0;
            int deductible_limit = 0;
            int rem_ded = 0 ;/*Integer.parseInt(planResultSet.getString(7));*/
            String plan_id = planResultSet.getString(3);
            int family_in_network_deductible_limit = Integer.parseInt(planResultSet.getString(7));
            int individual_in_network_deductible_limit = Integer.parseInt(planResultSet.getString(6));
            int family_in_network_oop_limit = Integer.parseInt(planResultSet.getString(9));
            int individual_in_network_oop_limit = Integer.parseInt(planResultSet.getString(8));
            int plan_level_co_insurance = Integer.parseInt(planResultSet.getString(10));
            int Individual_Consumption=0;
            int i=0;
            
            int memberCount =0;
        	memResultSet.absolute(rowNum);
            do{
               //compare email ids of primary and members to identify the number of members in the family
               if (memResultSet.getString("email_id").equals(email)) {
               memberCount++;        
               } 
            }while(memResultSet.next());
            if (memberCount > 1){
            	OOP_limit = family_in_network_oop_limit;
            	deductible_limit = family_in_network_deductible_limit;
            }
            	else{
            	OOP_limit = individual_in_network_oop_limit ; 	
            	deductible_limit = individual_in_network_deductible_limit;
            }
            
        	rem_ded = deductible_limit;
            memResultSet.absolute(rowNum);
            do{          	
            	
            //compare email ids of primary and members to identify the members in the family
             if (memResultSet.getString("email_id").equals(email)) {
                 //System.out.println(memResultSet.getString("member_id"));
                 //System.out.println(memResultSet.getString("gender"));
                
                 Individual_Consumption = Integer.parseInt(memResultSet.getString("individual_consumption"));
                 if (individual_in_network_deductible_limit < rem_ded){
                     eff_ded = individual_in_network_deductible_limit ;
                 }
                 else{
                     eff_ded = rem_ded;
                 }
                 if (Individual_Consumption < eff_ded){
                     mem_oop = Individual_Consumption;
                 }
                 else{
                     mem_oop = (int) (((Individual_Consumption - eff_ded) * plan_level_co_insurance) / 100) + eff_ded;
                 }

                 family_oop += mem_oop;
                 family_ded += mem_oop;
                 planCopay = calculatePlanCopay(planResultSet,memResultSet,rowNum);
                 
                 family_oop += planCopay;
                 //System.out.println(email+" "+plan_id+" "+i+"  " +Individual_Consumption+"  "+rem_ded+"  "+family_oop+"  "+family_ded);


                 //check and adjust OOP against individual OOP
                 if (family_oop >= OOP_limit) {
                     family_oop = OOP_limit;
                     break;
                 }

                 if (family_ded < deductible_limit ) {
                     rem_ded = deductible_limit - family_ded;
                 } else {
                     rem_ded = 0;
                 }
               }                
                
             }while(memResultSet.next());
          //System.out.println("OOP: "+family_oop);
          return family_oop;
        }//method close

       //Updated calculatePlanCopay method on 03/12/2018, Please refer the line just below the 'updated code' comment
       // calculate Copay for each member and for each plan
      public int calculatePlanCopay(ResultSet planResultSet, ResultSet memResultSet, int rowNum) throws SQLException {
            String gender, plan_id;
            ResultSet planCopayResultSet=null;
            int health_profile;
            //updated code on 03/12/2018
            int age, tot_copay=0;
            String employer_group,dzee_client;
            //memResultSet.absolute(rowNum);
            if (Double.parseDouble(planResultSet.getString(5)) == 0.0)
                return 0;
            else
            System.out.println(memResultSet.getString("gender"));
            health_profile = Integer.parseInt(memResultSet.getString("health_profile"));
            System.out.println(memResultSet.getString("health_profile"));
            gender = memResultSet.getString("gender");
            System.out.println(memResultSet.getString("gender"));
            age = Integer.parseInt(memResultSet.getString("age"));
            System.out.println(memResultSet.getString("age"));
            plan_id = planResultSet.getString(3);
            System.out.println("Plan Id -"+plan_id);
            employer_group = planResultSet.getString(2);
            dzee_client = planResultSet.getString(1);
                     
            connection.createStatement();
            try {

                PreparedStatement stmt = connection.prepareStatement(planCopaySql);
                stmt.setString(1, plan_id);
                stmt.setString(2, gender);
                stmt.setString(3, String.valueOf(health_profile));
                stmt.setString(4, String.valueOf(age));
                stmt.setString(5, enrollment_year);
                stmt.setString(6, employer_group);
                stmt.setString(7, dzee_client);
                planCopayResultSet = stmt.executeQuery();
                planCopayResultSet.first();
                //updated code on 03/12/2018
                tot_copay= Integer.parseInt(planCopayResultSet.getString("total_co_pay"));
              } catch (SQLException se) {      //Handle errors for JDBC
                	//updated code..on 03/12/2018 inserted return 0 
                	return 0;
            } catch (Exception e) {  //Handle errors for Class.forName
            	e.printStackTrace();
            }
            //updated code.. on 03/12/2018
            return (tot_copay);
        }//calculatePlanCopay method close


// calculate premium based on age wise or on health care plan id
       public double getPremium(String dzee_client,String employerGroup,String enrollment_year,String plan_id, String email_id, int detailed_premium) throws SQLException {
              ResultSet premiumResultSet =null; 
              connection.createStatement();
           try{   
            if (detailed_premium == 0) {
                	PreparedStatement stmt = connection.prepareStatement(premiumSql);
                    stmt.setString(1, email_id);
                    stmt.setString(2, dzee_client);
                    stmt.setString(3, employerGroup);
                    stmt.setString(4, enrollment_year );
                    stmt.setString(5, plan_id);
                    premiumResultSet = stmt.executeQuery();
                    premiumResultSet.first();                
              }
              else 
                {
            	  PreparedStatement stmt = connection.prepareStatement(detailPremiumSql);
                  stmt.setString(1, dzee_client);
                  stmt.setString(2, employerGroup);
                  stmt.setString(3, plan_id);
                  stmt.setString(4, email_id );
                  stmt.setString(5, enrollment_year);
                  premiumResultSet = stmt.executeQuery();
                  premiumResultSet.first();   
                 }
               } catch (SQLException se) {      //Handle errors for JDBC
               se.printStackTrace();
              } catch (Exception e) {  //Handle errors for Class.forName
               e.printStackTrace();
              }             
            premiumResultSet.first();
            return (Double.parseDouble(premiumResultSet.getString("premiums"))); // return premium for that plan
        }//getPremium method close
       

//printing the plan id and premiums and OOP
       public void printList(String email,ArrayList<planInfo> premiumList,ArrayList<planInfo> premium_OOP_List) {
            System.out.println("\n Print plan id and premium for employee -- "+ email );
            for (planInfo plan : premiumList)
                System.out.println(plan.healthcare_plan_id + "   " + plan.premium_OOP);
            System.out.println("Print plan id and premium + OOP for employee -- "+ email );
            for (planInfo plan : premium_OOP_List)
                System.out.println(plan.healthcare_plan_id + "   " + plan.premium_OOP);
        }//close method
       
        
// add to list collection premium and OOP and sort it
       public void addToList(String planId, double annualemployerPremiumContribution, double AnnualHealthcarePremiumPlusOOP,PlanAnalytics empObj) {
            premiumList.add(new planInfo(annualemployerPremiumContribution, planId));
            premium_OOP_List.add(new planInfo(AnnualHealthcarePremiumPlusOOP,planId));
            Collections.sort(premiumList);
            Collections.sort(premium_OOP_List);
        }//close method
       

// setting the plan ranks based on premium and OOP
     public void planRank(ArrayList<planInfo> premiumList,ArrayList<planInfo> premium_OOP_List){
        int rank =0;
       //System.out.println("\n Plan ranks --");
        planInfo pl = new planInfo();
        for(int i=0; i<premiumList.size(); i++){
            rank++;
            pl.setPlanRank(premiumList.get(i),rank);
            pl.setPlanRank(premium_OOP_List.get(i),rank);
        }
     }//method close
     

//clear all vectors and arrays 
      public void clearAll(ArrayList<planInfo> premiumList,ArrayList<planInfo> premium_OOP_List,Vector<PlanDetail> planDetailVector){
        premium_OOP_List.clear();
        premiumList.clear();
        planDetailVector.clear();
      }//method close
      
      
 //load CSV file into the database     
      public void loadCSV(String fileName){
      	ResultSet rs = null;
       	String loadPreEnrollmentStandardSql ="LOAD DATA local INFILE '"+outfilePath+fileName+"'"+" IGNORE "
                +" INTO TABLE pre_enrollment_analytics_standard "
                +" FIELDS TERMINATED BY '|' ignore 1 lines; ";
      	
      	 PreparedStatement stmt = null;
          try {
        	  stmt = connection.prepareStatement(loadPreEnrollmentStandardSql);
              rs = stmt.executeQuery();           
           } catch (SQLException se) {      //Handle errors for JDBC
              se.printStackTrace();
          } catch (Exception e) {  //Handle errors for Class.forName
              e.printStackTrace();
          }
      }
      
 //delete CSV file from the package
      public void deleteCSV(String filePath){
    	  
    	  try{

    	    Path fileToDeletePath = Paths.get(filePath); 
    	    //System.out.println(fileToDeletePath);
      		if(Files.deleteIfExists(fileToDeletePath))
      		{      			System.out.println("CSV File is deleted!");
      		}else{
      			System.out.println("Delete operation is failed.");
      		}

      	}catch(Exception e){
      		e.printStackTrace();
      	}
     }//method close
  }//Class close
