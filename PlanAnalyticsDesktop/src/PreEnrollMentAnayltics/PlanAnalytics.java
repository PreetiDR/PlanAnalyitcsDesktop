
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
import java.util.List;
import java.util.Vector;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.ListIterator;

import com.mysql.jdbc.Util;

import PreEnrollMentAnayltics.EmployeeConsumption;
import PreEnrollMentAnayltics.CSVWrite;

public class PlanAnalytics{
	
	private final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private String URL; // = "jdbc:mysql://dzanalytics.dzeecloud.com:3306/plan_recommend?useSSL=false";
    private final String USER = "root1";
    private final String PASSWORD = "Dzee2015$";
    static Connection connection = null;
    private ResultSet planResultSet, memResultSet,globalResultSet,employerPremiumContribution,hSAFSAResultSet,employeeConsumptionResultSet, organizationPlansResultSet;
    private ResultSet allPremium, familyPremium, coPay;
    public String server_name, database_name, dzee_client, employerGroup, enrollment_year;
    static ArrayList<Integer> list = new ArrayList<Integer>();
    int taxArray[][] = new int[2][5];
    ArrayList<planInfo> premiumList = new ArrayList<planInfo>();
    ArrayList<planInfo> premium_OOP_List = new ArrayList<planInfo>();
    Vector<PlanDetail> planDetailVector = new Vector<PlanDetail>();
    static CSVWrite filewrite = new CSVWrite();
    int noEmpRecords=0;
    int noOfPlanRecords = 0;
    public String filePath,fileName; 
    public final static String outdir = System.getProperty("user.dir")+ "/src/properties_pk";
    public static final String outstr = "preEnrollmentStandard";
    public final String outfilePath = "/Users/ratnakar/git/PlanAnalyitcsDesktop/PlanAnalyticsDesktop/src/properties_pk/";
  

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
    	
    	//updated planSql 29 Mar 2018
    
    
  //Modified employee_consumptiontempSql to include all additional fields from memSql on 03/28/2018
    
    String employee_consumptiontempSql ="CREATE TEMPORARY table IF NOT EXISTS employee_consumptiontemp (primary key ekey (email_id, member_id)) AS (select m.email_id, m.member_id, m.gender, "
    		+ " m.relation, m.health_profile, m.age, m.tobacco_usage, e.annual_income, e.plan_enrollment_year, " 
    		+ " (e.spouse + e.no_of_children +1) as member_count, b.total_consumption as individual_consumption "
    	    + " from benefit_consumption b, members m, employee e, employer emp " 
    		+ " where " 
    		+ " emp.dzee_client= ? "
    		+ " and emp.employer_group= ? "
    		+ " and e.plan_enrollment_year= ? "
    		+ " and emp.email_id = e.employer_id and m.email_id = e.email_id "
    		+ " and m.gender = b.gender and m.health_profile = b.health_profile "
    		+ " and b.service_year = e.plan_enrollment_year "
    		+ " and m.age between b.age_from and b.age_to "
    		+ " and ((m.member_id < 1) or (m.member_id = 1 and m.relation = 'Spouse' and m.age < 65) or (m.member_id > 0 and m.relation = 'Child' and m.age <= 26)) "
    		+ " and b.location='Colorado' "
    		+ " and emp.deleted=false  "
			+ " and e.deleted=false  "
    		+ " group by email_id, member_id,individual_consumption,e.plan_enrollment_year);";   
    
    	String memSql = "select * from employee_consumptiontemp;";
    	/*String memSql=" select emp.email_id, m.member_id,gender,relation,health_profile,age,tobacco_usage,annual_income,empcon.individual_consumption,emp.plan_enrollment_year,(emp.spouse+emp.no_of_children+1) as member_count"
                +" from employee emp, members m, employee_consumptiontemp empcon"
                +" where "
                +" m.member_id = empcon.member_id "
                +" and ((m.member_id < 1) or (m.member_id = 1 and m.relation = 'Spouse' and m.age < 65) or (m.member_id > 0 and m.relation = 'Child' and m.age <= 26))"
                +" and m.email_id = emp.email_id "
                +" and empcon.email_id= m.email_id"
    			+" order by emp.email_id,m.member_id "; 
         */
    
         //updated planSql 29 Mar 2018
        /* String planSql = "select dzee_client,employer_group, healthcare_plan_id," 
         		 +"case "
         		 +"when opd.plan_level_co_insurance = 0 then 9999999999999 "
         		 +"else " 
         		 +"opd.individual_in_network_deductible_limit + "
         		 +"(opd.individual_in_network_oop_limit - opd.individual_in_network_deductible_limit)/opd.plan_level_co_insurance "
                 +"end as minexpense, opd.individual_in_network_deductible_limit, family_in_network_deductible_limit, "
                 + "individual_in_network_oop_limit, family_in_network_oop_limit, plan_level_co_insurance, detailed_premium, opd.enrollment_year " 
				 + "from organization_plans_data opd "
				 + "where " 
                 + "opd.is_deleted <> 1 "
                 + "and dzee_client = ? "
                 + "and opd.employer_group= ? "
                 + "and enrollment_year = ? "  
                 + "and opd.is_deleted=false "
    			 + "and opd.dzee_approved=true";
    			*/
    	/*String planCopaySql = "SELECT e.email_id, pc.plan_id, sum(pc.total_co_pay) as copay"
        		+"                          FROM plan_copay pc,"
        		+"                          organization_plans_data opd, members m, employee e, employee_consumptiontemp em "
        		+"                  where"
        		+"                  opd.dzee_client  = ?"
        		+"                  and opd.employer_group = ? "
        		+"                  and opd.enrollment_year = ?"
        		+"                  and opd.healthcare_plan_id = pc.plan_id"
        		+"                  and e.email_id=em.email_id"
        		+"                  and m.email_id = e.email_id"
        		+"                  and pc.gender=m.gender"
        		+"                  and pc.health_profile=m.health_profile"
        		+"                  and (m.age between pc.min_age_range and pc.max_age_range)"
        		+"                  and pc.location = opd.state"
        		+"                  "
        		+"                  group by e.email_id, pc.plan_id"
        		+"                  order by e.email_id, pc.plan_id"
        		+"                  ;";
    		*/	
        //updated all following queries to refer to organizationPlans table instead on organizationPlansData table on 04/02/2018
    	
    	String planCopaySql = "SELECT e.email_id, pc.plan_id, sum(pc.total_co_pay) as copay"
        		+"                          FROM plan_copay pc,"
        		+"                          organizationPlans op, employee e, employee_consumptiontemp em "
        		+"                  where"
        		+"                  op.healthcare_plan_id = pc.plan_id"
        		+"                  and e.email_id=em.email_id"
        		+"                  and pc.gender=em.gender"
        		+"                  and pc.health_profile=em.health_profile"
        		+"                  and (em.age between pc.min_age_range and pc.max_age_range)"
        		+"                  and pc.location = op.state"
        		+"                  "
        		+"                  group by e.email_id, pc.plan_id"
        		+"                  order by e.email_id, pc.plan_id"
        		+"                  ;";
        
       /* String detailPremiumSql = "select emp.email_id, org.healthcare_plan_id,sum(ag.premium) as premiums,org.enrollment_year "
                + " from organization_plans_data org,employee emp,age_wise_premiums ag,members m, employee_consumptiontemp t  "
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
                + " and org.enrollment_year = ? "
                + " and emp.email_id = t.email_id"
                + " and org.detailed_premium=1 "
                + " group by emp.email_id, org.healthcare_plan_id ;";
*/
    	
    	
    	String detailPremiumSql = "select t.email_id, op.healthcare_plan_id,sum(ag.premium) as premiums"
                + " from organizationPlans op, age_wise_premiums ag, employee_consumptiontemp t  "
                + " where ag.healthcare_plan_id = op.healthcare_plan_id "
                + " and ag.dzee_client = op.dzee_client"
                + " and ag.employer_group = op.employer_group"
                + " and t.age between ag.age_from and ag.age_to"
                + " and op.detailed_premium=1 "
                + " group by t.email_id, op.healthcare_plan_id ;";


  /*      String premiumSql = "select m.email_id,healthcare_plan_id,"
       		 +"                  ( "
       		 +"                  CASE "
       		 +"                  WHEN(m.spouse ='0' && m.no_of_children='0') THEN opd.primary_healthcare_plan_premium "
       		 +"                  WHEN(m.spouse ='0' && m.no_of_children= '1') THEN opd.primary_one_child_healthcare_plan_premium"
       		 +"                  WHEN(m.spouse ='0' && m.no_of_children= '2') THEN opd. primary_two_children_healthcare_plan_premium "
       		 +"                  WHEN(m.spouse ='0' && m.no_of_children>= '3') THEN opd.primary_three_and_more_children_healthcare_plan_premium "
       		 +"                  WHEN(m.spouse ='1' && m.no_of_children= '0') THEN opd.couple_healthcare_plan_premium "
       		 +"                  WHEN(m.spouse ='1' && no_of_children = '1') THEN opd.couple_one_child_healthcare_plan_premium"
       		 +"                  WHEN(m.spouse ='1' && m.no_of_children= '2') THEN opd.couple_two_children_healthcare_plan_premium "
       		 +"                  WHEN(m.spouse ='1' && m.no_of_children>= '3') THEN opd.couple_three_children_and_more_healthcare_plan_premium "
       		 +"                  ELSE 1 "
       		 +"                  END)as premiums "
       		 +"                  from organization_plans_data opd, employee m, employee_consumptiontemp t"
       		 +"                  where"
       		 +"                  m.email_id = t.email_id"
       		 +"                  and t.member_id = 0"
       		 +"                  and opd.dzee_client = ?"
       		 +"                  and opd.employer_group= ?"
       		 +"                  and opd.enrollment_year= ? "
       		 +"                  and opd.is_deleted=false "
       		 +"                  and opd.dzee_approved=true "
       		 +"                  and opd.detailed_premium=0";
*/
        String premiumSql = "select m.email_id,healthcare_plan_id,"
          		 +"                  ( "
          		 +"                  CASE "
          		 +"                  WHEN(m.spouse ='0' && m.no_of_children='0') THEN op.primary_healthcare_plan_premium "
          		 +"                  WHEN(m.spouse ='0' && m.no_of_children= '1') THEN op.primary_one_child_healthcare_plan_premium"
          		 +"                  WHEN(m.spouse ='0' && m.no_of_children= '2') THEN op. primary_two_children_healthcare_plan_premium "
          		 +"                  WHEN(m.spouse ='0' && m.no_of_children>= '3') THEN op.primary_three_and_more_children_healthcare_plan_premium "
          		 +"                  WHEN(m.spouse ='1' && m.no_of_children= '0') THEN op.couple_healthcare_plan_premium "
          		 +"                  WHEN(m.spouse ='1' && no_of_children = '1') THEN op.couple_one_child_healthcare_plan_premium"
          		 +"                  WHEN(m.spouse ='1' && m.no_of_children= '2') THEN op.couple_two_children_healthcare_plan_premium "
          		 +"                  WHEN(m.spouse ='1' && m.no_of_children>= '3') THEN op.couple_three_children_and_more_healthcare_plan_premium "
          		 +"                  ELSE 1 "
          		 +"                  END)as premiums "
          		 +"                  from employee m, employee_consumptiontemp t, organizationPlans op "
          		 +"                  where"
          		 +"                  m.email_id = t.email_id"
          		 +"                  and t.member_id = 0"
          		 +"                  and op.is_deleted=false "
          		 +"                  and op.dzee_approved=true "
          		 +"                  and op.detailed_premium=0";
       

       String queryDelete = "Delete from pre_enrollment_analytics_standard where dzee_client=? and employer=? and enrollment_year=? and analytic_type=1";
       
      /* String employerContriSql = " SELECT employer_id,enrollment_year,contribution_type,fsa_single,fsa_family,hsa_single,hsa_family,premium_single,premium_family "
       		                     + " from employer emplyr,employer_contribution empContri "
       		                     + " where "
       		                     + " emplyr.email_id = empContri.employer_id"
    		                     + " and emplyr.dzee_client = ?"
    		                     + " and emplyr.employer_group = ?"
    		                     + " and empContri.enrollment_year  = ? "
    		                     + " and emplyr.deleted=false";        
       */
       String employerContriSql = " SELECT employer_id, op.enrollment_year,contribution_type,fsa_single,fsa_family,hsa_single,hsa_family,premium_single,premium_family "
                  + " from employer emplyr,employer_contribution empContri, organizationPlans op "
                  + " where "
                  + " emplyr.email_id = empContri.employer_id"
               + " and emplyr.dzee_client = op.dzee_client"
               + " and emplyr.employer_group = op.employer_group"
               + " and empContri.enrollment_year  = op.enrollment_year"
               + " and emplyr.deleted=false"; 

       String organisationPlanSql = " SELECT * FROM organization_plans_data "
    		                        + " where "
                                    + " dzee_client = ?"
                                    + " and employer_group = ?"
                                    + " and enrollment_year = ? "
                                    + " and dzee_approved=true "
                                	+ " and is_deleted=false"; 
       
       	//created following sql ...03/29/2018
       	String organizationPlansSql = "CREATE TEMPORARY table IF NOT EXISTS organizationPlans (primary key my_pkey (healthcare_plan_id)) AS (SELECT * FROM organization_plans_data "
    		                        + " where "
                                    + " dzee_client = ?"
                                    + " and employer_group = ?"
                                    + " and enrollment_year = ? "
                                    + " and dzee_approved=true "
                                	+ " and is_deleted=false);"; 
       
       //created following sql ...03/29/2018
       String selectOrgPlansSql = "Select * from organizationPlans;";
       
       String taxBracketSql = " SELECT * FROM tax_brackets ";
       
       String globalAssumptionsSql = "SELECT * FROM global_assumptions "; 
 
       

   Map premiumMap = new HashMap();
       Map coPayMap = new HashMap();
                
      /* public String loadPreEnrollmentStandardSql ="LOAD DATA local INFILE '"+"/Users/ratnakar/git/PlanAnalyitcsDesktop/PlanAnalyticsDesktop/src/properties_pk"+outstr+'_'+employerGroup+"'"
       		                                       +" IGNORE "
                                                   +" INTO TABLE pre_enrollment_analytics_standard "
                                                   +" FIELDS TERMINATED BY '|' "
                                                   +" ignore 1 lines; " ;
       		                  
      */
       public static void main(String[] args) throws Exception {


        	PlanAnalytics empObj = new PlanAnalytics();
        	//System.out.println("OUT DIR"+outdir);
        	//Updated following code to allow 2 more parameters ...server name and database 03/28/2018
        	System.out.println("Enter the Server Name, Database Name, Dzee client, Employer group and Enrollment year: ");
        	empObj.server_name = args[0];
            empObj.database_name = args[1];
            empObj.URL= "jdbc:mysql://" + empObj.server_name + ":3306/" + empObj.database_name;
            System.out.println("URL : "+empObj.URL);
        	empObj.dzee_client = args[2];
            empObj.employerGroup = args[3];
            empObj.enrollment_year = args[4];
            
                    
            try {
            	//empObj.filePath = empObj.employerGroup + ".csv";
            	//System.out.println("Filepath:"+empObj.filePath);
            	//filewrite.writeHeading(empObj.filePath);
            	//empObj.fileName = empObj.filePath.substring(empObj.filePath.lastIndexOf("/")+1);
            	//System.out.println(empObj.fileName);
            	empObj.databaseConnect(empObj.JDBC_DRIVER,empObj.URL,empObj.USER,empObj.PASSWORD);
            	//System.out.println(empObj.employee_consumptiontempSql);
                empObj.employeeConsumptionResultSet = empObj.creatingStatement(empObj.employee_consumptiontempSql, empObj.dzee_client, empObj.employerGroup, empObj.enrollment_year);
                //System.out.println(empObj.memSql);
                empObj.memResultSet = empObj.createStatement(empObj.memSql);
               
                //updated following code .. added empObj.qualifiedPlansResultSet, modified empObj.planResultSet on 03/28/2018
                empObj.organizationPlansResultSet =empObj.creatingStatement(empObj.organizationPlansSql, empObj.dzee_client, empObj.employerGroup, empObj.enrollment_year);
                //empObj.planResultSet = empObj.creatingStatement(empObj.planSql, empObj.dzee_client,empObj.employerGroup, empObj.enrollment_year);
                empObj.planResultSet = empObj.createStatement(empObj.selectOrgPlansSql);
                
                
                empObj.globalResultSet = empObj.executeSqlQuery(empObj.globalAssumptionsSql);
                //empObj.planResultSet = empObj.creatingStatement(empObj.organisationPlanSql,empObj.dzee_client,empObj.employerGroup, empObj.enrollment_year);
                //empObj.employerPremiumContribution = empObj.creatingStatement(empObj.employerContriSql,empObj.dzee_client,empObj.employerGroup,empObj.enrollment_year);
                empObj.employerPremiumContribution = empObj.createStatement(empObj.employerContriSql);
                empObj.taxArray = empObj.fetchfromTaxBrackets(empObj.taxBracketSql);
                empObj.noEmpRecords = empObj.getCountofRecords(empObj.memResultSet);
                empObj.noOfPlanRecords = empObj.getCountofRecords(empObj.planResultSet);
                
                if(empObj.noEmpRecords == 0 || empObj.noOfPlanRecords ==0 ){
                	throw new Exception ("No plans available for the employee or no employee's assigned for the plan");          	             			
                }
                empObj.allPremium = empObj.createStatement(empObj.premiumSql);
                empObj.rsToMap(empObj.premiumMap, empObj.allPremium, 1, 2, 3);
                
                empObj.allPremium = empObj.createStatement(empObj.detailPremiumSql);
                empObj.rsToMap(empObj.premiumMap, empObj.allPremium, 1, 2, 3);
                System.out.println(new Date());
                
                empObj.coPay = empObj.createStatement(empObj.planCopaySql);
                empObj.rsToMap(empObj.coPayMap, empObj.coPay, 1, 2, 3);
                System.out.println(new Date());
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
        public ResultSet createStatement(String query) throws SQLException {
            ResultSet rs = null;
            PreparedStatement stmt = null;

            //System.out.println("Creating statement...");
            try {	

                stmt = connection.prepareStatement(query);
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

            stmt = connection.prepareStatement(queryDelete);
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
/*
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
*/
 
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
            int member_count;
            String enrollmentSql = "insert into pre_enrollment_analytics_standard (dzee_client,employer,enrollment_year,employee_email_id,plan_id,total_cost_based_plan_rank,employer_total_cost_based_plan_rank, "
                    + "oop,monthly_premium,annual_healthcare_premium_plus_OOP,total_family_service_cost,annual_supplement_plan_premium,hsa_contribution,fsa_contribution,tax_adjusted_OOP,analytic_type, "
                    + "employer_premium_contribution, employer_oop_contribution) "
                    + " values \n";
      		String values="";
            int recs = 0;
            int employees =0;

            empObj.deleteFromPlanEnrollmentAnalytics(empObj.queryDelete,empObj.dzee_client,empObj.employerGroup, empObj.enrollment_year);
          System.out.println("Run Start: "+new Date());
            try{
             
              connection.setAutoCommit(false);	
              
              int employeesPerBatch = 2000 / empObj.noOfPlanRecords;
             // for each member loop    
             for (int i = 0; i <  list.size(); i++) {
            	employees++;
                empObj.memResultSet.absolute((Integer) list.get(i));
                String email = empObj.memResultSet.getString("email_id");
                member_count = empObj.memResultSet.getInt("member_count");
                int age = empObj.memResultSet.getInt("age");
                //System.out.println(new Date()+": "+i+","+member_count+","+empObj.memResultSet.getInt("member_id")+","+email);
                //System.out.println(new Date()+":" +i+email);
                //call to calculate employer premium contribution
                empObj.planResultSet.beforeFirst();

                while (empObj.planResultSet.next()) { // for each plan for that employer group
                    //String plan_id = empObj.planResultSet.getString("healthcare_plan_id");
                    String plan_id = empObj.planResultSet.getString(3);
                    recs++;
                    
                    //call calculate OOP for for each member for each plan
                    OOP = empObj.calculateExpenses(planResultSet, memResultSet, (Integer)list.get(i), email, member_count);
                    
                    //empObj.memResultSet.absolute((Integer)list.get(i));
                    DmonthlyPremium = getMapValue(premiumMap,email,plan_id);
                    
                    RmonthlyPremium = (int) Math.round(DmonthlyPremium);
                    employerPremiumContri = empObj.calculateEmployerPremiumContribution(email,memResultSet,planResultSet,employerPremiumContribution,(Integer)list.get(i),RmonthlyPremium, member_count);
                    employerHSAFSAContribution=getEmployerHSAFSAContibution(memResultSet,planResultSet,planResultSet,employerPremiumContribution,(Integer) list.get(i), email, member_count);
                    recommendedhsaFsaConribution = CalculateRecommendedHsaFsaContibution(OOP,planResultSet, memResultSet,planResultSet,employerPremiumContribution,globalResultSet,employerHSAFSAContribution,(Integer) list.get(i), email, member_count, age);
                    taxAdjustedOOP = calculateTaxAdjustedOOP(memResultSet,planResultSet,planResultSet,employerHSAFSAContribution,(Integer)list.get(i),recommendedhsaFsaConribution,taxArray,OOP);
                    AnnualHealthcarePremiumPlusOOP = (DmonthlyPremium- employerPremiumContri)*12 +taxAdjustedOOP;
                    //AnnualHealthcarePremiumPlusOOP = RmonthlyPremium*12+OOP;
                    annualEmployerPremiumContribution= (int) (employerPremiumContri * 12);
                    empObj.addToList(plan_id, employerPremiumContri, AnnualHealthcarePremiumPlusOOP,empObj);
                    int family_consumption = empObj.getFamilyConsumption(memResultSet,(Integer) list.get(i), email, member_count);
                    empObj.memResultSet.absolute((Integer)list.get(i));
                    planDetailVector.addElement(new PlanDetail(dzee_client,employerGroup,Integer.parseInt(enrollment_year),email,plan_id,OOP,DmonthlyPremium,
                    AnnualHealthcarePremiumPlusOOP,family_consumption,recommendedhsaFsaConribution[0][0],
                    recommendedhsaFsaConribution[1][0],taxAdjustedOOP,employerPremiumContri));                    
                }
                //printList(email,empObj.premiumList,empObj.premium_OOP_List);
                empObj.planRank(empObj.premiumList,empObj.premium_OOP_List);
                int lastrec=0;
                if(employees%employeesPerBatch == 0 || i == (list.size()-1) ) {
                	lastrec=1;
                }
                values+=empObj.insertPlanEnrollmentAnalytics(planDetailVector,empObj.premiumList,empObj.premium_OOP_List, lastrec);
                
                if(employees%employeesPerBatch == 0) {
                	System.out.println("Records Inserted: "+recs);
               	 	insertDB(values,enrollmentSql);
               	 	values="";
                }
                
                empObj.clearAll(premiumList,premium_OOP_List,planDetailVector);
            }
             
             insertDB(values,enrollmentSql);

             //System.out.println("Records Inserted: "+recs);
             //System.out.println(new Date()+": Insert Ended");
             
 //            empObj.loadCSV(empObj.fileName);
             
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
       
       public int getFamilyConsumption(ResultSet memResultSet,int rowNum, String email,int memberCount) throws NumberFormatException, SQLException{
    int members = 0;
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
    			   members++;
    			   
    			   family_consumption += Integer.parseInt(memResultSet.getString("individual_consumption"));
    			   if(members >= memberCount) {
    				   break;
    			   }
    			   }
    		   }  			   
    	  }while(memResultSet.next()); 	       
    	   return family_consumption;  	   
       }
 
 //Calculate suggested HSA/FSA Contribution 
        public int[][] CalculateRecommendedHsaFsaContibution(int OOP, ResultSet planResultSet, ResultSet memResultSet,
        		        ResultSet hSAFSAResultSet,ResultSet employerPremiumContribution,ResultSet globalResultSet,int employerHSAFSAContribution,int rowNum, String email, int members, int age) throws SQLException {
		// TODO Auto-generated method stub
         String hsaFsaflag = "";
         String plan_id = planResultSet.getString(3);
    
         int HSA_FSAContributionArray[][] = new int[2][1];
         int FSA_Max_limit_Family=0;
         int FSA_Max_limit_Individual=0;
         int HSA_Max_limit_Family=0;
         int HSA_Max_limit_Individual=0;
         int carryForward =0;
         
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
         
            if(hsaFsaflag.equalsIgnoreCase("F")){
            	HSA_FSAContributionArray[0][0]= 0;
               if(members > 1 ){
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
            	 if(members > 1 ){
            		 
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
    		ResultSet employerPremiumContribution,int rowNum,String email, int memberCount) throws SQLException{
    	
    	String hsaFsaflag = "";
        String plan_id = planResultSet.getString(3);
        
        int employerHSAFSAContribution=0;
        
        hSAFSAResultSet.beforeFirst();
    	while(hSAFSAResultSet.next()){
       	 if(plan_id.equals(hSAFSAResultSet.getString(3))){
       		 hsaFsaflag = hSAFSAResultSet.getString(33);
       		 break;
         	 }
        }
    	
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
      public int calculateEmployerPremiumContribution(String email,ResultSet memResultSet,ResultSet planResultSet,ResultSet employerPremiumContribution,Integer rowNum,int monthlyPremium, int memberCount) throws SQLException {
        // TODO Auto-generated method stub

        	int employerContriPercent = 0;

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
       public int calculateExpenses(ResultSet planResultSet, ResultSet memResultSet, Integer rowNum, String email, int memberCount) throws SQLException {

           int eff_ded = 0;
            int family_ded = 0;
            int planCopay = 0;
            int family_oop = 0;
            int mem_oop = 0;
            int OOP_limit =0;
            int deductible_limit = 0;
            int rem_ded = 0 ;/*Integer.parseInt(planResultSet.getString(7));*/
            String plan_id = planResultSet.getString(3);
          //  int family_in_network_deductible_limit = Integer.parseInt(planResultSet.getString(7));
            int family_in_network_deductible_limit = Integer.parseInt(planResultSet.getString("family_in_network_deductible_limit"));
        //    int individual_in_network_deductible_limit = Integer.parseInt(planResultSet.getString(6));
       //     int family_in_network_oop_limit = Integer.parseInt(planResultSet.getString(9));
        //    int individual_in_network_oop_limit = Integer.parseInt(planResultSet.getString(8));
        //    int plan_level_co_insurance = Integer.parseInt(planResultSet.getString(10));
            
            int individual_in_network_deductible_limit = Integer.parseInt(planResultSet.getString("individual_in_network_deductible_limit"));
                 int family_in_network_oop_limit = Integer.parseInt(planResultSet.getString("family_in_network_oop_limit"));
                int individual_in_network_oop_limit = Integer.parseInt(planResultSet.getString("individual_in_network_oop_limit"));
                 int plan_level_co_insurance = Integer.parseInt(planResultSet.getString("plan_level_co_insurance"));
            int Individual_Consumption=0;
            int i=0;
            
            if (memberCount > 1){
            	OOP_limit = family_in_network_oop_limit;
            	deductible_limit = family_in_network_deductible_limit;
            }
            	else{
            	OOP_limit = individual_in_network_oop_limit ; 	
            	deductible_limit = individual_in_network_deductible_limit;
            }
            
            planCopay = (int)getMapValue(coPayMap,email, plan_id);
        	rem_ded = deductible_limit;
            memResultSet.absolute(rowNum);
            int memProcessed = 0;
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
                 memProcessed++;
                 if(memProcessed>=memberCount) {
                	 break;
                 }
               }
             }while(memResultSet.next());

          return family_oop;
        }//method close

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
  /*    public void loadCSV(String fileName){
      	ResultSet rs = null;
       	String loadPreEnrollmentStandardSql ="LOAD DATA local INFILE '"+outfilePath+fileName+"'"+" IGNORE "
                +" INTO TABLE pre_enrollment_analytics_standard "
                +" FIELDS TERMINATED BY '|' ignore 1 lines; ";
       	System.out.println("Load sql"+loadPreEnrollmentStandardSql);
      	
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
    */  
public void populateMap(Map mapName, String k1, String k2, double val ) {
	String key = k1 + ":" +k2;
	mapName.put(key,val);
}

public double getMapValue (Map mapName, String k1, String k2) {
	String key = k1 + ":" +k2;
	if(mapName.containsKey(key)) {
		return (double)mapName.get(key);
	}
	return 0;
}

public void rsToMap (Map mapName, ResultSet rs, int k1pos, int k2pos, int vpos) {
	try {
		rs.beforeFirst();
		while(rs.next()) {
			populateMap(mapName, rs.getString(k1pos), rs.getString(k2pos), rs.getDouble(vpos));
		}
	} catch (SQLException e) {
		// TODO Auto-generated catch block
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
   // insert into the table plan enrollment analytics
  	public String insertPlanEnrollmentAnalytics(
  			List<PlanDetail> planDetailVector, List<planInfo> premiumList,
  			List<planInfo> premiumOOPList, int lastrec) {

  		int totalCostbasedPlanRank = 0;
  		int employerTotalCostBasedPlanRank = 0;
  		
  		String allVal="";
  		
  		for (int i = 0; i < planDetailVector.size(); i++) {
  			for (int j = 0; j < premiumList.size(); j++) {
  				if (premiumList.get(j).healthcare_plan_id
  						.equals(planDetailVector.get(i).gethealthcare_plan_id())) {
  					employerTotalCostBasedPlanRank = premiumList.get(j).planRank;
  				}
  			}
  			for (int k = 0; k < premiumOOPList.size(); k++) {
  				if (premiumOOPList.get(k).healthcare_plan_id
  						.equals(planDetailVector.get(i).gethealthcare_plan_id())) {
  					totalCostbasedPlanRank = premiumOOPList.get(k).planRank;
  				}
  			}
  			planDetailVector.get(i).setPlanRanks(totalCostbasedPlanRank,
  					employerTotalCostBasedPlanRank);
  			String val="( ";
  				// create the MySql insert prepared statement
  				val=popVal(val, planDetailVector.get(i).getdzee_client()); //1
  				val=popVal(val, planDetailVector.get(i).getemployer_group()); //2
  				val=popValInt(val, planDetailVector.get(i).getEnrollment_Year()); //3
  				val=popVal(val, planDetailVector.get(i).getEmployee_Email_ID()); //4
  				val=popVal(val, planDetailVector.get(i).gethealthcare_plan_id()); //5
  				val=popValInt(val, totalCostbasedPlanRank); //6
  				val=popValInt(val, employerTotalCostBasedPlanRank); //7
  				val=popValInt(val, planDetailVector.get(i).getOOP()); //6
  				val=popValDouble(val, planDetailVector.get(i).getPremium()); //7
  				val=popValDouble(val, planDetailVector.get(i).getpremium_OOP()); //8
  				val=popValInt(val, planDetailVector.get(i).getTotal_Consumption()); //9
  				val=popValInt(val, 0); //10
  				val=popValInt(val, planDetailVector.get(i).getHSAContribution()); //11
  				val=popValInt(val, planDetailVector.get(i).getFSAContribution()); //12
  				val=popValInt(val, planDetailVector.get(i).getTaxAdjustedOOP()); //13
  				val=popValInt(val, 1); //14
  				val=popValDouble(val, planDetailVector.get(i).getEmployerPremiumContribution()); //14
  				val=popValInt(val, 0, 1); //14
  				if(lastrec==1 && i == (planDetailVector.size()-1)) {
  					val+=");\n";
  				} else {
  				val+="),\n";
  				}
  						
  				allVal+=val;
  				/*
  				preparedStmt.setString(1, planDetailVector.get(i)
  						.getdzee_client());
  				preparedStmt.setString(2, planDetailVector.get(i)
  						.getemployer_group());
  				preparedStmt.setInt(3, planDetailVector.get(i)
  						.getEnrollment_Year());
  				preparedStmt.setString(4, planDetailVector.get(i)
  						.getEmployee_Email_ID());
  				preparedStmt.setString(5, planDetailVector.get(i).gethealthcare_plan_id());
  				preparedStmt.setInt(6, totalCostbasedPlanRank);// employer rank
  				preparedStmt.setInt(7, employerTotalCostBasedPlanRank);// premium
  																		// rank
  				preparedStmt.setDouble(8, planDetailVector.get(i).getOOP());// OOP
  				preparedStmt.setDouble(9, planDetailVector.get(i)
  						.getPremium());// premium
  				preparedStmt.setDouble(10, planDetailVector.get(i)
  						.getpremium_OOP());// premium+OOP
  				preparedStmt.setInt(11, planDetailVector.get(i)
  						.getTotal_Consumption());// //total consumption
  				preparedStmt.setInt(12, 0);// Annual supplement plan premium
  				preparedStmt.setInt(13, planDetailVector.get(i)
  						.getHSAContribution());// HSAContribution
  				preparedStmt.setInt(14, planDetailVector.get(i)
  						.getFSAContribution());// FSAContribution
  				preparedStmt.setInt(15, planDetailVector.get(i)
  						.getTaxAdjustedOOP());// TaxAdjustedOOP
  				preparedStmt.setInt(16, 1);// Analytic_Type
  				preparedStmt.setInt(17, 1);// Analytic_Type
  				preparedStmt.setInt(18, 1);// Analytic_Type
  				/*preparedStmt.setInt(
  						17,
  						getMin(planDetailVector.get(i).getPremium(),
  								planDetailVector.get(i)
  										.getEmployerPremiumContribution()));// EmployerPremiumContribution
  				preparedStmt.setInt(18, planDetailVector.get(i)
  						.getEmployerPremiumContribution());// EmployerOOPContribution

  				// execute the prepared statement
  				//preparedStmt.execute();
  				preparedStmt.addBatch();
  			} catch (SQLException se) { // Handle errors for JDBC
  				se.printStackTrace();
  			} catch (Exception e) { // Handle errors for Class.forName
  				e.printStackTrace();
  			}*/
  		}
  		return allVal;
  	}// method close
  	public void insertDB(String vals,String enrollmentSql) {
  		enrollmentSql += vals;
  	//	System.out.println(enrollmentSql);
  		try {
  			createStatement(enrollmentSql);
  		} catch (SQLException se) { // Handle errors for JDBC
				se.printStackTrace();
			} catch (Exception e) { // Handle errors for Class.forName
				e.printStackTrace();
			}
  		
  	}
  	public String popVal(String v, String ad) {
			v+="'"+ad+"',";
			return v;
		}
  	public String popValInt(String v, int ad) {
		v+=Integer.toString(ad)+",";
		return v;
	}
  	public String popValInt(String v, int ad, int nocomma) {
		v+=Integer.toString(ad);
		return v;
	}
  	public String popValDouble(String v, double ad) {
		v+=Double.toString(ad)+",";
		return v;
	}
  }//Class close
