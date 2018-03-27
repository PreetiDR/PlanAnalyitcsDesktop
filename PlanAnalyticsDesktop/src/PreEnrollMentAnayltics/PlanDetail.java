package PreEnrollMentAnayltics;

public class PlanDetail {
	 // TODO Auto-generated method stub
    private String dzee_client;
    private String employer_group;
    private int Enrollment_Year;
    private String Employee_Email_id;
    private String plan_id;
    private int TotalCostbasedPlanRank;
    private int PremiumbasedPlanRank;
    private int OOP;
    private double MonthlyPremium;
    private double AnnualHealthcarePremiumPlusOOP;
    private int TotalFamilyServiceCost;
    private double AnnualSupplementPlanPremium;
    private int HSAContribution;
    private int FSAContribution;
    private int TaxAdjustedOOP;
    private double EmployerPremiumContribution;
   // private int EmployerCost;

    public PlanDetail(){}
    public PlanDetail(String dzee_client, String employer_group, int Enrollment_Year, String Employee_Email_id, String plan_id,
                      int OOP, double MonthlyPremium, double AnnualHealthcarePremiumPlusOOP, int TotalFamilyServiceCost,
                      int HSAContribution,int FSAContribution, int TaxAdjustedOOP, double EmployerPremiumContribution){

        this.dzee_client= dzee_client;
        this.employer_group =employer_group;
        this.Enrollment_Year=Enrollment_Year;
        this.plan_id =plan_id;
        this.Employee_Email_id = Employee_Email_id;
        this.OOP = OOP;
        this.MonthlyPremium= MonthlyPremium;
        this.AnnualHealthcarePremiumPlusOOP =  AnnualHealthcarePremiumPlusOOP;
        this.TotalFamilyServiceCost = TotalFamilyServiceCost;
        this.HSAContribution = HSAContribution;
        this.FSAContribution = FSAContribution;
        this.EmployerPremiumContribution = EmployerPremiumContribution;
        this.TaxAdjustedOOP = TaxAdjustedOOP;
       // this.EmployerCost = EmployerCost;
    }
    public void setPlanRanks(int TotalCostbasedPlanRank,int PremiumbasedPlanRank){
        this.TotalCostbasedPlanRank = TotalCostbasedPlanRank;
        this.PremiumbasedPlanRank = PremiumbasedPlanRank; }

    public String getdzee_client() { return dzee_client; }
    public String getemployer_group() {return employer_group;}
    public int getEnrollment_Year() { return Enrollment_Year; }
    public String getEmployee_Email_ID() { return Employee_Email_id; }
    public String gethealthcare_plan_id (){ return plan_id; }
    public int getEmployee_Rank(){ return TotalCostbasedPlanRank; }
    public int getpremium_Rank(){ return PremiumbasedPlanRank; }
    public int getOOP() { return OOP; }
    public double getPremium() { return MonthlyPremium; }
    public double getpremium_OOP(){return AnnualHealthcarePremiumPlusOOP;}
    public int getTotal_Consumption(){ return TotalFamilyServiceCost;  }
    public int getHSAContribution() { return HSAContribution;}
    public int getFSAContribution() { return FSAContribution;}
    public int getTaxAdjustedOOP() { return TaxAdjustedOOP; }
    public double getEmployerPremiumContribution(){ return EmployerPremiumContribution ;}
   /* public int getTotalCostbasedPlanRank(){return TotalCostbasedPlanRank; }
    public int getPremiumbasedPlanRank(){return PremiumbasedPlanRank;}*/
   //public int getEmployerCost(){ return EmployerCost ;}
 }


