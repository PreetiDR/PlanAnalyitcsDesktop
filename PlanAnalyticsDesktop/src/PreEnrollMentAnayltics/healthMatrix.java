package PreEnrollMentAnayltics;

public class healthMatrix {
	
	 /*private String dzee_client;
	 private String employer_group;
	 private int Enrollment_Year;*/
	 private int health_profile;
	 private double value;
	
	 public healthMatrix(){}
	 public healthMatrix(int health_profile, double value){

	       /* this.dzee_client= dzee_client;
	        this.employer_group = employer_group;
	        this.Enrollment_Year = Enrollment_Year;*/
	        this.health_profile = health_profile;
	        this.value = value;

   }
	 
	/* public String getdzee_client() { return dzee_client; }
	 public String getemployer_group() {return employer_group;}
	 public int getEnrollment_Year() { return Enrollment_Year; }*/
	 public int gethealth_profile() { return health_profile; }
	 public double getValue() { return value; }
}
