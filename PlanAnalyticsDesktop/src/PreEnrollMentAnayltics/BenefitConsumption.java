package PreEnrollMentAnayltics;

import java.sql.Connection;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class BenefitConsumption {
	private static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    private static final String URL = "jdbc:mysql://96.88.82.251:3306/plan_recommend";
    private static final String USER = "root1";
    private static final String PASSWORD = "Dzee2015$";
    private String INSTRUCTIONS = new String();
    private Connection connection = null;
    private String fileName="";

    public static void main(String[] args) throws Exception{

        System.out.println("Main entered");
        BenefitConsumption  db = new BenefitConsumption ();
        final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
        try {
            Class.forName(JDBC_DRIVER);
        } catch (ClassNotFoundException e) {
            System.out.println("Where is your MySQL JDBC Driver?");
            e.printStackTrace();
            return;
        }

        System.out.println("MySQL JDBC Driver Registered!");

        try {
            db.connection = db.getConnection();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        finally{
            db.benefitConsumptionTable();
            if (db.connection != null) {
                System.out.println("You made it, take control of your database now!");
            }
            else {
                System.out.println("Failed to make connection!");
            }
        }
    }

    public  Connection getConnection() throws SQLException
    {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }

    public  void benefitConsumptionTable() throws SQLException
    {
        String s = new String();
        StringBuffer benefitConsumption = new StringBuffer();

        try
        {
            FileReader fr = new FileReader(new File(System.getProperty("user.dir")+ "\\src\\properties_pk\\benefitConsumption.txt"));
            // be sure to not have line starting with "--" or "/*" or any other non alphabetical character

            BufferedReader br = new BufferedReader(fr);

            while((s = br.readLine()) != null)
            {
                benefitConsumption.append(s);
            }
            System.out.println(benefitConsumption);
            br.close();

            // here is our splitter ! We use ";" as a delimiter for each request
            // then we are sure to have well formed statements
            String[] inst = benefitConsumption.toString().split(";");
            Connection conn = getConnection();
            Statement st = conn.createStatement();
            for(int i = 0; i<inst.length; i++)
            {
                // we ensure that there is no spaces before or after the request string
                // in order to not execute empty statements
                if(!inst[i].equals(""))
                {
                    st.executeUpdate(inst[i]);
                    System.out.println(">>"+inst[i]);
                }
            }
        }
        catch(Exception e)
        {
            System.out.println("*** Error : "+e.toString());
            System.out.println("*** Error : ");
            e.printStackTrace();
            System.out.println("################################################");
            System.out.println(benefitConsumption.toString());
        }
    }

}
