
import java.sql.*;
import java.util.Random;
import java.time.*;
import java.io.*;
import java.util.*;

/**
 * StockData program implements an application which can be used to query
 * the database called "stock_database.db".  
 * 
 * @author Chandana Jayasundara
 * @version 1.0
 * @since 2016-08-23
 */
public class StockData {
    public static final int NO_OF_RANDOM_ENTRIES = 50;
    private static final String DATABASE_NAME = "stock_database.db";
    
    //Delimiter used in CSV file
    private static final String COMMA_DELIMITER = ",";
    private static final String NEW_LINE_SEPARATOR = "\n";
    
    //CSV file header
    private static final String FILE_HEADER = "Date,Symbol,Open,High,Low,Close";
    
    //CSV file indexes
    private static final int DATE = 0;
    private static final int SYMBOL = 1;
    private static final int OPEN = 2;
    private static final int HIGH = 3;
    private static final int LOW = 4;
    private static final int CLOSE = 5;

    /**
     * StockData Constructor
     */
    public StockData(){
    }
    
    /**
     * This is the main method of the program
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        StockData sdb = new StockData();
        sdb.run();
    }

    /**
     * This is where program will call all the other methods
     */
    public void run(){
        /*Populate random entries in STOCKS table*/
        //populateRandomEntries(c, 50);
        
        /*For a given start and stop dates CSV file will be generated*/
        LocalDate start = LocalDate.of(2005,1,1);
	LocalDate stop = LocalDate.of(2016,6,30);
    	//writeCSV(start,stop);
        
        //Get a dictinary of Stock Symbol and Stock Id
        Dictionary d = getStockIdDictionary();
        
        /*Insert data into STOCK_PRICES table upto a given date*/
        LocalDate dd = LocalDate.of(2008,01,29);
        insertIntoStockPrices(dd,d);
        
        /*Get the last row of STOCK_PRICES table and find maximum date*/
        //insertIntoStockPrices2(getLastRecordOfStockPrices(),d);
  	System.out.println("COMPLETED");
    }
    
    /**
     * This method is used to get the database connection to SQLite database
     * @return Connection Returns the database connection object 
     */
    public Connection getConnection(){
        Connection con = null;
        try{
            Class.forName("org.sqlite.JDBC");
            con = DriverManager.getConnection("jdbc:sqlite:"+DATABASE_NAME);
        }
        catch (Exception e){
            System.err.println(e.getClass().getName()+": "+e.getMessage());
            System.exit(0);
        }
        System.out.println("Opened database successfully");
        return con;
    }
    
    /**
     * This method get all the stock symbols given in STOCKS table and 
     * stores in an string array which will be used later 
     * @return symbolArray contains a list of stock symbols
     */
    public String [] getStockSymbols(){
        String [] symbolArray  = new String[50];
        try{
            Connection con = getConnection();
	    Statement stmt = con.createStatement();
            String sql = "SELECT symbol FROM STOCKS";
            //stml.executeQuery(sql);
            ResultSet rs = stmt.executeQuery(sql);
            int count = 0;
            while (rs.next()) {
                symbolArray[count] = rs.getString("symbol");
                count = count + 1;
            }
            stmt.close();
	    con.close();
	}
	catch(Exception e) {
	    System.err.println(e.getClass().getName()+" : "+e.getMessage());
	    System.exit(0);
	}  
        return symbolArray;
    }

    /**
     * This method returns a dictionary which contains stock_id and stock_symbol
     * @return stockDictionary contains stock_symbol and corresponding stock_id
     */
    public Dictionary getStockIdDictionary(){
        
        Dictionary stockIdDictionary = new Hashtable();
        try{
            Connection con = getConnection();
	    Statement stmt = con.createStatement();
            String sql = "SELECT symbol,stock_id FROM STOCKS";
            ResultSet rs = stmt.executeQuery(sql);
             while (rs.next()) {
                stockIdDictionary.put(rs.getString("symbol"), rs.getString("stock_id"));
            }
            stmt.close();
	    con.close();
	}
	catch(Exception e) {
	    System.err.println(e.getClass().getName()+" : "+e.getMessage());
	    System.exit(0);
	    
	}  
        return stockIdDictionary;
    }
    
    /**
     * This method creates a CVS file containing stock prices for the stocks given in 
     * STOCK table, in the following format:
     * Date, Symbol, Open, High, Low, Close
     * 20050101,ABCDE,10.2,45.5,40.5,50.2
     * Date range is 2005-01-01 to 2016-06-30
     * All stock have price for everyday except weekends for the above duration
     * Entries are sorted by date
     * @param start this is the begining date of stock data
     * @param stop this is the end date of stock data
     */
    public void writeCSV(LocalDate start,LocalDate stop){
        String [] symbolArray = getStockSymbols();
        FileWriter fileWriter = null;
        try {
                File file = new File("test.csv");
                if(file.exists()){
                    fileWriter = new FileWriter("test.csv",true);
                    insertDataIntoCSV(start,stop, symbolArray, fileWriter);
                }
                else{
                    fileWriter = new FileWriter("test.csv");
                    //Write the CSV file header
                    fileWriter.append(FILE_HEADER.toString());
                    //Add a new line separator after the header
                    fileWriter.append(NEW_LINE_SEPARATOR);
                    insertDataIntoCSV(start,stop, symbolArray, fileWriter);
              }
            System.out.println("CSV file was created successfully !!!");
        }catch(Exception e){
            System.err.println("Error in CSV FileWriter");
            e.printStackTrace();
        }finally{
            try{
                fileWriter.flush();
                fileWriter.close();
            }catch(IOException e){ 
                System.out.println("Error while flushing/closing fileWriter !!!");
                e.printStackTrace();
            }
        }
    }
    
   /**
    * This method inserts data into the CSV file for a given period of time
    * @param start begining date
    * @param stop final date
    * @param symboleArray array of strings which contains stock symboles 
    * @param fileWriter FileWriter Object
    */
    public void insertDataIntoCSV(LocalDate start,LocalDate stop, String [] symbolArray, FileWriter fileWriter){
        LocalDate tempDate = start;
        try{
            while(tempDate.isBefore(stop)){
                 //tempDate = tempDate.plusDays(1);
            //write CSV file for start to stop date except SATURDAY and SUNDAY
                if(!(tempDate.getDayOfWeek()).toString().equals("SATURDAY") && !(tempDate.getDayOfWeek()).toString().equals("SUNDAY") ){
                    for (int i=0; i<symbolArray.length; i++){
                        String tempDateSt = tempDate.toString();
                        //tempDateSt.replaceAll("-","x");
                        fileWriter.append(tempDateSt.replaceAll("-",""));
                        fileWriter.append(COMMA_DELIMITER);
                        fileWriter.append(symbolArray[i].toString());
                        fileWriter.append(COMMA_DELIMITER);
                        fileWriter.append(random()+"");
                        fileWriter.append(COMMA_DELIMITER);
                        fileWriter.append(random()+"");
                        fileWriter.append(COMMA_DELIMITER);
                        fileWriter.append(random()+"");
                        fileWriter.append(COMMA_DELIMITER);
                        fileWriter.append(random()+"");
                        fileWriter.append(NEW_LINE_SEPARATOR);
                    }
               }
               tempDate = tempDate.plusDays(1);
            }
        }catch(Exception e){
           System.err.println("Error in insertDateToCSV");
           e.printStackTrace();
        }
    }
    
    /**
     * This method checks the last row of STOCK_PRICES table and and obtain the last date 
     * of the records.
     * @return String lastDate This is the last day of the last record in STOCK_PRICES table
     */
    public String getLastRecordOfStockPrices(){
        //sql = "select max(date) from STOCK_PRICES where stock_id = (select max(stock_id) from STOCK_PRICES)";
        String lastDate = "";
        try{
            Connection con = getConnection();
	    Statement stmt = con.createStatement();
            String sql = "select date from STOCK_PRICES where stock_id = (select max(stock_id) from STOCK_PRICES)";
            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                lastDate = rs.getString("date");
            }
            stmt.close();
            con.close();
        }
        catch(Exception e){
 	    System.err.println(e.getClass().getName()+" : "+e.getMessage());
	    System.exit(0);
        }
        return lastDate;
    }
     
   /**
     * This method process the CSV file and continue populating from the last inserted
     * record in the STOCK_PRICES table
     * @param date This is the last day of the last row of STOCK_PRICES table
     * @param d dictionary containing symbol and stock_id pair
     */
    public void insertIntoStockPrices2(String date, Dictionary d){
        BufferedReader fileReader = null;
        try{
            Connection con = getConnection();
	    Statement stmt = con.createStatement();
            String sql = "";
            fileReader = new BufferedReader(new FileReader("test.csv"));
            fileReader.readLine();
            
            // This is the last date of the last record of STOCK_PRICES
            int tempDate = Integer.parseInt(date.replaceAll("-",""));
            
            String line = "";
            while ((line = fileReader.readLine()) != null) {
                String[] tokens = line.split(COMMA_DELIMITER);
                if (tokens.length > 0) {
                    //if date is grester than the last date of last record of STOCK_PRICES 
                    //continue adding data to STOCK_PRICES
                    if (Integer.parseInt(tokens[DATE]) > tempDate){
                        int tempId = Integer.parseInt((d.get(tokens[SYMBOL])).toString());

                        String tD = tokens[DATE].toString();
                        int yy = Integer.parseInt(tD.substring(0, 4));
                        int mm = Integer.parseInt(tD.substring(5,6));
                        int dd = Integer.parseInt(tD.substring(7,8));
                        sql = "INSERT INTO STOCK_PRICES (stock_id,date,open,high,low,close) VALUES("
                                +Integer.parseInt((d.get(tokens[SYMBOL])).toString())
                                +",'"+LocalDate.of(yy,mm,dd)
                                +"',"+Double.parseDouble(tokens[OPEN])
                                +","+Double.parseDouble(tokens[HIGH])
                                +","+Double.parseDouble(tokens[LOW])
                                +","+Double.parseDouble(tokens[CLOSE])
                                +")";
                        stmt.executeUpdate(sql);
                    }
                }
            }
            
            stmt.close();
	    con.close();
        }catch(Exception e){
 	    System.err.println(e.getClass().getName()+" : "+e.getMessage());
	    System.exit(0);
        }
    }
    
    /**
     * This method insert data into STOCK_PRICES table up to a given date.
     * Read CSV file upto a given date and insert data into STOCK_PRICES
     * @param date up to this date data will be inserted into STOCK_PRICES
     * @param d dictionary containing symbol and stock_id pair
     */
    public void insertIntoStockPrices(LocalDate date, Dictionary d){
        BufferedReader fileReader = null;
        try{
            Connection con = getConnection();
	    Statement stmt = con.createStatement();
            String sql = "";
            fileReader = new BufferedReader(new FileReader("test.csv"));
            fileReader.readLine();
            //String ss = date.toString();
            int tempDate = Integer.parseInt(date.toString().replaceAll("-",""));
            
            String line = "";
            while ((line = fileReader.readLine()) != null) {
                String[] tokens = line.split(COMMA_DELIMITER);
                if (tokens.length > 0) {
                    if (Integer.parseInt(tokens[DATE]) > tempDate){
                        //Exit the While loop
                        break;
                        }
                    int tempId = Integer.parseInt((d.get(tokens[SYMBOL])).toString());
                    
                    String tD = tokens[DATE].toString();
                    int yy = Integer.parseInt(tD.substring(0, 4));
                    int mm = Integer.parseInt(tD.substring(5,6));
                    int dd = Integer.parseInt(tD.substring(7,8))+1;
                    sql = "INSERT INTO STOCK_PRICES (stock_id,date,open,high,low,close) VALUES("
                            +Integer.parseInt((d.get(tokens[SYMBOL])).toString())
                            +",'"+LocalDate.of(yy,mm,dd)
                            +"',"+Double.parseDouble(tokens[OPEN])
                            +","+Double.parseDouble(tokens[HIGH])
                            +","+Double.parseDouble(tokens[LOW])
                            +","+Double.parseDouble(tokens[CLOSE])
                            +")";
                    stmt.executeUpdate(sql);
                    }
                }
            
            stmt.close();
	    con.close();
        }catch(Exception e){
 	    System.err.println(e.getClass().getName()+" : "+e.getMessage());
	    //System.exit(0);
        }
    }
   
    /**
     * This method populates random entries into "stock_database.db"
     * @param entries number of entries to enter into the database
     * @param database name of the database
     */
    public void populateRandomEntries(int entries){
        try{
            Connection con = getConnection();
	    Statement stmt = con.createStatement();
            String sql = "";
            int count = 0;
            while (count < entries){
                //String sql;
                
                sql = "INSERT INTO STOCKS (stock_id,symbol,description) VALUES("+(count+1)+",'"+count+"ABC'"+","+"'Stock"+count+"')";
                stmt.executeUpdate(sql);
                //System.out.println(sql);
                count = count + 1;
            }
	    
	    stmt.close();
	    con.close();
	}
	catch(Exception e) {
	    System.err.println(e.getClass().getName()+" : "+e.getMessage());
	    System.exit(0);
	    
	}       
    }
    
    /**
     * Return a random number
     * @return double x 
     */
    public double random(){
	Random ran = new Random();
	double x = (ran.nextInt(2000)+1000)/100.0;
	return x;
    }
 
}

