A Java program that reads data from a CSV file which contains stock price data and write to a database.

Program contains the following files

StockData.java - main java class
All methods are implemented in this class

stock_database.db - Sqlite database which contains STOCKS and STOCK_PRICES tables

test.csv - CSV file which contains sotck data from date 2005-01-01 to 2016-06-30

sqlite-jdbc-3.7.2.jar - Sqlite JDBC driver

Compile:
javac StockData.java

Run:
java -cp ".:sqlite-jdbc-3.7.2.jar" StockData


