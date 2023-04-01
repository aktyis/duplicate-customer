package service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import model.Customer;

public class DatabaseService {

  public void createValidCustomerTable(Connection connection) {
    System.out.println("Creating valid, invalid customer table");

    String validCustomerTable = "Create table if not exists customers ("
        + " ID int GENERATED BY DEFAULT AS IDENTITY primary key,"
        + " firstName varchar(50),"
        + " lastName varchar(50),"
        + " city varchar(50),"
        + " state varchar(50),"
        + " zipCode varchar(50),"
        + " phoneNo varchar(50),"
        + " email varchar(50),"
        + " ip varchar(50)"
        + ");"
        + " Create table if not exists non_valid_customers ("
        + " ID int GENERATED BY DEFAULT AS IDENTITY primary key,"
        + " firstName varchar(50),"
        + " lastName varchar(50),"
        + " city varchar(50),"
        + " state varchar(50),"
        + " zipCode varchar(50),"
        + " phoneNo varchar(50),"
        + " email varchar(50),"
        + " ip varchar(50)"
        + ");";

    Statement statement;
    try {
      statement = connection.createStatement();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    try {
      statement.execute(validCustomerTable);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void saveToDatabase(Connection connection, List<Customer> customers) {

    String INSERT_VALID = "Insert into customers ( firstName,lastName,city,state,"
        + "zipCode,phoneNo,email,ip ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    customerInsert(connection, customers, INSERT_VALID);
  }

  private void customerInsert(Connection connection, List<Customer> customers, String insertSql) {
    PreparedStatement statement;
    try {
      statement = connection.prepareStatement(insertSql);
      for (Customer customer : customers) {
        int paramIndex = 1;

        statement.setObject(paramIndex++, customer.getFirstName());
        statement.setObject(paramIndex++, customer.getLastName());
        statement.setObject(paramIndex++, customer.getCity());
        statement.setObject(paramIndex++, customer.getState());
        statement.setObject(paramIndex++, customer.getZipCode());
        statement.setObject(paramIndex++, customer.getPhoneNo());
        statement.setObject(paramIndex++, customer.getEmail());
        statement.setObject(paramIndex, customer.getIp());
        statement.addBatch();
      }


    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    try {
      int[] rows = statement.executeBatch();
      System.out.println("inserted rows : " + rows.length);

    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void saveToDatabaseInvalid(Connection connection, List<Customer> customers) {
    String INSERT_IN_VALID = "Insert into non_valid_customers ( firstName,lastName"
        + ",city,state,zipCode,phoneNo,email,ip ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
    customerInsert(connection, customers, INSERT_IN_VALID);
  }


}
