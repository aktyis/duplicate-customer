package service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Stack;
import model.Customer;


public class CustomerImportService {

  public static final String RESOURCE_1_M_CUSTOMERS_TXT = "src/resource/1M-customers.txt";

  public Stack<Customer> importFromFile() {
    System.out.println("Reading file from disk");
    Stack<Customer> customerStack = new Stack<>();

    String splitBy = ",";
    try {
      BufferedReader br = new BufferedReader(new FileReader(RESOURCE_1_M_CUSTOMERS_TXT));
      String line = "";
      while ((line = br.readLine()) != null) {
        String[] record = line.split(splitBy);
        Customer customer;
        if (record.length == 8) {
          customer = new Customer(
              record[0],
              record[1],
              record[2],
              record[3],
              record[4],
              record[5],
              record[6],
              record[7]
          );
        } else {
          customer = new Customer(
              record[0],
              record[1],
              record[2],
              record[3],
              record[4],
              record[5],
              record[6],
              ""
          );
        }
        customerStack.push(customer);

      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return customerStack;

  }
}
