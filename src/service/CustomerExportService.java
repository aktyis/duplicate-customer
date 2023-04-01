package service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.List;
import model.Customer;

public class CustomerExportService {

  public void exportToFile(List<Customer> customers, String nameAndBatchNo) {
    if (customers.isEmpty()) {
      return;
    }
    System.out.println("writing file to disk");
    File csvOutputFile = new File("src/resource/customer" + nameAndBatchNo + ".csv");
    try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
      customers.stream()
          .map(Customer::toString)
          .forEach(pw::println);
    } catch (FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }


}
