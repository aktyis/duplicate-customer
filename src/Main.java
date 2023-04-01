import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.EmptyStackException;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import model.Customer;
import service.CustomerExportService;
import service.CustomerImportService;
import service.DatabaseService;

public class Main implements Callable<Boolean> {

  public static final int BATCH_SIZE = 100000;
  public static final int N_THREADS = 10;
  public static final String VALID_BATCH_FILE_PREFIX = "_valid_batch_";
  public static final String INVALID_BATCH_FILE_PREFIX = "_invalid_batch_";
  static Stack<Customer> customerStack;
  static Stack<Customer> inValidCustomerStack;
  static ConcurrentHashMap<String, Customer> validCustomerMap = new ConcurrentHashMap<>();


  public static void main(String[] args) {
    LocalDateTime start = LocalDateTime.now();
    System.out.println("***  After App start:" + LocalDateTime.now());

    inValidCustomerStack = new Stack<>();
    String jdbcURL = "jdbc:h2:mem:test";

    Connection connection;
    try {
      connection = DriverManager.getConnection(jdbcURL);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    CustomerImportService customerImportService = new CustomerImportService();
    customerStack = customerImportService.importFromFile();
    System.out.println("customerStack : " + customerStack.size());
    System.out.printf("***  After file read operation: %d\n",
        ChronoUnit.SECONDS.between(start, LocalDateTime.now()));
    DatabaseService databaseService = new DatabaseService();
    databaseService.createValidCustomerTable(connection);

    CustomerExportService customerExportService = new CustomerExportService();

    // Process to multithreading
    ExecutorService executor = Executors.newFixedThreadPool(N_THREADS);
    List<Future<Boolean>> list;
    Callable<Boolean> callable = new Main();
    list = IntStream.range(0, 100).mapToObj(i -> executor.submit(callable))
        .collect(Collectors.toList());

    for (Future<Boolean> fut : list) {
      try {
        fut.get();
//        System.out.println(LocalDateTime.now() + "::" + fut.get());
      } catch (InterruptedException | ExecutionException e) {
        e.printStackTrace();
      }
    }
    executor.shutdown();
    System.out.println(LocalDateTime.now());
    System.out.println("inValidCustomers : " + inValidCustomerStack.size());
    System.out.println("validCustomerMap : " + validCustomerMap.size());
    System.out.printf("*** After file process operation: %d\n",
        ChronoUnit.SECONDS.between(start, LocalDateTime.now()));
    // csv processed to map

    //start save operation
    List<Customer> customers = new ArrayList<>();
    Connection finalConnection = connection;
    //valid customer block
    AtomicInteger batchCounter = new AtomicInteger();
    validCustomerMap.forEach((s, customer) -> {
      if (customers.size() == BATCH_SIZE) {
        saveAndClear(databaseService, customers, finalConnection, true);
        customerExportService.exportToFile(customers,
            VALID_BATCH_FILE_PREFIX + batchCounter.getAndIncrement());
      } else {
        customers.add(customer);
      }
    });
    if (!customers.isEmpty()) {
      //save the remaining customers
      customerExportService.exportToFile(customers,
          VALID_BATCH_FILE_PREFIX + batchCounter.getAndIncrement());
      saveAndClear(databaseService, customers, finalConnection, true);
    }

    //Invalid customer block
    batchCounter.set(0);
    customers.clear();

    inValidCustomerStack.forEach(customer -> {
      if (customers.size() == BATCH_SIZE) {
        customerExportService.exportToFile(customers,
            INVALID_BATCH_FILE_PREFIX + batchCounter.getAndIncrement());
        saveAndClear(databaseService, customers, finalConnection, false);
      } else {
        customers.add(customer);
      }
    });
    if (!customers.isEmpty()) {
      //save the remaining customers
      customerExportService.exportToFile(customers,
          INVALID_BATCH_FILE_PREFIX + batchCounter.getAndIncrement());
      saveAndClear(databaseService, customers, finalConnection, false);
    }
    System.out.printf("***  After save to db operation & file operation: %d\n",
        ChronoUnit.SECONDS.between(start, LocalDateTime.now()));
    System.out.printf("***  total execution time :%d\n",
        ChronoUnit.SECONDS.between(start, LocalDateTime.now()));

    try {
      connection.close();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private static void saveAndClear(DatabaseService databaseService, List<Customer> customers,
      Connection finalConnection, boolean isValid) {
    if (isValid) {
      databaseService.saveToDatabase(finalConnection, customers);
    } else {
      databaseService.saveToDatabaseInvalid(finalConnection, customers);
    }
    customers.clear();
  }

  @Override
  public Boolean call() {
    //Code block to validate and check duplicate customer
    while (!customerStack.empty()) {
      //System.out.println("looping ***" + Thread.currentThread().getName());
      Customer customer;
      try {
        customer = customerStack.pop();
      } catch (EmptyStackException e) {
        System.out.println("EmptyStackException");
        break;
      }
      if (customer.validate()) {
        //System.out.println("valid");

        if (validCustomerMap.containsKey(keyBuilder(customer))) {
          inValidCustomerStack.push(customer);
        } else {
          validCustomerMap.put(keyBuilder(customer), customer);
        }
      } else {
        //System.out.println("invalid");
        inValidCustomerStack.push(customer);
      }
    }
    return true;
  }

  private static String keyBuilder(Customer customer) {
    return customer.getPhoneNo() + customer.getEmail();
  }
}