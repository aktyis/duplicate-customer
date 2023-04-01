package model;

import java.util.Objects;
import java.util.regex.Pattern;

public class Customer {

  private static final String REGEX_PHONE = "^(1 |)(\\([0-9]{3}\\)( |)|[0-9]{3})"
      + "( |-|)[0-9]{3}( |-|)[0-9]{4}";

  private static final String REGEX_EMAIL = "^(?=.{1,64}@)[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)*@"
      + "[^-][A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)*(\\.[A-Za-z]{2,})$";

  private String firstName;
  private String lastName;
  private String city;
  private String state;
  private String zipCode;
  private String phoneNo;
  private String email;
  private String ip;

  public Customer(String firstName, String lastName, String city, String state, String zipCode,
      String phoneNo, String email, String ip) {
    this.firstName = firstName;
    this.lastName = lastName;
    this.city = city;
    this.state = state;
    this.zipCode = zipCode;
    this.phoneNo = phoneNo;
    this.email = email;
    this.ip = ip;
  }

  public String getFirstName() {
    return firstName;
  }

  public void setFirstName(String firstName) {
    this.firstName = firstName;
  }

  public String getLastName() {
    return lastName;
  }

  public void setLastName(String lastName) {
    this.lastName = lastName;
  }

  public String getCity() {
    return city;
  }

  public void setCity(String city) {
    this.city = city;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public String getZipCode() {
    return zipCode;
  }

  public void setZipCode(String zipCode) {
    this.zipCode = zipCode;
  }

  public String getPhoneNo() {
    return phoneNo;
  }

  public void setPhoneNo(String phoneNo) {
    this.phoneNo = phoneNo;
  }

  public String getEmail() {
    return email;
  }

  public void setEmail(String email) {
    this.email = email;
  }

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Customer customer = (Customer) o;
    return phoneNo.equals(customer.phoneNo) && email.equals(customer.email);
  }

  @Override
  public int hashCode() {
    return Objects.hash(phoneNo, email);
  }
  public boolean validate() {
    if (this.email != null && this.phoneNo != null) {
      return patternMatches(this.email, REGEX_EMAIL)
          && patternMatches(this.phoneNo, REGEX_PHONE);
    }
    return false;
  }

  public static boolean patternMatches(String match, String regexPattern) {
    return Pattern.compile(regexPattern)
        .matcher(match)
        .matches();
  }

  @Override
  public String toString() {
    return firstName +
        ", " + lastName +
        ", " + city +
        ", " + state +
        ", " + zipCode +
        ", " + phoneNo +
        ", " + email +
        ", " + ip;
  }
}
