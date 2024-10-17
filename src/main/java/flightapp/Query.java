package flightapp;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.*;

/**
 * Runs queries against a back-end database
 */
public class Query extends QueryAbstract {
  //
  // Canned queries
  //

  // Flight capacity
  private static final String FLIGHT_CAPACITY_SQL = 
  "SELECT COUNT(*) AS count " +
  "FROM Reservations_sknguyen " +
  "WHERE first_flight_id = ? OR second_flight_id = ?";
  private PreparedStatement flightCapacityStmt;

  // Clear tables
  private static final String CLEAR_USERS_SQL = "DELETE FROM Users_sknguyen";
  private PreparedStatement clearUsersStmt;
  private static final String CLEAR_RESERVATIONS_SQL = "DELETE FROM Reservations_sknguyen";
  private PreparedStatement clearReservationsStmt;

  // Create Customer
  private static final String INSERT_USER_SQL = "INSERT INTO Users_sknguyen VALUES (?,?,?)"; 
  private PreparedStatement insertUserStmt;

  // Login
  private static final String GET_USER_SQL = "SELECT salted_hashed_password FROM Users_sknguyen WHERE username = ?";
  private PreparedStatement getUserStmt;

  // Search
  private static final String GET_ONE_HOP_SQL = 
    "SELECT TOP (?) fid, day_of_month, carrier_id, flight_num, origin_city, dest_city, actual_time, capacity, price " +
    "FROM Flights " +
    "WHERE origin_city = ? " +
    "AND dest_city = ? " + 
    "AND day_of_month = ? " +
    "AND canceled = 0 " + 
    "ORDER BY actual_time ASC";
  private PreparedStatement getOneHopStmt;
  private static final String GET_TWO_HOP_SQL = 
    "SELECT TOP (?) " +
    "F1.fid AS F1_fid, F1.day_of_month AS F1_day_of_month, F1.carrier_id AS F1_carrier_id, F1.flight_num AS F1_flight_num, " +
    "F1.origin_city AS F1_origin_city, F1.dest_city AS F1_dest_city, F1.actual_time AS F1_actual_time, F1.capacity AS F1_capacity, " +
    "F1.price AS F1_price, " +
    "F2.fid AS F2_fid, F2.day_of_month AS F2_day_of_month, F2.carrier_id AS F2_carrier_id, F2.flight_num AS F2_flight_num, " +
    "F2.origin_city AS F2_origin_city, F2.dest_city AS F2_dest_city, F2.actual_time AS F2_actual_time, F2.capacity AS F2_capacity, " +
    "F2.price AS F2_price, " +
    "(F1.actual_time + F2.actual_time) AS total_time " +
    "FROM Flights AS F1, Flights AS F2 " +
    "WHERE F1.origin_city = ? AND F2.dest_city = ? " + 
    "AND F1.dest_city = F2.origin_city " +
    "AND F1.day_of_month = ? " + 
    "AND F1.canceled = 0 AND F2.canceled = 0 " +
    "AND F1.day_of_month = F2.day_of_month " +
    "ORDER BY total_time ASC";
  private PreparedStatement getTwoHopStmt;

  // Reservation
  private static final String GET_USER_RES_SQL = "SELECT * FROM Reservations_sknguyen WHERE res_username = ?";
  private PreparedStatement getUserResStmt;
  private static final String GET_FLIGHT_SQL = "SELECT * FROM Flights WHERE fid = ?";
  private PreparedStatement getFlightStmt;

  // Book
  private static final String GET_RES_DATES_SQL = 
    "SELECT day_of_month " + 
    "FROM Reservations_sknguyen AS R, Flights AS F " + 
    "WHERE R.res_username = ? AND F.fid = R.first_flight_id";
  private PreparedStatement getResDatesStmt;
  private static final String INSERT_RES_SQL = "INSERT INTO Reservations_sknguyen VALUES (?, ?, 0, ?, ?, ?)";
  private PreparedStatement insertResStmt;
  private static final String GET_NUM_RES_SQL = "SELECT COUNT(*) AS count FROM Reservations_sknguyen";
  private PreparedStatement getNumResStmt; 
  

  // Pay
  private static final String GET_RES_SQL =
    "SELECT * " +
    "FROM Reservations_sknguyen " +
    "WHERE reservation_id = ? AND res_username = ?";
  private PreparedStatement getResStmt;
  private static final String GET_USER_BAL_SQL = "SELECT balance FROM Users_sknguyen WHERE username = ?";
  private PreparedStatement getUserBalStmt;
  private static final String UPDATE_USER_BAL_SQL = "UPDATE Users_sknguyen SET balance = ? WHERE username = ?";
  private PreparedStatement updateUserBalStmt;
  private static final String UPDATE_RES_TO_PAID_SQL = "UPDATE Reservations_sknguyen SET is_paid = 1 WHERE reservation_id = ?";
  private PreparedStatement updateResToPaidStmt;

  //
  // Instance variables
  //
  private String currentLogInUser;
  private List<Itinerary> itineraries;

  protected Query() throws SQLException, IOException {
    prepareStatements();
  }

  /**
   * Clear the data in any custom tables created.
   * 
   * WARNING! Do not drop any tables and do not clear the flights table.
   */
  public void clearTables() {
    try {
      clearReservationsStmt.executeUpdate();
      clearUsersStmt.executeUpdate();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*
   * prepare all the SQL statements in this method.
   */
  private void prepareStatements() throws SQLException {
    flightCapacityStmt = conn.prepareStatement(FLIGHT_CAPACITY_SQL);

    clearUsersStmt = conn.prepareStatement(CLEAR_USERS_SQL);
    clearReservationsStmt = conn.prepareStatement(CLEAR_RESERVATIONS_SQL);

    insertUserStmt = conn.prepareStatement(INSERT_USER_SQL);
    getUserStmt = conn.prepareStatement(GET_USER_SQL);

    getOneHopStmt = conn.prepareStatement(GET_ONE_HOP_SQL);
    getTwoHopStmt = conn.prepareStatement(GET_TWO_HOP_SQL);

    getUserResStmt = conn.prepareStatement(GET_USER_RES_SQL);
    getFlightStmt = conn.prepareStatement(GET_FLIGHT_SQL);

    getResDatesStmt = conn.prepareStatement(GET_RES_DATES_SQL);
    insertResStmt = conn.prepareStatement(INSERT_RES_SQL);
    getNumResStmt = conn.prepareStatement(GET_NUM_RES_SQL);

    getResStmt = conn.prepareStatement(GET_RES_SQL);
    getUserBalStmt = conn.prepareStatement(GET_USER_BAL_SQL);
    updateUserBalStmt = conn.prepareStatement(UPDATE_USER_BAL_SQL);
    updateResToPaidStmt = conn.prepareStatement(UPDATE_RES_TO_PAID_SQL);
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_login(String username, String password) {
    try {
      // check if there's a user logged in
      if (currentLogInUser != null) {
        return "User already logged in\n";
      }
      
      String lcUsername = username.toLowerCase();
      getUserStmt.clearParameters();
      getUserStmt.setString(1, lcUsername);
      ResultSet userResults = getUserStmt.executeQuery();
      boolean userExists = userResults.next();

      // check if user exists
      if (!userExists) {
        return "Login failed\n"; 
      }

      // get the salted/hashed password
      byte[] storedPassword = userResults.getBytes("salted_hashed_password");
      userResults.close();

      boolean correctPassword = PasswordUtils.plaintextMatchesSaltedHash(password, storedPassword);
      if (correctPassword) {
        currentLogInUser = lcUsername;
        itineraries = null;
        return "Logged in as " + username + "\n";
      }
      return "Login failed\n";
    } catch (Exception e) {
      e.printStackTrace();
      return "Login failed\n";
    }
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_createCustomer(String username, String password, int initAmount) {
    try {
      String lcUsername = username.toLowerCase();
      getUserStmt.clearParameters();
      getUserStmt.setString(1, lcUsername);
      ResultSet userResults = getUserStmt.executeQuery();
      
      // fail if balance is negative or username already exists
      if (initAmount < 0 || userResults.next()) {
        return "Failed to create user\n";
      }
      userResults.close();

      byte[] saltPlusSaltedHash = PasswordUtils.saltAndHashPassword(password);
      insertUserStmt.clearParameters();
      insertUserStmt.setString(1, lcUsername);
      insertUserStmt.setBytes(2, saltPlusSaltedHash);
      insertUserStmt.setInt(3, initAmount);
      insertUserStmt.executeUpdate();
      
      return "Created user " + username + "\n";
    } catch (Exception e) {
      e.printStackTrace();
      return "Failed to create user\n";
    }
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_search(String originCity, String destinationCity, 
                                   boolean directFlight, int dayOfMonth,
                                   int numberOfItineraries) {
    try {
      StringBuffer sb = new StringBuffer();
      
      // one hop itineraries
      getOneHopStmt.clearParameters();
      getOneHopStmt.setInt(1, numberOfItineraries);
      getOneHopStmt.setString(2, originCity);
      getOneHopStmt.setString(3, destinationCity);
      getOneHopStmt.setInt(4, dayOfMonth);

      ResultSet oneHopResults = getOneHopStmt.executeQuery();

      itineraries = new ArrayList<>();
      while (oneHopResults.next()) {
        Flight f1 = new Flight(
          oneHopResults.getInt("fid"),
          oneHopResults.getInt("day_of_month"),
          oneHopResults.getString("carrier_id"),
          oneHopResults.getString("flight_num"),
          oneHopResults.getString("origin_city"),
          oneHopResults.getString("dest_city"),
          oneHopResults.getInt("actual_time"),
          oneHopResults.getInt("capacity"),
          oneHopResults.getInt("price")
        );

          itineraries.add(new Itinerary(f1));
      }
      oneHopResults.close();
      
      int itinerariesLeft = numberOfItineraries - itineraries.size();

      if (!directFlight && itinerariesLeft > 0) {
        getTwoHopStmt.clearParameters();
        getTwoHopStmt.setInt(1, itinerariesLeft);
        getTwoHopStmt.setString(2, originCity);
        getTwoHopStmt.setString(3, destinationCity);
        getTwoHopStmt.setInt(4, dayOfMonth);
        
        ResultSet twoHopResults = getTwoHopStmt.executeQuery();

        while (twoHopResults.next()) {
          Flight f1 = new Flight(
            twoHopResults.getInt("F1_fid"),
            twoHopResults.getInt("F1_day_of_month"),
            twoHopResults.getString("F1_carrier_id"),
            twoHopResults.getString("F1_flight_num"),
            twoHopResults.getString("F1_origin_city"),
            twoHopResults.getString("F1_dest_city"),
            twoHopResults.getInt("F1_actual_time"),
            twoHopResults.getInt("F1_capacity"),
            twoHopResults.getInt("F1_price")
          );

          Flight f2 = new Flight(
            twoHopResults.getInt("F2_fid"),
            twoHopResults.getInt("F2_day_of_month"),
            twoHopResults.getString("F2_carrier_id"),
            twoHopResults.getString("F2_flight_num"),
            twoHopResults.getString("F2_origin_city"),
            twoHopResults.getString("F2_dest_city"),
            twoHopResults.getInt("F2_actual_time"),
            twoHopResults.getInt("F2_capacity"),
            twoHopResults.getInt("F2_price"));
            itineraries.add(new Itinerary(f1, f2)
          );
        }
        twoHopResults.close();
      }

      if (itineraries.isEmpty()) {
        return "No flights match your selection\n";
      }

      Collections.sort(itineraries);

      for (int i = 0; i < itineraries.size(); i++) {
        sb.append("Itinerary " + i + ": " + itineraries.get(i).toString() + "\n");
      }

      return sb.toString();
    } catch (SQLException e) {
      e.printStackTrace();
      return "Failed to search\n";
    }
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_book(int itineraryId) {
    if (currentLogInUser == null) {
      return "Cannot book reservations, not logged in\n";
    } else if (itineraries == null || itineraryId < 0 || itineraryId > itineraries.size() - 1) {
      return "No such itinerary " + itineraryId + "\n";
    }
    Itinerary itnToBook = itineraries.get(itineraryId);

    while (true) {
      try {
        conn.setAutoCommit(false);
        getResDatesStmt.clearParameters();
        getResDatesStmt.setString(1, currentLogInUser);
        ResultSet resDatesResult = getResDatesStmt.executeQuery();

        while (resDatesResult.next()) {
          int curDay = resDatesResult.getInt("day_of_month");
          if (itnToBook.f1.dayOfMonth == curDay) {
            resDatesResult.close();
            conn.rollback();
            conn.setAutoCommit(true);
            return "You cannot book two flights in the same day\n";
          }
        }
        resDatesResult.close();

        if (checkFlightCapacity(itnToBook.f1)) {
          ResultSet getNumResResult = getNumResStmt.executeQuery();
          int curNumRes = 0;
          if (getNumResResult.next()) {
            curNumRes = getNumResResult.getInt("count");
          }
          getNumResResult.close();

          insertResStmt.clearParameters();
          insertResStmt.setInt(1, curNumRes + 1);
          insertResStmt.setInt(4, itnToBook.f1.fid);

          int totalPrice = itnToBook.f1.price;

          if (!itnToBook.isDirect) {
            if (checkFlightCapacity(itnToBook.f2)) {
              totalPrice += itnToBook.f2.price;
              insertResStmt.setInt(5, itnToBook.f2.fid);
            } else {
              conn.rollback();
              conn.setAutoCommit(true);
              return "Booking failed\n";
            }
          } else {
            insertResStmt.setNull(5, Types.INTEGER);
          }
          insertResStmt.setString(2, currentLogInUser);
          insertResStmt.setInt(3, totalPrice);
          insertResStmt.executeUpdate();

          conn.commit();
          conn.setAutoCommit(true);
          return "Booked flight(s), reservation ID: " + (curNumRes + 1) + "\n";
        } else {
          conn.rollback();
          conn.setAutoCommit(true);
          return "Booking failed\n";
        }
      } catch (SQLException e1) {
        e1.printStackTrace();
        try {
          conn.rollback();
          conn.setAutoCommit(true);
        } catch (SQLException e2) {
          if (!isDeadlock(e1) && !isDeadlock(e2)) {
            return "Booking failed\n";
          }
        }
      }
    }
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_pay(int reservationId) {
    if (currentLogInUser == null) {
        return "Cannot pay, not logged in\n";
    }
    while (true) {
      try {
        conn.setAutoCommit(false);
        getResStmt.clearParameters();
        getResStmt.setInt(1, reservationId);
        getResStmt.setString(2, currentLogInUser);
        ResultSet getResResult = getResStmt.executeQuery();

        if (!getResResult.next() || getResResult.getInt("is_paid") == 1) {
          getResResult.close();
          conn.rollback();
          conn.setAutoCommit(true);
          return "Cannot find unpaid reservation " + reservationId + " under user: " + currentLogInUser + "\n";
        }
        int cost = getResResult.getInt("total_price");
        getResResult.close();

        getUserBalStmt.clearParameters();
        getUserBalStmt.setString(1, currentLogInUser);
        ResultSet getUserBalResult = getUserBalStmt.executeQuery();
        getUserBalResult.next();
        int balance = getUserBalResult.getInt("balance");
        getResResult.close();

        if (cost > balance) {
          conn.rollback();
          conn.setAutoCommit(true);
          return "User has only " + balance + " in account but itinerary costs " + cost +"\n";
        }

        updateUserBalStmt.clearParameters();
        updateUserBalStmt.setInt(1, balance - cost);
        updateUserBalStmt.setString(2, currentLogInUser);
        updateUserBalStmt.executeUpdate();

        updateResToPaidStmt.clearParameters();
        updateResToPaidStmt.setInt(1, reservationId);
        updateResToPaidStmt.executeUpdate();

        conn.commit();
        conn.setAutoCommit(true);
        return "Paid reservation: " + reservationId + " remaining balance: " + (balance - cost) + "\n";
      } catch (SQLException e1) {
        e1.printStackTrace();
        try {
          conn.rollback();
          conn.setAutoCommit(true);
        } catch (SQLException e2) {
          if (!isDeadlock(e1) && !isDeadlock(e2)) {
            return "Booking failed\n";
          }
        }
        return "Failed to pay for reservation " + reservationId + "\n";
      }
    }
  }

  /* See QueryAbstract.java for javadoc */
  public String transaction_reservations() {
    try {
      if (currentLogInUser == null) {
        return "Cannot view reservations, not logged in\n";
      }

      StringBuffer sb = new StringBuffer();

      // Get all reservation 
      getUserResStmt.clearParameters();
      getUserResStmt.setString(1, currentLogInUser);
      ResultSet getUserResResult = getUserResStmt.executeQuery();

      int numReservations = 0; 

      while (getUserResResult.next()) {
        sb.append("Reservation " + getUserResResult.getInt("reservation_id") + " ");
        if (getUserResResult.getInt("is_paid") == 1) {
          sb.append("paid: true:\n");
        } else {
          sb.append("paid: false:\n");
        }
        
        int f1_fid = getUserResResult.getInt("first_flight_id");
        getFlightStmt.clearParameters();
        getFlightStmt.setInt(1, f1_fid);
        ResultSet firstFlightResult = getFlightStmt.executeQuery();
        firstFlightResult.next();
        Flight f1 = new Flight(
          firstFlightResult.getInt("fid"),
          firstFlightResult.getInt("day_of_month"),
          firstFlightResult.getString("carrier_id"),
          firstFlightResult.getString("flight_num"),
          firstFlightResult.getString("origin_city"),
          firstFlightResult.getString("dest_city"),
          firstFlightResult.getInt("actual_time"),
          firstFlightResult.getInt("capacity"),
          firstFlightResult.getInt("price")
        );
        firstFlightResult.close();
        sb.append(f1.toString() + "\n");

        int f2_fid = getUserResResult.getInt("second_flight_id");
        if (f2_fid != 0) {
          getFlightStmt.clearParameters();
          getFlightStmt.setInt(1, f2_fid);
          ResultSet secondFlightResult = getFlightStmt.executeQuery();
          secondFlightResult.next();
          Flight f2 = new Flight(
            secondFlightResult.getInt("fid"),
            secondFlightResult.getInt("day_of_month"),
            secondFlightResult.getString("carrier_id"),
            secondFlightResult.getString("flight_num"),
            secondFlightResult.getString("origin_city"),
            secondFlightResult.getString("dest_city"),
            secondFlightResult.getInt("actual_time"),
            secondFlightResult.getInt("capacity"),
            secondFlightResult.getInt("price")
          );
          secondFlightResult.close();
          sb.append(f2.toString() + "\n");
        }

        numReservations++;
      }
      getUserResResult.close();

      if (numReservations == 0) {
        return "No reservations found\n";
      }

      return sb.toString();
    } catch (SQLException e){
      e.printStackTrace();
      return "Failed to retrieve reservations\n";
    }
    
  }

  /**
   * Example utility function that uses prepared statements
   */
  private boolean checkFlightCapacity(Flight flight) throws SQLException {
    flightCapacityStmt.clearParameters();
    flightCapacityStmt.setInt(1, flight.fid);
    flightCapacityStmt.setInt(2, flight.fid);
    ResultSet flightCapacityResult = flightCapacityStmt.executeQuery();

    if (flightCapacityResult.next()) {
      if (flight.capacity - flightCapacityResult.getInt("count") <= 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Utility function to determine whether an error was caused by a deadlock
   */
  private static boolean isDeadlock(SQLException e) {
    return e.getErrorCode() == 1205;
  }

  /**
   * A class to store information about a single flight
   */
  class Flight {
    public int fid;
    public int dayOfMonth;
    public String carrierId;
    public String flightNum;
    public String originCity;
    public String destCity;
    public int time;
    public int capacity;
    public int price;

    Flight(int id, int day, String carrier, String fnum, String origin, String dest, int tm,
           int cap, int pri) {
      fid = id;
      dayOfMonth = day;
      carrierId = carrier;
      flightNum = fnum;
      originCity = origin;
      destCity = dest;
      time = tm;
      capacity = cap;
      price = pri;
    }
    
    @Override
    public String toString() {
      return "ID: " + fid + " Day: " + dayOfMonth + " Carrier: " + carrierId + " Number: "
          + flightNum + " Origin: " + originCity + " Dest: " + destCity + " Duration: " + time
          + " Capacity: " + capacity + " Price: " + price;
    }
  }

  /**
   * A class to store information about a single itinerary
   */
  class Itinerary implements Comparable<Itinerary> {
    public Flight f1;
    public Flight f2;
    public boolean isDirect; // true = direct, false = indirect
    public int totalDuration;

    public Itinerary(Flight f1) {
      this(f1, null);
    }

    public Itinerary(Flight f1, Flight f2) {
      this.f1 = f1;
      this.f2 = f2;

      if (f2 == null) {
        this.isDirect = true;
        this.totalDuration = f1.time;
      } else {
        this.isDirect = false;
        this.totalDuration = f1.time + f2.time;
      }
    }

    @Override
    public String toString() {
      int numFlights = 1;
      String firstFlight = f1.toString();
      String secondFlight = "";

      if (!this.isDirect) {
        numFlights = 2;
        firstFlight += "\n";
        secondFlight = f2.toString();
      }

      return numFlights + " flight(s), " + this.totalDuration + " minutes" + "\n" 
             + firstFlight + secondFlight;
    }

    @Override
    public int compareTo(Itinerary o) {
      if (this.totalDuration > o.totalDuration) {
        return 1;
      } else if (this.totalDuration == o.totalDuration) {
        if (this.f1.fid > o.f1.fid) {
          return 1;
        } else if (this.f1.fid == o.f1.fid && !this.isDirect && !o.isDirect) {
          if (this.f2.fid > o.f2.fid) { 
            return 1;
          } else if (this.f2.fid == o.f2.fid) { 
            return 0; // might not need this case
          }
        } else if (this.f1.fid == o.f1.fid && this.isDirect && o.isDirect) {
          return 0; // might not need this case
        }
      }
      return -1;
    }
  }
}