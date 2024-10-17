# CSE 344 FlightApp Project

## Objectives
To gain experience with database application development and, in particular, transaction management. To learn how to use SQL from within Java via JDBC.

- Design a database of airline flights, their customers, and their reservations.
- Prototype the management service; it should connect to a live database (in Azure) and implement the functionality below.
- The prototype uses a command-line interface (CLI), but in real life you would probably develop a web-based interface.

## Build the application
Package the application files and its dependencies into a single .jar file, then run the main method from FlightService.java (you must run these commands in the project directory where the pom.xml file is located. Otherwise, you will run into an error that says  “...there is no POM in this directory”).
```
$ mvn clean compile assembly:single
$ java -jar target/FlightApp-1.0-jar-with-dependencies.jar
```

If you want to run directly without first creating a jar, you can run:
```
$ mvn compile exec:java
```

If either of those two commands starts the UI below, you are good to go!
```
*** Please enter one of the following commands ***
> create <username> <password> <initial amount>
> login <username> <password>
> search <origin city> <destination city> <direct> <day> <num itineraries>
> book <itinerary id>
> pay <reservation id>
> reservations
> quit
```