import jdk.nashorn.internal.scripts.JD;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Command-line application for a set of queries
 * for our Movie Database
 * 
 * Names: Darrien Kennedy and Justin Gagne
 * Section: COMP 2670 - 09
 * 
 * @author Darrien Kennedy
 * @author Justin Gagne
 */


public class MovieApp {

	private static MovieApp instance;
	private static Connection mConnection = null;
	private static String databaseName = "Movies";
	private static String host = "localhost";
	private static String port = "3306";
	private static String user = "gagnej3";
	private static String password = "comp2670";
	private static String DB_URL = "jdbc:mysql://localhost/EMP";
	private static String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	private static String url = "jdbc:mysql://"+host+":"+port+"/"+databaseName+ "?user=" + user + "&password=" + password;

	public static enum QueryTypes{
		ACTORS_AND_MORE,
		REVENUE_BY_DIRECTOR,
		SAME_MOVIE,
		TOP_MOVIE_IN_GENRE,
		WHO_WON_WHAT
	}

	private static class QueryData {
		final public QueryTypes queryType;
		final public String queryParam;

		public QueryData(QueryTypes qn, String qp) {
			queryType = qn;
			queryParam = qp;
		}

		@Override
		public String toString() {
			return String.format("%s" + ((queryParam == null)?(""):(" (%s)")), queryType, queryParam);
		}
	}


	/**
	 * Usage statement, then exit
	 * @return null (to make other code easier)
	 */
	private static QueryData _usage() {
		System.out.printf("Usage: java %s <path to Chinook database> <query #> [parameter value]%n%n", MovieApp.class.getCanonicalName());
		System.out.printf("1) Find all people who are actors and participants.\n");
		System.out.printf("2) Find the total revenue made by a director: [first name] [last name]\n");
		System.out.printf("3) Find all movies starring two specified actors: [first name 1] [last name 1] [first name 2] [last name 2]\n");
		System.out.printf("4) Find the top movies of a specified genre: [genre name]\n");
		System.out.printf("5) Find the person with the most awards. List all award information.\n");
		System.exit(0);
		return null;
	}



	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		
		// validates the inputs, exits if bad
		final QueryData qd = validateInputs(args);		
		
		// Try to make connection to the database
		try (final Connection connection = DriverManager.getConnection(url)) {

			if (qd.queryType == QueryTypes.ACTORS_AND_MORE) {

				//Query that
				String sqlActorsMore =
						"SELECT DISTINCT Person.FirstName AS first_name, Person.LastName AS last_name, Movies.Title AS title, "
							+ "ParticipantDid.Produced AS produced, ParticipantDid.Directed AS directed, "
							+ "ParticipantDid.Composed AS composed, GROUP_CONCAT(Genre.Name SEPARATOR ',') AS movie_genre "
							+ "FROM (((((Actor INNER JOIN Person ON Person.ID = Actor.PersonId) "
							+ "INNER JOIN Participant ON Participant.PersonId = Person.ID) "
							+ "INNER JOIN ParticipantDid ON Participant.PersonId = ParticipantDid.PersonId) "
							+ "INNER JOIN Movies ON Movies.ID = ParticipantDid.MovieId) "
							+ "INNER JOIN MovieGenre ON Movies.ID = MovieGenre.MovieId) "
							+ "INNER JOIN Genre ON MovieGenre.GenreId = Genre.ID "
							+ "GROUP BY Movies.ID "
							+ "ORDER BY Person.LastName ASC, Person.FirstName ASC, Movies.Title ASC;";

				//Create the PreparedStatement for the sql, no arguments used for this one
				PreparedStatement stmt = connection.prepareStatement(sqlActorsMore);

				//Create the result set and print out the number of customers by country.
				final ResultSet res = stmt.executeQuery();
				while ( res.next() ) {
					System.out.printf("%s %s %s %s %s %s %s\n", res.getString("first_name"), res.getString("last_name"),
							res.getString("title"), res.getString("produced"),  res.getString("directed"),
							res.getString("composed"), res.getString("movie_genre"));
				}
				
			} else if (qd.queryType == QueryTypes.REVENUE_BY_DIRECTOR) {
				
				String sqlDirectorRev =
						"SELECT Person.FirstName AS first, Person.LastName AS last, COUNT(Movies.Title) AS movies_directed, "
							+ "CONCAT('$' , FORMAT(SUM(Cast(REPLACE(Movies.Revenue, ',','') AS INT)), 0)) AS total_revenue "
							+ "FROM ((Person INNER JOIN Participant ON Person.ID = Participant.PersonId) "
							+ "INNER JOIN ParticipantDid ON Participant.PersonId = ParticipantDid.PersonId) "
							+ "INNER JOIN Movies ON Movies.ID = ParticipantDid.MovieId "
							+ "WHERE (ParticipantDid.Directed = 'YES') "
							+ "GROUP BY Person.ID "
							+ "ORDER BY last ASC, first ASC;";
				
				//Create the PreparedStatement for the sql
				PreparedStatement stmt = connection.prepareStatement(sqlDirectorRev);
				
				//Pass in the arguments
				
				
				
				//Create the result set in the structure of "Id. FirstName LastName (Title)" and print it.
				final ResultSet res = stmt.executeQuery();
				while(res.next()){
					System.out.println(res.getString("first") + " " + res.getString("last") + " " + res.getString("movies_directed") + " " + res.getString("total_revenue"));
				}
				
			} else if (qd.queryType == QueryTypes.SAME_MOVIE) {
				
				String sqlSameMovie =
						"SELECT Movies.Title as movie_title "
							+ "FROM ((Person INNER JOIN Actor ON Person.ID = Actor.PersonId) "
							+ "INNER JOIN Cast ON Cast.PersonId = Actor.PersonId) "
							+ "INNER JOIN Movies ON Cast.MovieId = Movies.ID "
							+ "WHERE (Person.FirstName = ? AND Person.LastName = ? AND Movies.ID = ANY("
							+ "SELECT Movies.ID AS movie_id "
							+ "FROM ((Person INNER JOIN Actor ON Person.ID = Actor.PersonId) "
							+ "INNER JOIN Cast ON Cast.PersonId = Actor.PersonId) "
							+ "INNER JOIN Movies ON Cast.MovieId = Movies.ID "
							+ "WHERE (Person.FirstName = ? AND Person.LastName = ?))) "
							+ "ORDER BY movie_title ASC;";

				
				//Create the PreparedStatement for the sql
				PreparedStatement stmt = connection.prepareStatement(sqlSameMovie);
				
				//Pass in the arguments 
				
				
				
				//Create the result set and print the customers which were served based on the employee number
				final ResultSet res = stmt.executeQuery();
				while(res.next()){
					System.out.println(res.getString("movie_title") + " " + res.getString("movie_id"));
				}
				
			} else if (qd.queryType == QueryTypes.TOP_MOVIE_IN_GENRE) {
				
				String sqlTopInGenre =
						"SELECT Movies.Title AS title, Genre.Name AS genre_name , "
							+ "ROUND((((Movies.RottenTomatoes + Movies.MetaCritic) / 10 + (Movies.IMBD)) / 3), 2) AS average_rating "
							+ "FROM (Movies INNER JOIN MovieGenre ON Movies.ID = MovieGenre.MovieId) "
							+ "INNER JOIN Genre ON Genre.ID = MovieGenre.GenreId "
							+ "WHERE (Genre.Name = ?) "
							+ "ORDER BY average_rating DESC LIMIT 2;";

				//Create the prepared statement for the sql
				PreparedStatement stmt = connection.prepareStatement(sqlTopInGenre);
				
				//Pass in the arguments
				
				
				
				//Create the result set in the format "id. firstname lastname (city, state, country)" and print it.
				final ResultSet res = stmt.executeQuery();
				while(res.next()){
					System.out.println(res.getString("title") + " " + res.getString("genre_name") + " " + res.getString("average_rating"));
				}
				
			} else if (qd.queryType == QueryTypes.WHO_WON_WHAT) {
				
				String sqlWhoWon =
						"SELECT Person.FirstName as 'First Name', Person.LastName as 'Last Name', "
							+ "Organization.Name as Organization, AwardNames.Name AS 'Award Won', Award.Year as Year "
							+ "FROM ((Organization INNER JOIN AwardNames ON Organization.ID = AwardNames.OrganizationId) "
							+ "INNER JOIN Award ON Award.OrganizationId = AwardNames.OrganizationId AND Award.AwardName = AwardNames.Name) "
							+ "INNER JOIN Person ON Person.ID = Award.PersonId "
							+ "WHERE( "
							+ "(SELECT Award.PersonId as person_id FROM Award "
							+ "GROUP BY person_id "
							+ "ORDER BY COUNT(Award.PersonId) DESC LIMIT 1) = Person.ID) "
							+ "ORDER BY Year DESC, 'Last Name' ASC;";
				
				//Create the prepared statements for the sql
				PreparedStatement stmt = connection.prepareStatement(sqlWhoWon);
				
				//Pass in the arguments
				
				
				
				//Create the result set and print it
				final ResultSet res = stmt.executeQuery();
			
				while(res.next()){
					System.out.println(res.getString("First Name") + " " + res.getString("Last Name") + " " + res.getString("Organization") 
						+ " " + res.getString("Award Won") + " " + res.getString("Year"));
				}
			}		
		}
	}

	/**
	 * Validates command-line arguments
	 *
	 * @param args command-line arguments
	 * @return query data, or null if invalid
	 * @throws ClassNotFoundException cannot find JDBC driver
	 */
	private static QueryData validateInputs(String[] args) throws ClassNotFoundException {
		// must have at least two arguments
		if (args.length < 1) {
			return _usage();
		}

		// attempt connecting to the database
		try{
			Class.forName(JDBC_DRIVER);
			mConnection = DriverManager.getConnection(url);
		} catch (Exception e) {
			System.out.printf("An error occurred when connecting to the database.\n");
			e.printStackTrace();
			return _usage();
		}
		// make sure second argument is a valid query number
		// and third is appropriate to query
		try {

			//The database connection is not made through the command line.
			final int queryNum = Integer.valueOf(args[0]);

			//The first query option
			if (queryNum == 1) {

				//This one should have not parameters besides the query number to be run
				if (args.length != 1) {
					return _usage();
				}

				else {
					return new QueryData(QueryTypes.ACTORS_AND_MORE, null);
				}
			}
			//the second query option
			else if (queryNum == 2) {
				if (args.length != 2) {
					return _usage();
				} else {
					return new QueryData(QueryTypes.REVENUE_BY_DIRECTOR, null);
				}
			}
			//the third query option
			else if (queryNum == 3) {
				if (args.length != 3) {
					return _usage();
				} else {
					Integer.valueOf(args[2]);
					return new QueryData(QueryTypes.SAME_MOVIE, args[2]);
				}
			}
			//the fourth query option
			else if (queryNum == 4) {
				if (args.length != 2) {
					return _usage();
				} else {
					return new QueryData(QueryTypes.TOP_MOVIE_IN_GENRE, null);
				}
			}
			//the fifth query option
			else if (queryNum == 5) {
				if (args.length != 3) {
					return _usage();
				} else {
					Integer.valueOf(args[2]);
					return new QueryData(QueryTypes.WHO_WON_WHAT, args[2]);
				}
			} else {
				return _usage();
			}

		} catch (NumberFormatException e) {
			return _usage();
		}
	}
}
