/*
 * @(#)GlobalParameters.java   1.0   Jul 24, 2018
 */
package twistor;

/**
 * Class for all parameters defining the artificial Twitter stream.
 * 
 * @author Harry Schilling &lt;harry.schilling@uni.kn&gt;
 * @version 1.0
 */
public class GlobalParameters {
	///////////
	// INPUT //
	///////////
	
	// the date the simulated stream begins with
	public static String DATE_START = "01/01/2000 00:00:00";
	
	// window size of the simulated stream in minutes
	// min = 1, max = 60
	// standard = 1
	public static int WINDOW_SIZE = 1;
	
	// length of the simulated stream in minutes
	// -1 will create 24h
	// this value should be divisible by WINDOW_SIZE or more than LIMIT_SIZE minutes will be created
	public static int LIMIT_SIZE = 60;
	
	// scale of the simulated stream in percent
	// standard = 10 (gardenhose)
	// the higher the value the more tweets will be created
	public static int SCALE = 100;
	
	// path to the file which contains the information to create the simulated stream
	public static String TWISTOR_BASE = "./data/stream.zip";

	
	////////////
	// OUTPUT //
	////////////
	
	// header of the created file(s) with the simulated twitter stream
	public static String TWITTER_HEADER = "TWEET_ID\tTWEET_CREATIONDATE\tTWEET_CONTENT";
	
	// split the simulated twitter stream in hourly files
	// if true 60 must be divisible by WINDOW_SIZE or this will not work correctly
	public static boolean SPLIT_IN_HOUR_FILES = false; 
	
	// insert comments into the simulated stream
	// mostly for debug reasons
	// every row with a comment starts with '#'
	public static boolean INSERT_COMMENTS = false;
	
	// the output directory for the file(s) with the simulated twitter stream
	public static String OUTPUT_DIR = "./data/output";

	
	/////////////////////
	// EVENT INJECTION //
	/////////////////////
	
	// the sampling rate of the idf values from the events in seconds
	// standard = 1
	public static int IDF_WINDOW = 1;
	
	// all events will be standardized in case of the length (EVENT_DURATION) and the event start (MINUTES_BEFORE_EVENT_START)
	public static boolean TRIM_EVENT = true;
	
	// minutes before the drop of the idf values of the events
	// max = 15
	public static int MINUTES_BEFORE_EVENT_START = 2;
	
	// maximal length of a event in minutes
	// max = 180
	public static int EVENT_DURATION = 20;
	
	// events can be inserted manually by adding them in initEvents() in Stream.java
	// or automatically (randomly)
	// if you choose automatically a file (events.xml) will be created in the OUTPUT_DIR which contains the information about the inserted events
	public static boolean AUTOMATIC_EVENT_INJECTION = true;
	
	// amount of the automatically added events
	// max = 10
	public static int AMOUNT_EVENTS = 10;
	
	// minimum distance between automatically added events in minutes
	// here the distance between the drop of the idf values is meant
	public static int MIN_DISTANCE_BETWEEN_EVENTS = 3;
	
	// the directory which contains the information of the events
	public static String EVENT_DIR = "./data/events";
}
