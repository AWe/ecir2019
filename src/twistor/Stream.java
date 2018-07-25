/*
 * @(#)Stream.java   1.0   Jul 24, 2018
 */
package twistor;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.stream.Collectors;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.lang3.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Class for creating the artifical Twitter stream.
 * 
 * @author Harry Schilling &lt;harry.schilling@uni.kn&gt;
 * @version 1.0
 */
public class Stream {
	private Map<Integer, Integer> amountTermsDistribution = new TreeMap<>(Collections.reverseOrder());
	private Map<String, Integer> termDistribution = new HashMap<>();
	public static List<Event> events = new ArrayList<>();
	private Document eventsDocument;
	
	private List<String> tweets = new ArrayList<>();
	private List<Long> timestamps = new ArrayList<>();
	private Date date = new Date();
	private List<String> outputLines = new ArrayList<>();
	private int allTweetCount = 0;
	private int allIdfWindows = 0;
	private int window = 0;
	private int outputFileCount = 0;
	
	private ZipFile zip;
	private Date outputFileDate = new Date();
	private String outputFile = GlobalParameters.OUTPUT_DIR;
	private String outputFileWindowSize = "_" + GlobalParameters.WINDOW_SIZE + "min";
	private String outputFileScale = "_" + GlobalParameters.SCALE + "s";
	
	private String[] allTerms;
	private int countAllMinutes;
	private int fileWindow;
	private List<FileInput> fileInputs = new ArrayList<>();
	
	private class FileInput {
		private int start;
		private int end;
		private Map<String, int[]> termDistribution;
		private Map<Integer, int[]> amountTermsDistribution;
		private boolean loaded = false;
		private ZipArchiveEntry termDistributionZipEntry;
		private ZipArchiveEntry amountTermsDistributionZipEntry;
		
		private FileInput(int start, int end, ZipArchiveEntry termDistributionZipEntry, ZipArchiveEntry amountTermsDistributionZipEntry) {
			this.start = start;
			this.end = end;
			this.termDistributionZipEntry = termDistributionZipEntry;
			this.amountTermsDistributionZipEntry = amountTermsDistributionZipEntry;
		}
		
		public String toString() {
			return start + " -> " + end;
		}
		
		private void loadData() {
			if (!loaded) {
				termDistribution = fileToMap(false);
				amountTermsDistribution = fileToMap(true);
				
				loaded = true;
				System.out.println("loaded file input: " + start + " -> " + end);
			}
		}
		
		@SuppressWarnings("unchecked")
		private <T> void computeDistribution(Map<T, Integer> map, boolean keyIsInteger, int startIndex, int endIndex) {
			@SuppressWarnings("rawtypes")
			Set set = keyIsInteger ? amountTermsDistribution.entrySet() : termDistribution.entrySet();
			for (Entry<T, int[]> entry : (Set<Entry<T, int[]>>)set) {
				T key = (T)entry.getKey();
				int arraySum = arraySum(entry.getValue(), startIndex, endIndex);
				if (arraySum > 0) {
					if (GlobalParameters.SCALE != 10) {
						arraySum = (int)Helper.round((arraySum * GlobalParameters.SCALE) / (double)10, 0);
					}
					map.put(key, map.containsKey(key) ? map.get(key) + arraySum : arraySum);
				}
			}
		}
		
		private void unloadData() {
			if (loaded) {
				termDistribution.clear();
				amountTermsDistribution.clear();
				loaded = false;
				System.out.println("unloaded file input: " + start + " -> " + end);
			}
		}
		
		private int arraySum(int[] array, int start, int end) {
			int sum = 0;
			for (int i = start; i <= end; i++) {
				sum += array[i];
			}
			return sum;
		}	
		
		@SuppressWarnings("unchecked")
		private <T> Map<T, int[]> fileToMap(boolean keyIsInteger) {
			Map<T, int[]> map = new HashMap<>();

			try {
				InputStream inputStream = zip.getInputStream(keyIsInteger ? amountTermsDistributionZipEntry : termDistributionZipEntry);
				BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
				br.readLine(); // skip first line
				
				int i = 0;
				String line = br.readLine();
				while (line != null) {
					if (keyIsInteger) {
						String[] data = line.split("\t");
						T key = (T)Integer.valueOf(data[0]);					
						map.put(key, getValues(data[1]));
					}
					else {
						T key = (T)allTerms[i];					
						map.put(key, getValues(line));
					}

					line = br.readLine();
					i++;
				}
				br.close();
			} catch (Exception e) {
				System.out.println(e.getMessage());
			}

			return map;
		}			
		
		private int[] getValues(String str) {
			int[] intArray = new int[StringUtils.countMatches(str, ",") + 1];
			String number = null;
			int index = 0;
			char[] charArray = str.toCharArray();
			for (int i = 0; i < charArray.length; i++) {
				char c = charArray[i];
				if (c == ',') {
					if (number != null) {
						intArray[index] = Integer.valueOf(number);
					}
					number = null;
					index++;
				}
				else {
					if (number == null) {
						number = String.valueOf(c);
					}
					else {
						number += c;
					}
				}
			}
			if (number != null) {
				intArray[index] = Integer.valueOf(number);
			}
			return intArray;
		}	
	}
	
	public Stream(final String filename) {
		this.outputFile = filename;
		init();
	}
	
	private void init() {
    	TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
		System.out.println("Creating Twistor Stream...");
		
		// init output files
		outputFileDate.setTime(Helper.timeStringToMilliseconds(GlobalParameters.DATE_START));
		if (!GlobalParameters.SPLIT_IN_HOUR_FILES) {
			//outputFile = GlobalParameters.OUTPUT_DIR + "/" + getOutputFileDate(outputFileDate.getTime())
			//				+ outputFileWindowSize
			//				+ "_" + ((GlobalParameters.LIMIT_SIZE == -1) ? "24h" : GlobalParameters.LIMIT_SIZE + "min")
			//				+ outputFileScale + ".txt";
			
			if (new File(outputFile).exists()) {
				Helper.overwriteFile(outputFile);
			}
		}
		else {
			int limit = GlobalParameters.LIMIT_SIZE == -1 ? 23 
					: Helper.roundToInteger(((double)GlobalParameters.LIMIT_SIZE / 60) - 1, true);
			for (int i = 0; i <= limit; i++) {
				String fileName = getOutputFileDateSplitHour(i);
				
				if (new File(fileName).exists()) {
					Helper.overwriteFile(fileName);
				}				
			}
		}
		
    	initEvents();
    	initDistribution();
    	
    	date.setTime(Helper.timeStringToMilliseconds(GlobalParameters.DATE_START));
    	List<FileInput> currentFileInputs = null;
    	for (int i = 0; i < countAllMinutes; i += GlobalParameters.WINDOW_SIZE, window++) {
    		int windowEnd = i + GlobalParameters.WINDOW_SIZE - 1;
			if (windowEnd > countAllMinutes) {
				windowEnd = countAllMinutes - 1;
			}
			
			if (currentFileInputs == null) {
	    		currentFileInputs = getAndLoadFiles(i, windowEnd);
			}
			else {
				List<FileInput> lastFileInputs = currentFileInputs;
	    		currentFileInputs = getAndLoadFiles(i, windowEnd);
	    		for (FileInput fileInput : lastFileInputs) {
	    			if (!currentFileInputs.contains(fileInput)) {
	    				fileInput.unloadData();
	    			}
	    		}
			}
			System.out.println("create window: " + i + " -> " + windowEnd + " (file input: " + StringUtils.join(currentFileInputs, ", ") + ")");
			
    		int end, start, startIndex, endIndex;
    		for (FileInput fileInput : currentFileInputs) {
    			start = (fileInput.start < i) ? i : fileInput.start;
    			end = (fileInput.end > windowEnd) ? windowEnd : fileInput.end;
    			startIndex = start - fileInput.start;
    			endIndex = startIndex + (end - start);
    			
    			System.out.println("file input: " + start + " -> " + end + ", index: " + startIndex + "..." + endIndex);
    			fileInput.computeDistribution(termDistribution, false, startIndex, endIndex);
    			fileInput.computeDistribution(amountTermsDistribution, true, startIndex, endIndex);
    		}
        		
        	// create window
    		termDistribution = Helper.sortMapByValue(termDistribution);
        	createWindow();
    		
    		// reset data distribution
    		termDistribution.clear();
    		amountTermsDistribution.clear();
    		
    		// abort
    		if (GlobalParameters.LIMIT_SIZE > -1 && i > 0 && GlobalParameters.LIMIT_SIZE / (double)(i + GlobalParameters.WINDOW_SIZE) <= 1) {
    			break;
    		}    		
        }
    	
		try {
			zip.close();
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
    	
    	System.out.println("done");
	}
	
	private String getOutputFileDate(long milliseconds) {
		DateFormat simpleDateFormat = new SimpleDateFormat("yyyy_MM_dd_HH");
		return simpleDateFormat.format(milliseconds).toString();
	}	
	
	private String getOutputFileDateSplitHour(int hour) {
		return getOutputFileDate(outputFileDate.getTime() + hour * 60 * 60 * 1000) 
				+ outputFileWindowSize 
				+ outputFileScale
				+ ".txt";
	}
	
	private List<FileInput> getAndLoadFiles(int start, int end) {
		List<FileInput> currentFileInputs = fileInputs.stream()
				.filter(input -> (
						input.start >= start && input.end <= end) // files in range
						|| (input.start <= end && input.end > end) // transition from lower to higher window
						|| (input.start < start && input.end >= start)) // transition form higher to lower window
				.collect(Collectors.toList());
		currentFileInputs.forEach(input -> input.loadData());
		return currentFileInputs;
	}	
	
	private void initDistribution() {
		List<ZipArchiveEntry> entriesList = new ArrayList<>();
		try {
			zip = new ZipFile(GlobalParameters.TWISTOR_BASE);
			Enumeration<ZipArchiveEntry> entries = zip.getEntries();
			
			while (entries.hasMoreElements()) {
				entriesList.add(entries.nextElement());
			}
			
			// info
			ZipArchiveEntry infoFile = entriesList.stream().filter(e -> e.getName().equals("info.txt")).findAny().get();
			InputStream inputStream = zip.getInputStream(infoFile);
			BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
			br.readLine(); // skip first line
			String[] data = br.readLine().split("\t");
			fileWindow = Integer.valueOf(data[0]);
			countAllMinutes = Integer.valueOf(data[1]);
			allTerms = new String[Integer.valueOf(data[2])];
			br.close();
			
			// terms
			ZipArchiveEntry termFile = entriesList.stream().filter(e -> e.getName().equals("terms.txt")).findAny().get();
			inputStream = zip.getInputStream(termFile);
			br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
			br.readLine(); // skip first line
			
			int j = 0;
			String line = br.readLine();
			while (line != null) {
				allTerms[j] = line;
				line = br.readLine();
				j++;
			}
			br.close();
			
			// amount terms in tweets / term counts 
			List<ZipArchiveEntry> termCountFiles = entriesList.stream().filter(e -> e.getName().startsWith("term_counts_")).collect(Collectors.toList());
			int start = 0;
			for (int i = 0; i < termCountFiles.size(); i++) {
				int end = start + fileWindow;
				if (i == termCountFiles.size() - 1) { // last file
					end = countAllMinutes;
				}
				addFileInput(start, end - 1, entriesList);
				start = end;
			}
		} catch (IOException e) {
			System.out.println(e.getMessage());
		}
	}
	
	private void addFileInput(int start, int end, List<ZipArchiveEntry> entriesList) {
		ZipArchiveEntry termCounts = entriesList.stream().filter(e -> e.getName().equals("term_counts_" + start + "-" + end + ".txt")).findAny().get();
		ZipArchiveEntry amountTermsInTweets = entriesList.stream().filter(e -> e.getName().equals("amount_terms_in_tweets_" + start + "-" + end + ".txt")).findAny().get();
		fileInputs.add(new FileInput(start, end, termCounts, amountTermsInTweets));
	}
	
	private void initEvents() {
		// boris
		Event boris = new Event(Event.EventId.BORIS);
		boris.setEventStart("02/27/2015 21:24:00");
		boris.selectTerms("boris", "nemtsov", "nemzow");
		
		// boston bombing
		Event bostonBombing = new Event(Event.EventId.BOSTON_BOMBING);
		bostonBombing.setEventStart("04/15/2013 18:50:00");
		bostonBombing.selectTerms("boston", "marathon", "explosion");
		
		// charlotte
		Event charlotte = new Event(Event.EventId.CHARLOTTE);
		charlotte.setEventStart("05/02/2015 10:09:00");
		charlotte.selectTerms("royal", "princess", "cambridge");
		
		// germanwings
		Event germanwings = new Event(Event.EventId.GERMANWINGS);
		germanwings.setEventStart("03/24/2015 10:29:00");
		germanwings.selectTerms("airbus", "plane", "crash");
		
		// mh17
		Event mh17 = new Event(Event.EventId.MH17);
		mh17.setEventStart("07/17/2014 15:08:00");
		mh17.selectTerms("malaysian", "airlines", "crashed");
		
		// nepal
		Event nepal = new Event(Event.EventId.NEPAL);
		nepal.setEventStart("04/25/2015 06:11:00");
		nepal.selectTerms("nepal", "earthquake", "quake");		

		// philip
		Event philip = new Event(Event.EventId.PHILIP);
		philip.setEventStart("02/02/2014 18:22:00");
		philip.selectTerms("philip", "hoffman", "deadphilip");
		
		// pope election
		Event popeElection = new Event(Event.EventId.POPE_ELECTION);
		popeElection.setEventStart("03/13/2013 18:06:00");
		popeElection.selectTerms("habemus", "papam", "fumata");		
		
		// robin
		Event robin = new Event(Event.EventId.ROBIN);
		robin.setEventStart("08/11/2014 22:51:00");
		robin.selectTerms("robin", "williams", "deadrobin");	
		
		// worldcup
		Event worldcup = new Event(Event.EventId.WORLDCUP);
		worldcup.setEventStart("07/13/2014 21:24:00");
		worldcup.selectTerms("mario", "goetze", "scored");	
		
		// simulated event example
		EventSimulated simulated = new EventSimulated(Event.EventId.SIMULATED);
		simulated.addTerm("test1", 4, 10, 1, 1);
		simulated.addTerm("test2", 5, 10, 4, 2);
		simulated.setStartDelayInSeconds(60);
		
		if (!GlobalParameters.AUTOMATIC_EVENT_INJECTION) {
			// add events manually to stream
			// add any event you want have in the simulated stream
			events.add(bostonBombing);
			events.add(charlotte);
			events.add(boris);
			events.add(germanwings);
			events.add(mh17);
			events.add(nepal);
			events.add(philip);
			events.add(popeElection);
			events.add(robin);
			events.add(worldcup);
			bostonBombing.setStartDelayInMinutes(2);
			charlotte.setStartDelayInMinutes(2);
			boris.setStartDelayInMinutes(2);
			germanwings.setStartDelayInMinutes(2);
			mh17.setStartDelayInMinutes(2);
			nepal.setStartDelayInMinutes(2);
			philip.setStartDelayInMinutes(2);
			popeElection.setStartDelayInMinutes(2);
			worldcup.setStartDelayInMinutes(2);
			robin.setStartDelayInMinutes(2);
		}
		else {
			// add events automatically
			
			events.add(bostonBombing);
			events.add(charlotte);
			events.add(boris);
			events.add(germanwings);
			events.add(mh17);
			events.add(nepal);
			events.add(philip);
			events.add(popeElection);
			events.add(robin);
			events.add(worldcup);
			
			Element eventsXmlFile = getEventsXmlFile();
			int amountEvents = GlobalParameters.AMOUNT_EVENTS;
			int minDistanceMinutes = GlobalParameters.MIN_DISTANCE_BETWEEN_EVENTS;
			
			int minStreamDuration = GlobalParameters.EVENT_DURATION + (amountEvents * minDistanceMinutes);
			int toleranceMinutes = GlobalParameters.LIMIT_SIZE - minStreamDuration;
			
			if (toleranceMinutes >= 0) {
				events = Helper.getRandomElements(events, amountEvents);
				
				int delay = minDistanceMinutes;
				for (int i = 0; i < events.size(); i++) {
					if (toleranceMinutes > 0) {
						Event event = events.get(i);
						int random = Helper.getRandom(0, toleranceMinutes);
						delay += random;
						
						if (i == 0) {
							event.setStartDelayInSeconds(delay*60);
						}
						else {
							long preEventStart = events.get(i-1).getEventStart();
							long eventStart = event.getEventStart();
							
							int eventDistance = (int)(preEventStart - eventStart);
							event.setStartDelayInSeconds(eventDistance + (delay*60));
						}
						addEventToXmlFile(event, eventsXmlFile, event.getId(), delay, event.getEventStart());
		
						toleranceMinutes -= random;
						delay = minDistanceMinutes;
					}
				}
				
				writeEventXmlFile();
			}
			else {
				System.out.println("Amount of events (" + amountEvents + ") with min distance between events (" + minDistanceMinutes + ")"
						+ " and event duration (" + GlobalParameters.EVENT_DURATION + ")." + System.lineSeparator() 
						+ "(" + amountEvents + " * " + minDistanceMinutes + ") + " + GlobalParameters.EVENT_DURATION + " = " + minStreamDuration
						+ " must be greater or equal than overall minutes (" + GlobalParameters.LIMIT_SIZE + ") or the events can not be included.");
				System.exit(0);
			}			
		}
	}
	
	private Element getEventsXmlFile() {
		try {			
			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();
			
			// root
			eventsDocument = docBuilder.newDocument();
			Element root = eventsDocument.createElement("event_output");
			eventsDocument.appendChild(root);
			
			// event option
			Element eventOption = eventsDocument.createElement("event_option");
			root.appendChild(eventOption);
			
			// minutes before option
			Element minutesBefore = eventsDocument.createElement("minutes_before_event_start");
			minutesBefore.appendChild(eventsDocument.createTextNode(String.valueOf(GlobalParameters.MINUTES_BEFORE_EVENT_START)));
			eventOption.appendChild(minutesBefore);
			
			// event duration option
			Element eventDuration = eventsDocument.createElement("event_duration");
			eventDuration.appendChild(eventsDocument.createTextNode(String.valueOf(GlobalParameters.EVENT_DURATION)));
			eventOption.appendChild(eventDuration);
			
			// events
			Element eventsNode = eventsDocument.createElement("events");
			root.appendChild(eventsNode);
			
			return eventsNode;
		}
		catch (Exception e) {
			System.out.println(e.toString());
			return null;
		}
	}
	
	private void addEventToXmlFile(Event evento, Element events, Event.EventId id, int eventDelay, long eventStart) {
		Element event = eventsDocument.createElement("event");
		events.appendChild(event);
		event.setAttribute("id", id.toString());
		
		// delay
		Element delay = eventsDocument.createElement("delay_to_pre_event");
		delay.appendChild(eventsDocument.createTextNode(String.valueOf(eventDelay)));
		event.appendChild(delay);
		
		// start timestamp
		Element startTimestamp = eventsDocument.createElement("start_epoch");
		startTimestamp.appendChild(eventsDocument.createTextNode(String.valueOf(eventStart)));
		event.appendChild(startTimestamp);
		
		Element terms = eventsDocument.createElement("terms");
		terms.appendChild(eventsDocument.createTextNode(String.join(", ", evento.getEventTerms())));
		event.appendChild(terms);
		
		// start string
		Element startString = eventsDocument.createElement("start_string");
		Date date = new Date();
		date.setTime(eventStart*1000);
		startString.appendChild(eventsDocument.createTextNode(date.toString()));
		event.appendChild(startString);
	}
	
	private void writeEventXmlFile() {
		try {
			// write the content into xml file
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
			
			DOMSource source = new DOMSource(eventsDocument);
			StreamResult result = new StreamResult(new File(GlobalParameters.OUTPUT_DIR + "/events_" + GlobalParameters.SCALE + ".xml"));
			transformer.transform(source, result);
		}
		catch (Exception e) {
			System.out.println(e.toString());
		}
	}
	
	private void createWindow() {
    	if (window == 0) {
    		outputLines.add(GlobalParameters.TWITTER_HEADER);
    	}
    	int tweetCount = getTweetCount(amountTermsDistribution);
    	
    	if (GlobalParameters.INSERT_COMMENTS) {
    		outputLines.add("# window " + window + ", tweet count: " + tweetCount);
    	}
    	
		if (tweetCount > 0) {
			tweets.clear();
			
			// create timestamps for window
	    	int windowSizeInSeconds = GlobalParameters.WINDOW_SIZE*60;
	    	double secondPerTweet = (double)windowSizeInSeconds / tweetCount;
	    	
	    	Double adaptIncrease = null;
	    	if (secondPerTweet < 1) {
	    		// how many tweets should be in one second
	    		adaptIncrease = 1 / secondPerTweet;
	    	}
	    	
			if (timestamps.size() > 0) {
				long lastTimestamp = timestamps.get(timestamps.size() - 1);
				
				// replace the last three digits of the timestamp with 000
				// the last three digits is millisecond precision which is not needed here 
				// and will led to problems with the actual timestamps in the tweets
				lastTimestamp = lastTimestamp - (lastTimestamp%1000); 
				date.setTime(lastTimestamp + 1000);
			}
	    	long endTimestamp = date.getTime() + windowSizeInSeconds*1000;
	    	timestamps.clear();
	    	
			for (int i = 0; i < tweetCount; i++) {
				if (adaptIncrease == null) {						
					Helper.increaseDate(date, Calendar.SECOND, Helper.roundToInteger(secondPerTweet, true));
				}
				else {
					int integerIncrease = Helper.roundToInteger(adaptIncrease, false);
					if (i > 0 && i%integerIncrease == 0) {
						Helper.increaseDate(date, Calendar.SECOND, 1);
					}
				}
				
				long currentTimestamp = date.getTime();
				if (currentTimestamp >= endTimestamp) {
					date.setTime(endTimestamp - 1000);
				}
				
				timestamps.add(date.getTime());
			}			
			
			System.out.println("window " + window + ", tweets to create: " + tweetCount);
			
	    	for (Entry<Integer, Integer> amountTermsEntry : amountTermsDistribution.entrySet()) {
	    		int termAmount = amountTermsEntry.getKey();
	    		int absoluteCountInTweets = amountTermsEntry.getValue();
	    		
	    		for (int i = 0; i < absoluteCountInTweets; i++) {
	    			List<String> terms = new ArrayList<>();
	    			Iterator<Entry<String, Integer>> termIterator = termDistribution.entrySet().iterator();
	    			
	    			for (int j = 0; j < termAmount; j++) {
						if (termIterator.hasNext()) {
							Entry<String, Integer> entryTerm = termIterator.next();
							String term = entryTerm.getKey();
							int count = entryTerm.getValue();
							terms.add(term);
							
							if (count == 1) {
								termIterator.remove();
							}
							else {
								termDistribution.put(term, count - 1);
							}
						}
	    			}
				
					tweets.add(StringUtils.join(terms, " "));
	    		}
	    	}	
	
			Collections.shuffle(tweets);
			insertEvents();
			writeResult();
		}
	}
	
	private void insertEvents() {
		// how many tweets in 1 idf window
		Map<Integer, Integer> tweetAmountPerIdfWindow = new HashMap<>();
		int tweetCount = 0;
		int idfWindowCount = 0;
		long startTimestamp = timestamps.get(0) / 1000;
		for (int i = 0; i < timestamps.size(); i++) {
			long timestamp = timestamps.get(i) / 1000;
			long duration = (timestamp - startTimestamp);
			if (duration < GlobalParameters.IDF_WINDOW) {
				tweetCount++;
			}
			else {
				tweetAmountPerIdfWindow.put(idfWindowCount++, tweetCount);
				tweetCount = 1;
    			startTimestamp = timestamp;
			}
		}
		tweetAmountPerIdfWindow.put(idfWindowCount, tweetCount);
		
		int tweetAmountInIdfWindow = tweetAmountPerIdfWindow.get(0);
		Map<String, Integer> amountTermsFraction = new HashMap<>();  
		
		idfWindowCount = 0;
		startTimestamp = timestamps.get(0);
		Date date = new Date(); 
		for (int i = 0; i < tweets.size(); i++) {
			String tweet = tweets.get(i);
			long timestamp = timestamps.get(i);
			
			date.setTime(timestamp);
			long duration = ((timestamp / 1000) - (startTimestamp / 1000));
			if (duration < GlobalParameters.IDF_WINDOW) {
				if (i == 0) { // first tweet which introduces the new window
					initIdfWindow(amountTermsFraction, tweetAmountInIdfWindow, idfWindowCount);
        			idfWindowCount++;
					allIdfWindows++;
				}
				
				tweet = addTermsToTweet(amountTermsFraction, i, tweet);
				outputLines.add(String.format("%018d", allTweetCount++) + "\t" + date + "\t" + tweet);
			}
			else {
				tweetAmountInIdfWindow = tweetAmountPerIdfWindow.get(idfWindowCount);
				
				initIdfWindow(amountTermsFraction, tweetAmountInIdfWindow, idfWindowCount);					
    			idfWindowCount++;
    			allIdfWindows++;
    			
				tweet = addTermsToTweet(amountTermsFraction, i, tweet);
				outputLines.add(String.format("%018d", allTweetCount++) + "\t" + date + "\t" + tweet);
    			startTimestamp = timestamps.get(i);
			}
		}		
	}
	
	private void initIdfWindow(Map<String, Integer> amountTermsFraction, int tweetAmountInMinuteWindow, int idfWindowCount) {
		List<String> eventsOutput = new ArrayList<>();
		for (Event eventInput : events) {
			if (eventInput.getStartDelay() <= allIdfWindows) {
				// used for logging
				List<String> termsWithCount = new ArrayList<>();
				
				for (Entry<String, List<Event.EventInputEntry>> entry : eventInput.getTermEventInputEntries().entrySet()) {
					List<Event.EventInputEntry> eventEntries = entry.getValue();
					
					if ((allIdfWindows - eventInput.getStartDelay()) <= eventEntries.size() - 1) {
						String term = entry.getKey();
						Double idf = entry.getValue().get(allIdfWindows - eventInput.getStartDelay()).getIdf();
						
						if (idf != null) {
							int amountTerm = getTermAmountInWindow(tweetAmountInMinuteWindow, idf);
							amountTermsFraction.put(term, (int)Math.ceil((double)tweetAmountInMinuteWindow / amountTerm));
							termsWithCount.add(term + " -> " + amountTerm + " -> idf to reach: " + idf + ", idf reached: " + Math.log((double)tweetAmountInMinuteWindow / amountTerm));
						}
						else {
							termsWithCount.add(term + " -> 0");
							amountTermsFraction.remove(term);
						}
					}
				}
				
				if (termsWithCount.size() > 0) {
					for (String termWithCount : termsWithCount) {
						eventsOutput.add("# " + eventInput.getId() + ": " + termWithCount);
					}
				}
			}
		}
		
		if (GlobalParameters.INSERT_COMMENTS) {
			outputLines.add("# start " + GlobalParameters.IDF_WINDOW + " seconds idf window " + idfWindowCount + " (" + allIdfWindows 
					+ "), amount tweets: " + tweetAmountInMinuteWindow);
		
			for (String eventOut : eventsOutput) {
				outputLines.add(eventOut);
			}
		}
	}	
	
	private int getTermAmountInWindow(int tweetAmountInMinuteWindow, double idf) {
		int ceil = (int)Math.ceil(tweetAmountInMinuteWindow / Math.pow(Math.E, idf));
		int floor = (int)Math.floor(tweetAmountInMinuteWindow / Math.pow(Math.E, idf));
		
		if (floor > 0) {
			double absCeil = Math.abs(Math.log((double)tweetAmountInMinuteWindow / ceil) - idf);
			double absFloor = Math.abs(Math.log((double)tweetAmountInMinuteWindow / floor) - idf);
			
			return absCeil < absFloor ? ceil : floor;
		}
		
		return ceil;
	}

	private String addTermsToTweet(Map<String, Integer> amountTermsFraction, int i, String tweet) {
		for (Entry<String, Integer> entry : amountTermsFraction.entrySet()) {
			int tweetAmountInWindowFraction = entry.getValue();
			String term = entry.getKey();
			
			if (i%tweetAmountInWindowFraction == 0) {
				tweet += " " + term;
			}
		}
		return tweet;
	}
	
	private void writeResult() {
		try {
			if (GlobalParameters.SPLIT_IN_HOUR_FILES) {
				// last file will not be written if 60 is not straight divisible by WINDOW_SIZE, so this workaround
				int lastWindow = Helper.roundToInteger(((double)GlobalParameters.LIMIT_SIZE / GlobalParameters.WINDOW_SIZE), true) - 1;
				if (((window + 1) * GlobalParameters.WINDOW_SIZE) % 60 == 0 || window == lastWindow) {
					String fileName = GlobalParameters.OUTPUT_DIR + "/" + getOutputFileDateSplitHour(outputFileCount++);
					Files.write(Paths.get(fileName), outputLines, Charset.forName("UTF-8"));
					
					outputLines.clear();
					// outputLines.add(GlobalParameters.TWITTER_HEADER);
				}
			} else {
				File file = new File(outputFile);
				if (file.exists()) {
					Files.write(Paths.get(outputFile), outputLines, Charset.forName("UTF-8"),
							StandardOpenOption.APPEND);
				} else {
					Files.write(Paths.get(outputFile), outputLines, Charset.forName("UTF-8"));
				}
				outputLines.clear();
			}
		} catch (Exception e) {
			System.out.println(e);
		}
	}
	
	private int getTweetCount(Map<Integer, Integer> map) {
		int sum = 0;
		for (Integer value : map.values()) {
			sum += value;
		}
		return sum;
	}
}
