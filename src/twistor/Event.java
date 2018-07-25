/*
 * @(#)Event.java   1.0   Jul 24, 2018
 */
package twistor;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Class for the description of an event.
 * 
 * @author Harry Schilling &lt;harry.schilling@uni.kn&gt;
 * @version 1.0
 */
public class Event {
	public enum EventId {
		BORIS, BOSTON_BOMBING, CHARLOTTE, GERMANWINGS, MH17, NEPAL, PHILIP, POPE_ELECTION, ROBIN, WORLDCUP, SIMULATED
	}
	
	public class EventInputEntry {
		private long timestamp;
		private Double idf;
		
		protected EventInputEntry(long timestamp, Double idf) {
			this.timestamp = timestamp;
			this.idf = idf;
		}

		public Double getIdf() {
			return idf;
		}
		
		public String toString() {
			Date date = new Date();
			date.setTime(timestamp*1000);
			
			return date.toString() + " -> " + idf; 
		}
	}	
	
	private EventId id;
	protected long eventStart = 0;
	protected Map<String, List<EventInputEntry>> termEventInputEntries = new HashMap<>();
	private int startDelay = 0; // in seconds
	private List<String> eventTerms;
	
	public EventId getId() {
		return id;
	}
	
	public long getEventStart() {
		return eventStart;
	}
	
	public Map<String, List<EventInputEntry>> getTermEventInputEntries() {
		return termEventInputEntries;
	}

	/***
	 * Set the delay of the event start in seconds.
	 * @return
	 */
	public int getStartDelay() {
		return startDelay;
	}
	
	public void setStartDelayInSeconds(int value) {
		startDelay = value;
		eventStart += value;
	}
	
	public String toString() {
		return id.toString();
	}

	public Event(EventId id) {
		this.id = id;

		if (id != EventId.SIMULATED) {
			List<String> lines = Helper.getAllLines(new File(GlobalParameters.EVENT_DIR + "/" + id.toString().toLowerCase() + "/" + "data.txt"), 0);
			if (lines != null) {
				for (String line : lines) {
					String[] lineData = line.split("\t");
					
					String term = lineData[2];
					if (!termEventInputEntries.containsKey(term))
						termEventInputEntries.put(term, new ArrayList<>());
					
					// missing value
					Double idf = Double.valueOf(lineData[1]);
					if (Double.isNaN(idf) || Double.isInfinite(idf)) {
						idf = null;
					}
					
					long timestamp = Long.valueOf(lineData[0]);
					termEventInputEntries.get(term).add(new EventInputEntry(timestamp, idf));
				}
			}	
		}
	}
	
	public void setEventStart(String eventStart) {
		long secondsEventStart = Helper.timeStringToSeconds(eventStart);
		
		if (!termEventInputEntries.isEmpty()) {
			// transform official event start timestamp to twistor event start timestamp
			this.eventStart = Helper.timeStringToSeconds(GlobalParameters.DATE_START) + (secondsEventStart - getFirstTimestamp());
		}
		
		if (GlobalParameters.TRIM_EVENT) {
			long fromTimestamp = secondsEventStart - (GlobalParameters.MINUTES_BEFORE_EVENT_START*60);
			long toTimestamp = fromTimestamp + (GlobalParameters.EVENT_DURATION*60);
			filterIdfValues(Helper.timestampToString(fromTimestamp), Helper.timestampToString(toTimestamp));
			
			// event start also has to be adapted 
			this.eventStart = Helper.timeStringToSeconds(GlobalParameters.DATE_START) + (GlobalParameters.MINUTES_BEFORE_EVENT_START*60);
		}
	}
	
	private long getFirstTimestamp() {
		return termEventInputEntries.values().stream().findFirst().get().get(0).timestamp;
	}
	
	public void selectTerms(String... terms) {
		List<String> termsList = Helper.toList(terms);
		eventTerms = termsList;
		Iterator<Entry<String, List<EventInputEntry>>> iterator = termEventInputEntries.entrySet().iterator();
		
		while (iterator.hasNext()) {
			Entry<String, List<EventInputEntry>> entry = iterator.next();
			
			if (!termsList.contains(entry.getKey())) {
				iterator.remove();
			}
		}
	}
	
	public List<String> getEventTerms() {
		return this.eventTerms;
	}
	
	public void filterIdfValues(String from, String to) {
		long fromTimestamp = Helper.timeStringToSeconds(from);
		long toTimestamp = Helper.timeStringToSeconds(to);
		
		for (List<EventInputEntry> eventEntryList : termEventInputEntries.values()) {
			Iterator<EventInputEntry> eventEntryIterator = eventEntryList.iterator();
			
			while (eventEntryIterator.hasNext()) {
				EventInputEntry eventInput = eventEntryIterator.next();
				
				if (eventInput.timestamp <= fromTimestamp || eventInput.timestamp >= toTimestamp) {
					eventEntryIterator.remove();
				}
			}
		}
	}

	public void setStartDelayInMinutes(int minutes) {
		setStartDelayInSeconds(minutes*60);
	}
}
