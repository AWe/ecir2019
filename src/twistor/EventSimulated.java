/*
 * @(#)EventSimulated.java   1.0   Jul 24, 2018
 */
package twistor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * Class for the description of a simulated event.
 * 
 * @author Harry Schilling &lt;harry.schilling@uni.kn&gt;
 * @version 1.0
 */
public class EventSimulated extends Event {
	private double minIdf = 2.0;
	private double maxIdf = 6.0;
	
	private int lastAmount = 10;
	private int[] factors = new int[] { 5, 5, 5, 18, 17 };
	
	public EventSimulated(EventId id) {
		super(id);
		this.eventStart = Helper.timeStringToSeconds(GlobalParameters.DATE_START) + GlobalParameters.MINUTES_BEFORE_EVENT_START * 60;
	}
	
	/***
	 * Adds a term for the simulated event.
	 * @param term
	 * @param eventIdfStartInput Possible 1-10. The greater the higher the start.
	 * @param eventDropIdfInput Possible 1-10. The greater the bigger the drop.
	 * @param eventStartingDuration Minutes the event has taken place. Increasing this number will decrease the slope of the event. Recommended 1 for significant event else is subjective. The Greater the value the lower the slope.
	 * @param eventEndFactorInput The strength of the increase after the event has happened. Recommended: 1-10. The Greater the bigger the increase.
	 */
	public void addTerm(String term, int eventStartIdfInput, int eventDropIdfInput, int eventStartingDuration, int eventEndFactorInput) {
		if (eventStartIdfInput < 1 || eventStartIdfInput > 10) {
			System.out.println("the event start input for a simulated event should be between 1 and 10");
			System.exit(1);
		}
		if (eventDropIdfInput < 1 || eventDropIdfInput > 10) {
			System.out.println("the event drop input for a simulated event should be between 1 and 10");
			System.exit(1);
		}
		if (eventEndFactorInput < 1 || eventEndFactorInput > 10) {
			System.out.println("the end event increase factor for a simulated event should be between 1 and 10");
			System.exit(1);
		}		
		
		double eventIdfStart = 0;
		if (eventStartIdfInput == 1) {
			eventIdfStart = minIdf;
		}
		else if (eventStartIdfInput == 10) {
			eventIdfStart = maxIdf;
		}
		else {
			eventIdfStart = 2 + (0.44 * (eventStartIdfInput - 1));
		}
		
		double diff = eventIdfStart - minIdf;
		double eventDropIdf = eventIdfStart - ((eventDropIdfInput * diff) / 10);
		
		int eventDurationLeft = GlobalParameters.EVENT_DURATION;
		List<EventInputEntry> entryList = new ArrayList<>();
		termEventInputEntries.put(term, entryList);
		
		// before event
		long start = Helper.timeStringToSeconds(GlobalParameters.DATE_START);
		int duration = GlobalParameters.MINUTES_BEFORE_EVENT_START;
		setBeforeEventStart(entryList, eventIdfStart, start, duration);
		eventDurationLeft -= duration;
		
		// event starting
		double eventStartStep = (eventIdfStart - eventDropIdf) / eventStartingDuration;
		eventDropIdf = eventIdfStart - eventStartStep;
		start += duration - 1;
		addEntriesIdf(entryList, eventDropIdf, eventStartStep, start, false);
		
		for (int i = 0; i < eventStartingDuration - 1; i++) {
			eventDropIdf -= eventStartStep;
			start += 60;
			addEntriesIdf(entryList, eventDropIdf, eventStartStep, start, false);
		}
		eventDurationLeft -= eventStartingDuration;
		
		// event ending
		double eventEndFactor = (double)eventEndFactorInput / 100;
		for (int i = 0; i < eventDurationLeft; i++) {
			eventDropIdf += eventEndFactor;
			start += 60;
			addEntriesIdf(entryList, eventDropIdf, eventEndFactor, start, true);
		}
	}
	
	private void setBeforeEventStart(List<EventInputEntry> entryList, double eventIdfStart, long start, int duration) {
		for (int i = 0; i < duration; i++) {
			if (i > 0) {
				start += 60;
			}
			addEntriesIdf(entryList, eventIdfStart, 0, start, true);
		}
	}
	
	private void addEntriesIdf(List<EventInputEntry> entryList, double idfValue, double step, long start, boolean up) {
		List<Double> idfValues = new ArrayList<>();
		
		if (step == 0) {
			for (int i = 0; i < 60; i++) {
				idfValues.add(idfValue);
			}
		}
		else {
			double stepping = step / 6;
			double startFactor = idfValue - (step / 2);
	
			Map<Double, Integer> map = up ? new TreeMap<>() : new TreeMap<>(Collections.reverseOrder());
			double factorSum = 0;
			for (int i = 0; i < factors.length; i++) {
				map.put(startFactor, factors[i]);
				factorSum += factors[i]*startFactor;
				startFactor += stepping;
			}
			
			double lastValue = ((idfValue*60) - factorSum) / lastAmount;
			map.put(lastValue, map.containsKey(lastValue) ? map.get(lastValue) + lastAmount : lastAmount);
	
			
			for (Entry<Double, Integer> entry : map.entrySet()) {
				for (int i = 0; i < entry.getValue(); i++) {
					idfValues.add(entry.getKey());
				}
			}
		}
		
		addEntriesEvenForMinute(entryList, start, 60, idfValues);
	}
	
	/**
	 * Distributes the IDF values in valueList uniformly by the minute for every minute from startTimestamp to end (duration). 
	 * @param startTimestamp
	 * @param duration
	 * @param idfValues
	 * @return
	 */
	private List<EventInputEntry> addEntriesEvenForMinute(List<EventInputEntry> entryList, long startTimestamp, int duration, List<Double> idfValues) {
		int amountEntriesInMinute = idfValues.size();
		if (amountEntriesInMinute == 60) {
			for (int i = 0; i < duration; i++) {
				entryList.add(new EventInputEntry(startTimestamp++, idfValues.get(i)));
			}
		}
		else {
			int fraction = Helper.roundToInteger(60 / (double)amountEntriesInMinute, true);
			int missedEntries = amountEntriesInMinute - (60 / fraction);
			
			int valuesAdded = 0;
			for (int i = 0; i < duration; i++) {
				int minuteDistribution = i%60;
				
				// reset missed entries after 1 minute
				if (minuteDistribution == 0) {
					missedEntries = amountEntriesInMinute - (60 / fraction);
					valuesAdded = 0;
				}
				
				if (minuteDistribution > 0 && minuteDistribution%fraction == (fraction - 1)) {
					entryList.add(new EventInputEntry(startTimestamp, idfValues.get(valuesAdded++)));
				}
				else if (missedEntries > 0) {
					entryList.add(new EventInputEntry(startTimestamp, idfValues.get(valuesAdded++)));
					missedEntries--;
				}
				else {
					entryList.add(new EventInputEntry(startTimestamp, null));
				}
				startTimestamp++;
			}
		}
		return entryList;
	}
}
