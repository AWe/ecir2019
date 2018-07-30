package kn.uni.inf.twistorino;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class EvaluationModule {

	/** List with contained evaluation events. */
	List<Evaent> events = new ArrayList<>();

	/**
	 * Main method.
	 * @param args arguments
	 */
	public static void main(String[] args) {
		try {
			new EvaluationModule();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Constructor.
	 * @param events file with events
	 * @param resultfolder folder with result files
	 * @throws Exception exception
	 */
	public EvaluationModule() throws Exception {
		this.readEventsFile();
		String fpath = "./results";
		//System.out.println("Type\tFScore\tLatency\tPerformance");
		File folder = new File(fpath);
		String[] resultfiles = folder.list();
		Arrays.sort(resultfiles, Collections.reverseOrder());
		int c = 1;
		for(String rfile : resultfiles) {
			// read file content
			if (rfile.startsWith(".")) continue;
			List<String> results = Files.readAllLines(Paths.get(fpath + "/" + rfile), Charset.defaultCharset());
			evaluateResults(rfile, this.events, results);
			//System.out.print("D" + c + "\t");
			c++;
		}
	}

	/**
	 * Evaluates the results.
	 * @param name of the technique
	 * @param events events to proof
	 * @param results results to proof
	 * @throws IOException I/O exception
	 */
	private void evaluateResults(final String name, final List<Evaent> events, final List<String> results) throws IOException {
		ArrayList<Evaent> cevents = new ArrayList<Evaent>();
		cevents.addAll(events);
		int slength = cevents.size();
		double perform = 0;
		double latency = 0;
		int timeadder = getTimeAdder(name);
		for (String res : results) {
			String[] resitems = res.split("\t");
			if (resitems.length < 2) {
				perform = calculatePerformance(Double.valueOf(res));
				continue;
			}
			String fevent = resitems[2];
			Evaent found = null;
			for (Evaent cevent : cevents) {
				if (cevent.eventMatch(fevent)) {
					found = cevent;
					break;
				}
			}
			if (found != null) {
				latency += found.latencyCheck(Helper.timeStringToSeconds2(resitems[1]) + timeadder);
				cevents.remove(found);
			}
		}
		double recall = (slength - cevents.size()) / Double.valueOf(slength);
		double precision = (slength - cevents.size()) / Double.valueOf(results.size() - 1);
		double fscore = 2 * ((precision * recall) / (precision + recall));
		double avglatency = latency / Double.valueOf((slength - cevents.size()));
		recall = Math.round(recall*1000)/1000.0;
		precision = Math.round(precision*1000)/1000.0;
		fscore = Math.round(fscore*1000)/1000.0;
		avglatency = Math.round(avglatency*1000)/1000.0;
		System.out.print(perform + "\t");
		//System.out.println(name + "\t" + fscore + "\t" + avglatency + "\t" + perform);
	}

	/**
	 * Returns performance value based on the hypotheses that 5000 tweets per seconds are enough to process
	 * per second.
	 * @param input performance value
	 * @return calculated performance value
	 */
	private double calculatePerformance(final double input) {
		// assumption that processing of 30,000 tweets per second leads to the best score
		double res = Math.round((input * 0.00003)*1000)/1000.0;
		return Math.min(1.0, res);
	}

	/**
	 * Returns the time that needs to be added to the event time cause of time output of Niagarino.
	 * @param name name of file
	 * @return time to add to
	 */
	private int getTimeAdder(final String name) {
		// shifty always reports events with a timestamp 4 minutes to early
		if (name.toLowerCase().contains("shifty")) {
			return 240;
		} else if (name.toLowerCase().contains("topn")){
			// the other topn technique reports events with a timestamp from the beginning of the timewindow
			// instead of the end
			int s = name.lastIndexOf("_") + 1;
			int e = name.lastIndexOf(".");
			return (int) (Integer.valueOf(name.substring(s, e)) / 1000.0);
		}
		return 0;
	}

	/**
	 * Reads the file with the event descriptions.
	 * @throws Exception exception
	 */
	public void readEventsFile() throws Exception {
		File file = new File("./events_10p.xml");
		DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory
		        .newInstance();
		DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
		Document document = documentBuilder.parse(file);
		NodeList nl = document.getElementsByTagName("event");
		if (nl != null) {
	        int length = nl.getLength();
	        for (int i = 0; i < length; i++) {
	            if (nl.item(i).getNodeType() == Node.ELEMENT_NODE) {
	                Element el = (Element) nl.item(i);
	                String[] lines = el.getTextContent().split("\n");
	                ArrayList<String> terms = new ArrayList<String>(Arrays.asList(lines[3].trim().split(", ")));
	                events.add(new Evaent(el.getAttribute("id"), terms, lines[2].trim()));
	            }
	        }
		}
	}

	/**
	 * Inner class for an evalatuion event.
	 */
	private class Evaent {

		private String name;
		private ArrayList<String> terms;
		private String startDate;

		/**
		 * Constructor.
		 * @param name name of event
		 * @param terms list of terms describing the event
		 * @param startDate date of event start
		 */
		private Evaent(final String name, final ArrayList<String> terms, final String startDate) {
			this.name = name;
			this.terms = terms;
			this.startDate = startDate;
		}

		/**
		 * Matches an event against another.
		 * @param needle term to match
		 * @return flag if needle is found in termlist
		 */
		private boolean eventMatch(final String needle) {
			return this.terms.contains(needle);
		}

		/**
		 * Checks latency of a detected event.
		 * @param foundtime time of event detection
		 * @return value of gap between detection and actual event start
		 */
		private double latencyCheck(final long foundtime) {
			// we assume that 60 minutes is the worst latency time
			double latency = ((foundtime - (Long.valueOf(startDate))) / 60.0) * 0.0166;
			return Math.max(0.0, latency);
		}
	}
}
