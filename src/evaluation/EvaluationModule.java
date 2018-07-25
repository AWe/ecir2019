/*
 * @(#)EvaluationModule.java   1.0   Jul 24, 2018
 */
package evaluation;

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

import twistor.Helper;

/**
 * Class for running the evaluation of event detection results.
 *
 * @author Andreas Weiler &lt;wele@zhaw.ch&gt;
 * @version 1.0
 */
public class EvaluationModule {

	/** List with contained evaluation events. */
	List<Evaent> events = new ArrayList<>();

	/**
	 * Constructor.
	 * @param events file with events
	 * @param eventsFile file with events description
	 * @param resultspath folder with result files
	 * @throws Exception exception
	 */
	public EvaluationModule(final String eventsFile, final String resultspath) throws Exception {
		this.readEventsFile(eventsFile);
		String fpath = resultspath;
		System.out.println("Type\tPrecision\tRecall\tFScore\tAVG(Latency)\tPerformance");
		File folder = new File(fpath);
		String[] resultfiles = folder.list();
		Arrays.sort(resultfiles, Collections.reverseOrder());
		for(String rfile : resultfiles) {
			// read file content
			if (rfile.startsWith(".")) continue;
			List<String> results = Files.readAllLines(Paths.get(fpath + "/" + rfile), Charset.defaultCharset());
			evaluateResults(rfile, this.events, results);
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
		long latency = 0;
		for (String res : results) {
			String[] resitems = res.split("\t");
			if (resitems.length < 2) {
				perform = Double.valueOf(res);
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
				latency += found.latencyCheck(Helper.timeStringToSeconds2(resitems[1]));
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
		System.out.println(name + "\t" + precision + "\t" + recall + "\t" + fscore + "\t" + avglatency + "\t" + perform);
	}

	/**
	 * Reads the file with the event descriptions.
	 * @throws Exception exception
 	*/
	public void readEventsFile(final String eventsFile) throws Exception {
		File file = new File(eventsFile);
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
 * Returns the time that needs to be added to the results cause of wrong time output of Niagarino.
 * Niagarino always prints the timestamp of the first seen tuple in the window.
 * Note: The filenames need to contain the miliseconds of type or the name Shifty.
 * @param name name of file
 * @return time to add to
 */
private int getTimeAdder(final String name) {
	if (name.contains("Shifty")) {
		return 240;
	} else {
		int s = name.lastIndexOf("_") + 1;
		int e = name.lastIndexOf(".");
		return (int) (Integer.valueOf(name.substring(s, e)) / 1000.0);
	}
}

	/**
	* Inner class for an evaluation event instance.
	*/
	private class Evaent {

		private String name;
		private ArrayList<String> terms;
		private String startDate;

		private Evaent(final String name, final ArrayList<String> terms, final String startDate) {
			this.name = name;
			this.terms = terms;
			this.startDate = startDate;
		}

		private boolean eventMatch(final String needle) {
			return this.terms.contains(needle);
		}

		private double latencyCheck(final long foundtime) {
			double latency = (Long.valueOf(startDate) - foundtime) / 60.0;
			return Math.max(1, latency);
		}
	}
}
