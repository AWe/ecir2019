/*
 * @(#)ECIR2019_Experiment.java   1.0   Jul 24, 2018
 */
package main;

import eventdetection.EventDetectorModules;

/**
 * Class for running the complete ECIR 2019 experiment.
 *
 * @author Andreas Weiler &lt;wele@zhaw.ch&gt;
 * @version 1.0
 */
public class ECIR2019_Experiment {

	public static void main(String[] args) {
		System.out.println("STARTING 10percent evaluation...");
		new ECIR2019_Experiment("./data/twistor_stream_10p.csv", "./results/10p");
	}

	private ECIR2019_Experiment(final String streamFile, final String outputFolder) {
		try {
			EventDetectorModules ed = new EventDetectorModules(streamFile, outputFolder);
			String[] types = {"random"};
			int[] iws = {600000, 900000, 1200000, 1500000};
			for (String type : types) {
				switch (type) {
				case "random":
					for (int i = 1; i <= 25; i++) {
						for (int w : iws) {
							System.out.println("Running random...");
							ed.runRandomEvents(type, i, w);
						}
					}
					break;
				case "topn":
					for (int i = 1; i <= 25; i++) {
						for (int w : iws) {
							System.out.println("Running topN...");
							ed.runTopN(type, i, w);
						}
					}
					break;
				case "shifty":
					for (int i = 1; i <= 100; i++) {
						System.out.println("Running shifty...");
						ed.runShifty(type, i);
					}
					break;
				case "llh":
					for (int i = 1; i <= 25; i++) {
						for (int w : iws) {
							System.out.println("Running LLH...");
							ed.runLLH(type, i, w);
						}
					}
					break;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
