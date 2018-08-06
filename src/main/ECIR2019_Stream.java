/*
 * @(#)ECIR2019_Stream.java   1.0   Jul 24, 2018
 */
package main;

import twistor.GlobalParameters;
import twistor.Stream;

/**
 * Class for creating the Twistor data stream with 10 percent scale.
 *
 * @author Andreas Weiler &lt;wele@zhaw.ch&gt;
 * @version 1.0
 */
public class ECIR2019_Stream {

	public static void main(String[] args) {
		System.out.println("STARTING of 10percent stream creation...");
		new ECIR2019_Stream("./data/twistor_stream_10p.csv", 10);
	}

	private ECIR2019_Stream(final String outputStreamFile, final int scale) {
		GlobalParameters.SCALE = scale;
		new Stream(outputStreamFile);
	}
}
