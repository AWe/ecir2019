/*
 * @(#)ECIR2019_Evaluation.java   1.0   Jul 24, 2018
 */
package main;

import evaluation.EvaluationModule;

/**
 * Class for running the evaluation of the ECIR 2019 results.
 * 
 * @author Andreas Weiler &lt;wele@zhaw.ch&gt;
 * @version 1.0
 */
public class ECIR2019_Evaluation {

	public static void main(String[] args) {
		try {
			System.out.println("STARTING of 10percent result evaluation...");
			new EvaluationModule("./data/events_10p.xml", "./results/10p");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
