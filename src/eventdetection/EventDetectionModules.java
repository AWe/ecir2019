/*
 * @(#)EventDetectionModules.java   1.0   Jul 24, 2018
 */
package eventdetection;

import java.io.File;
import java.io.PrintStream;
import java.util.Date;
import java.util.function.Function;

import edu.pdx.cs.niagarino.PhysicalQueryPlan;
import edu.pdx.cs.niagarino.PhysicalQueryPlan.OperatorType;
import edu.pdx.cs.niagarino.operator.AggregationFunction;
import edu.pdx.cs.niagarino.operator.AggregationType;
import edu.pdx.cs.niagarino.operator.Derive;
import edu.pdx.cs.niagarino.operator.Operator;
import edu.pdx.cs.niagarino.operator.OrderedAggregate;
import edu.pdx.cs.niagarino.operator.Print;
import edu.pdx.cs.niagarino.operator.Scan;
import edu.pdx.cs.niagarino.operator.ValueWindow;
import edu.pdx.cs.niagarino.stream.Attribute;
import edu.pdx.cs.niagarino.stream.Schema;

import kn.uni.inf.niagarino.operator.Projection;
import kn.uni.inf.niagarino.operator.LimitationType;
import kn.uni.inf.niagarino.operator.OrderingType;
import kn.uni.inf.niagarino.operator.Unnest;
import kn.uni.inf.niagarino.operator.eatd.Distinct;
import kn.uni.inf.niagarino.operator.eatd.LLH;
import kn.uni.inf.niagarino.operator.eatd.RandomEvents;
import kn.uni.inf.niagarino.util.TokensDerivationFunction;

/**
 * Class for running different event detection approaches for the ECIR 2019 experiment.
 *
 * @author Andreas Weiler &lt;wele@zhaw.ch&gt;
 * @version 1.0
 */
public class EventDetectionModules {

	/** Twitter schema of tuples in test data set. */
	private static final Schema TWITTER_SCHEMA = new Schema(1, new Attribute("tweetid", Long.class),
			new Attribute("creationdate", Date.class), new Attribute("content", String.class));
	/** Datasource. */
	private String streamFile;
	/** OutputFolder. */
	private String outputFolder;

	public EventDetectionModules(final String streamFile, final String outputF) throws Exception {
		this.streamFile = streamFile;
		this.outputFolder = outputF;
	}

	/**
	 * Preprocessing steps.
	 * @param unnest flag for unnesting
	 * @return physical query plan
	 */
	private PhysicalQueryPlan preprocess(final boolean unnest) {
		// scan file
		final Operator scan = new Scan("scan", TWITTER_SCHEMA, this.streamFile, false, "\t");
		// derives tokens of tweet
		Function<String, String> cleanTerm = check -> { return TokensDerivationFunction.getCleanTerm(check); };
		final Operator deriveTokens = new Derive(scan.getOutputSchema(), new TokensDerivationFunction(2, "\\s", cleanTerm));

		// physical query plan
		final PhysicalQueryPlan plan = new PhysicalQueryPlan(false);
		// add operators
		plan.addOperator(scan, OperatorType.SOURCE);
		plan.addOperator(deriveTokens);
		// add streams
		plan.addStream(scan, deriveTokens);
		if (unnest) {
			// unnest nested tokens
			final Operator unnestTokens = new Unnest(deriveTokens.getOutputSchema(), 3, "token");
			plan.addOperator(unnestTokens);
			plan.addStream(deriveTokens, unnestTokens);
		}
		return plan;
	}

	/**
	 * Starts the approach of the MSDM paper.
	 *
	 * @param name
	 *        string of name
	 * @param threshold
	 *         threshold to become an event
	 * @throws Exception exception
	 */
	public void runShifty(final String name, final int threshold) throws Exception {
		final PhysicalQueryPlan plan = this.preprocess(true);
		final Operator last = plan.getLastStreamOperator();
		// create windows and calculate values
		final Operator tupleWindowIDF = new ValueWindow(last.getOutputSchema(), 60000, 60000);
		final Operator idf = new OrderedAggregate(tupleWindowIDF.getOutputSchema(), new int[]{4}, new int[]{0},
				new AggregationFunction(AggregationType.IDF, LimitationType.LTWINDOWAVERAGE));//, new AggregationFunction(AggregationType.TOPCOOC, 5));
		final Operator tupleWindowSShifts = new ValueWindow(idf.getOutputSchema(), 120000, 60000);
		final Operator shift = new OrderedAggregate(tupleWindowSShifts.getOutputSchema(), new int[]{1}, new int[]{2},
				new AggregationFunction(AggregationType.SHIFT, LimitationType.GTWINDOWAVERAGE));//, new AggregationFunction(AggregationType.TOPCOOC, 5));
		final Operator tupleWindowTShifts = new ValueWindow(shift.getOutputSchema(), 240000, 60000);
		final Operator tshift = new OrderedAggregate(tupleWindowTShifts.getOutputSchema(), new int[]{1}, new int[]{2},
				new AggregationFunction(AggregationType.SUM, 4, LimitationType.GTVALUE, threshold));//, new AggregationFunction(AggregationType.TOPCOOC, 5));
		// projects away unnecessary attributes
		final Operator projection = new Projection(tshift.getOutputSchema(), new int[]{0, 1});
		// distinct events for hourly windows
		final Operator distinct = new Distinct("distinct", projection.getOutputSchema(), 1, 59);
		// print results
		PrintStream output = new PrintStream(new File(this.outputFolder + "/" + name + "_" + threshold + ".txt"));
		final Operator printer = new Print(distinct.getOutputSchema(), false, output);

		plan.addOperator(tupleWindowIDF);
		plan.addOperator(idf);
		plan.addOperator(tupleWindowSShifts);
		plan.addOperator(shift);
		plan.addOperator(tupleWindowTShifts);
		plan.addOperator(tshift);
		plan.addOperator(projection);
		plan.addOperator(distinct);
		plan.addOperator(printer, OperatorType.SINK);

		// add streams
		plan.addStream(last, tupleWindowIDF);
		plan.addStream(tupleWindowIDF, idf);
		plan.addStream(idf, tupleWindowSShifts);
		plan.addStream(tupleWindowSShifts, shift);
		plan.addStream(shift, tupleWindowTShifts);
		plan.addStream(tupleWindowTShifts, tshift);
		plan.addStream(tshift, projection);
		plan.addStream(projection, distinct);
		plan.addStream(distinct, printer);
		// execute stream
		plan.execute(output);
	}

	/**
	 * Starts a LLH analysis.
	 *
	 * @param name
	 *        string of name
	 * @param topN
	 *        top N results
	 * @throws Exception exception
	 */
	public void runLLH(final String name, final int topN, final int iws) throws Exception {
		final PhysicalQueryPlan plan = this.preprocess(false);
		final Operator last = plan.getLastStreamOperator();

		// create windows
		final Operator tupleWindow = new ValueWindow(last.getOutputSchema(), iws, iws);
		final Operator llh = new LLH("LLH", tupleWindow.getOutputSchema(), 3, topN);
		// print results
		PrintStream output = new PrintStream(new File(this.outputFolder + "/" + name + "_" + topN + "_" + iws + ".txt"));
		final Operator printer = new Print(llh.getOutputSchema(), false, output);

		plan.addOperator(tupleWindow);
		plan.addOperator(llh);
		plan.addOperator(printer, OperatorType.SINK);
		// add streams
		plan.addStream(last, tupleWindow);
		plan.addStream(tupleWindow, llh);
		plan.addStream(llh, printer);
		// execute stream
		plan.execute(output);
	}

	   /**
	    * Starts a RandomEvents analysis.
	    *
	    * @param name
	    *        string of name
	    * @param n
	    *        number of results
	    * @throws Exception exception
	    */
	   public void runRandomEvents(final String name, final int n, final int iws) throws Exception {
	      final PhysicalQueryPlan plan = this.preprocess(false);
	      final Operator last = plan.getLastStreamOperator();

	      // create windows
	      final Operator tupleWindow = new ValueWindow(last.getOutputSchema(), iws, iws);
	      final Operator randomTopics = new RandomEvents("RandomEvents", tupleWindow.getOutputSchema(), 3, n);
	      // print results
	      PrintStream output = new PrintStream(new File(this.outputFolder + "/" + name + "_" + n + "_" + iws + ".txt"));
	      final Operator printer = new Print(randomTopics.getOutputSchema(), false, output);

	      plan.addOperator(tupleWindow);
	      plan.addOperator(randomTopics);
	      plan.addOperator(printer, OperatorType.SINK);
	      // add streams
	      plan.addStream(last, tupleWindow);
	      plan.addStream(tupleWindow, randomTopics);
	      plan.addStream(randomTopics, printer);
	      // execute stream
	      plan.execute(output);
	   }

	   /**
	    * Starts a really simple approach.
	    *
	    * @param name
	    *        string of name
	    * @param topN
	    *        top N number of hits
	    * @throws Exception exception
	    */
	   public void runTopN(final String name, final int topN, final int iws) throws Exception {
	      final PhysicalQueryPlan plan = this.preprocess(true);
	      final Operator last = plan.getLastStreamOperator();
	      // create windows with window size
	      final Operator tupleWindow = new ValueWindow(last.getOutputSchema(), iws, iws);
	      // aggregate
	      final Operator aggregate = new OrderedAggregate(tupleWindow.getOutputSchema(), new int[] { 4 },
	            new int[] { 0 }, new AggregationFunction(AggregationType.COUNT, OrderingType.DESC, LimitationType.TUPLECOUNT, topN));
	      // print
	      PrintStream output = new PrintStream(new File(this.outputFolder + "/" + name + "_" + topN + "_" + iws + ".txt"));
	      final Operator printer = new Print(aggregate.getOutputSchema(), false, output);

	      plan.addOperator(tupleWindow);
	      plan.addOperator(aggregate);
	      plan.addOperator(printer, OperatorType.SINK);

	      plan.addStream(last, tupleWindow);
	      plan.addStream(tupleWindow, aggregate);
	      plan.addStream(aggregate, printer);
	      // execute
	      plan.execute(output);
	   }
}
