package scratch.kevin.cybershake.pshaVariability;

import java.awt.Color;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.apache.commons.math3.stat.StatUtils;
import org.jfree.chart.plot.DatasetRenderingOrder;
import org.jfree.chart.ui.RectangleAnchor;
import org.jfree.data.Range;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DefaultXY_DataSet;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.EvenlyDiscretizedFunc;
import org.opensha.commons.data.function.HistogramFunction;
import org.opensha.commons.data.function.XY_DataSet;
import org.opensha.commons.data.uncertainty.UncertainArbDiscFunc;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotUtils;
import org.opensha.commons.util.DataUtils;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.commons.util.DataUtils.MinMaxAveTracker;
import org.opensha.sha.earthquake.faultSysSolution.util.SolHazardMapCalc.ReturnPeriods;
import org.opensha.sha.earthquake.observedEarthquake.ObsEqkRupture;
import org.opensha.sha.simulators.RSQSimEvent;
import org.opensha.sha.simulators.utils.SimulatorUtils;

import com.google.common.base.Preconditions;

import scratch.UCERF3.erf.utils.ProbabilityModelsCalc;
import scratch.kevin.simCompare.IMT;
import scratch.kevin.simCompare.SimulationHazardCurveCalc;
import scratch.kevin.simCompare.SimulationRotDProvider;

public abstract class WindowedFractionalExceedanceCalculator<E> {
	
	private List<E> events;
	private List<? extends Site> sites;
	private ReturnPeriods[] rps;
	private boolean[][][] eventSiteExceedances;
	
	private List<DiscretizedFunc> curves;
	private double[][] siteGroundMotions;
	
	private Random rand = new Random();
	
	static class RSQSim extends WindowedFractionalExceedanceCalculator<RSQSimEvent> {

		public RSQSim(List<RSQSimEvent> events, SimulationRotDProvider<RSQSimEvent> simProv, List<? extends Site> sites,
				IMT imt, ReturnPeriods[] rps) {
			super(events, simProv, sites, imt, rps);
		}

		@Override
		protected double getEventTimeYears(RSQSimEvent event) {
			return event.getTimeInYears();
		}

		@Override
		protected RSQSimEvent cloneNewTime(RSQSimEvent event, double newTimeYears) {
			double timeSecs = newTimeYears * SimulatorUtils.SECONDS_PER_YEAR;
			return event.cloneNewTime(timeSecs, event.getID());
		}
		
	}
	
	static class ObsRup extends WindowedFractionalExceedanceCalculator<ObsEqkRupture> {

		public ObsRup(List<ObsEqkRupture> events, SimulationRotDProvider<ObsEqkRupture> simProv, List<? extends Site> sites,
				IMT imt, ReturnPeriods[] rps) {
			super(events, simProv, sites, imt, rps);
		}

		@Override
		protected double getEventTimeYears(ObsEqkRupture event) {
			return (double)event.getOriginTime() / ProbabilityModelsCalc.MILLISEC_PER_YEAR;
		}

		@Override
		protected ObsEqkRupture cloneNewTime(ObsEqkRupture event, double newTimeYears) {
			long newTime = (long)(newTimeYears * ProbabilityModelsCalc.MILLISEC_PER_YEAR + 0.5);
			ObsEqkRupture clone = (ObsEqkRupture)event.clone();
			clone.setOriginTime(newTime);
			return clone;
		}
		
	}

	public WindowedFractionalExceedanceCalculator(List<E> events, SimulationRotDProvider<E> simProv,
			List<? extends Site> sites, IMT imt, ReturnPeriods[] rps) {
		this.events = events;
		this.sites = sites;
		this.rps = rps;
		
		double prevTime = Double.NEGATIVE_INFINITY;
		for (E event : events) {
			double time = getEventTimeYears(event);
			Preconditions.checkState(time >= prevTime, "Catalog not in order! %s > %s", prevTime, time);
		}
		
		curves = new ArrayList<>();
		siteGroundMotions = new double[rps.length][sites.size()];
		
		SimulationHazardCurveCalc<E> simCurveCalc = new SimulationHazardCurveCalc<>(simProv);
		
		List<CompletableFuture<DiscretizedFunc>> curveFutures = new ArrayList<>();
		
		System.out.println("Calculating curves for "+sites.size()+" sites");
		
		for (int s=0; s<sites.size(); s++) {
			Site site = sites.get(s);
			curveFutures.add(CompletableFuture.supplyAsync(new Supplier<DiscretizedFunc>() {

				@Override
				public DiscretizedFunc get() {
					try {
						return simCurveCalc.calc(site, imt, 1d);
					} catch (IOException e) {
						throw ExceptionUtils.asRuntimeException(e);
					}
				}
			}));
		}
		
		for (int s=0; s<sites.size(); s++) {
			DiscretizedFunc curve = curveFutures.get(s).join();
			System.out.println("curve for site "+sites.get(s).name);
			curves.add(curve);
			System.out.println("Ground motions:");
			for (int r=0; r<rps.length; r++) {
				double val;
				if (rps[r].oneYearProb > curve.getMaxY())
					val = 0d;
				else if (rps[r].oneYearProb < curve.getMinY())
					// saturated
					val = curve.getMaxX();
				else
//					val = curve.getFirstInterpolatedX_inLogXLogYDomain(rps[r].oneYearProb);
					val = curve.getFirstInterpolatedX(rps[r].oneYearProb);
				System.out.println("\t"+rps[r]+":\t"+(float)val+" (g)");
				siteGroundMotions[r][s] = val;
			}
		}
		
		System.out.println("Calculating event/site exceedances for "+events.size()+" events and "+sites.size()+" sites");
		eventSiteExceedances = new boolean[events.size()][sites.size()][rps.length];
		List<CompletableFuture<int[]>> siteExceedCountFutures = new ArrayList<>(sites.size());
		for (int s=0; s<sites.size(); s++) {
			int siteIndex = s;
			Site site = sites.get(siteIndex);
			siteExceedCountFutures.add(CompletableFuture.supplyAsync(new Supplier<int[]>() {

				@Override
				public int[] get() {
					int[] exceeds = new int[rps.length];
					for (int i=0; i<events.size(); i++) {
						E event = events.get(i);
						double imVal;
						try {
							imVal = simProv.getValue(site, event, imt, 0);
						} catch (IOException e) {
							throw ExceptionUtils.asRuntimeException(e);
						}
						for (int r=0; r<rps.length; r++) {
							if ((float)imVal >= (float)siteGroundMotions[r][siteIndex]) {
								eventSiteExceedances[i][siteIndex][r] = true;
								exceeds[r]++;
							}
						}
					}
					return exceeds;
				}
			}));
		}
		for (int s=0; s<sites.size(); s++) {
			Site site = sites.get(s);
			int[] exceeds = siteExceedCountFutures.get(s).join();
			System.out.println("Site "+s+"/"+sites.size()+", "+site.getName());
			for (int r=0; r<rps.length; r++) {
				System.out.println("\t"+exceeds[r]+"/"+events.size()+" ("
						+pDF.format((double)exceeds[r]/(double)events.size())+") events exceed "
						+(float)siteGroundMotions[r][s]+" (g)");
			}
		}
	}
	
	public void setRandom(Random rand) {
		this.rand = rand;
	}
	
	private class ShuffledEvent {
		final E event;
		final boolean[][] siteExceedances;
		
		public ShuffledEvent(E event, boolean[][] siteExceedances) {
			super();
			this.event = event;
			this.siteExceedances = siteExceedances;
		}
	}
	
	private class ShuffledEventCompare implements Comparator<ShuffledEvent> {

		@Override
		public int compare(ShuffledEvent o1, ShuffledEvent o2) {
			return Double.compare(getEventTimeYears(o1.event), getEventTimeYears(o2.event));
		}
		
	}
	
	protected abstract double getEventTimeYears(E event);
	
	protected abstract E cloneNewTime(E event, double newTimeYears);
	
	public List<E> getEvents() {
		return events;
	}
	
	private static final DecimalFormat pDF = new DecimalFormat("0.00%");
	public static DecimalFormat oDF = new DecimalFormat("0.##");
	
	public static double calcExpectedExceedanceFract(ReturnPeriods rp, double observedDuration) {
		return 1d - Math.exp(-observedDuration/rp.returnPeriod);
	}
	
	public void plotCatalogExceedanceFractDists(double windowDuration, int numCatalogs,
			File outputDir, String prefix) throws IOException {
		ExceedanceResult[] results = getRandomFractExceedances(windowDuration, numCatalogs);
		
		for (int r=0; r<rps.length; r++) {
			String rpPrefix = prefix+"_"+rps[r].name();
			
			String title = rps[r].label+" Map, "+oDF.format(windowDuration)+"yr Observations";
			
			double expected = calcExpectedExceedanceFract(rps[r], windowDuration);
			
			System.out.println(title);
			System.out.println("\tExpected: "+pDF.format(expected));
			printExceedStats(results[r].catalogFractSiteExceedances);
			
			plotCatalogExceedanceFractDist(results[r], expected, false, outputDir, rpPrefix, title);
			plotCatalogExceedanceFractDist(results[r], expected, true, outputDir, rpPrefix+"_log", title);
		}
	}
	
	public void plotCatalogExceedanceFractDist(ExceedanceResult rpExceedances, double expected, boolean logX,
			File outputDir, String prefix, String title) throws IOException {
		HistogramFunction hist;
		Range xRange;
		if (logX) {
			hist = HistogramFunction.getEncompassingHistogram(-3, 0d, 0.05);
			xRange = new Range(1e-3, 1);
		} else {
			hist = HistogramFunction.getEncompassingHistogram(0d, 1d, 0.02);
			xRange = new Range(0, 1);
		}
		
		for (int i=0; i<rpExceedances.catalogFractSiteExceedances.length; i++) {
			double exceed = rpExceedances.catalogFractSiteExceedances[i];
			Preconditions.checkState(exceed >= 0d);
			Preconditions.checkState(exceed <= 1d);
			
			if (logX) {
				if (exceed == 0d)
					hist.add(0, 1d);
				else
					hist.add(hist.getClosestXIndex(Math.log10(exceed)), 1d);
			} else {
				hist.add(hist.getClosestXIndex(exceed), 1d);
			}
		}
		
		// normalize
		hist.normalizeBySumOfY_Vals();
//		// convert to density
//		if (logX) {
//			double halfDelta = 0.5*hist.getDelta();
//			for (int i=0; i<hist.size(); i++) {
//				double logCenter = hist.getX(i);
//				double left = Math.pow(10, logCenter-halfDelta);
//				double right = Math.pow(10, logCenter+halfDelta);
//				double width = right - left;
//				hist.set(i, hist.getY(i)/width);
//			}
//		} else {
//			hist.scale(1d/hist.getDelta());
//		}
		
		double maxY = calcFractionalPlotMaxY(hist.getMaxY());
		
		double mean = StatUtils.mean(rpExceedances.catalogFractSiteExceedances);
		double median = DataUtils.median(rpExceedances.catalogFractSiteExceedances);
		
		List<XY_DataSet> funcs = new ArrayList<>();
		List<PlotCurveCharacterstics> chars = new ArrayList<>();
		
		if (logX) {
			ArbitrarilyDiscretizedFunc linearHist = new ArbitrarilyDiscretizedFunc();
			for (Point2D pt : hist)
				linearHist.set(Math.pow(10, pt.getX()), pt.getY());
			funcs.add(linearHist);
		} else {
			funcs.add(hist);
		}
		chars.add(new PlotCurveCharacterstics(PlotLineType.HISTOGRAM, 1f, Color.GRAY));
		
		DefaultXY_DataSet expectedLine = new DefaultXY_DataSet();
		expectedLine.setName("Expected: "+pDF.format(expected));
		if (logX)
			expected = Math.max(expected, xRange.getLowerBound());
		expectedLine.set(expected, 0d);
		expectedLine.set(expected, maxY);
		funcs.add(expectedLine);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.GREEN.darker()));
		
		DefaultXY_DataSet meanLine = new DefaultXY_DataSet();
		meanLine.setName("Mean: "+pDF.format(mean));
		mean = Math.max(mean, xRange.getLowerBound());
		meanLine.set(mean, 0d);
		meanLine.set(mean, maxY);
		funcs.add(meanLine);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.BLUE.darker()));
		
		DefaultXY_DataSet medianLine = new DefaultXY_DataSet();
		medianLine.setName("Median: "+pDF.format(median));
		median = Math.max(median, xRange.getLowerBound());
		medianLine.set(median, 0d);
		medianLine.set(median, maxY);
		funcs.add(medianLine);
		chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.RED.darker()));
		
		PlotSpec spec = new PlotSpec(funcs, chars, title, "Fraction of Sites Exceeding Map Value", "Fraction");
		spec.setLegendInset(true);
		
		HeadlessGraphPanel gp = PlotUtils.initHeadless();
		gp.getPlotPrefs().scaleFontSizes(1.4);
		
		gp.drawGraphPanel(spec, logX, false, xRange, new Range(0d, maxY));
		
		PlotUtils.writePlots(outputDir, prefix, gp, 850, 800, true, true, false);
	}
	
	private static double calcFractionalPlotMaxY(double maxY) {
		if (maxY > 0.5)
			return 1;
		else if (maxY > 0.3)
			return 0.5;
		else if (maxY > 0.1)
			return 0.25;
		else
			return 0.15;
	}
	
	public void plotCatalogExceedanceFractsVsDuration(double minWindowDuration, double maxWindowDuration, int numWindows,
			int numCatalogs, File outputDir, String prefix) throws IOException {
		EvenlyDiscretizedFunc windowFunc = new EvenlyDiscretizedFunc(minWindowDuration, maxWindowDuration, numWindows);
		
		EvenlyDiscretizedFunc[] expectedFuncs = new EvenlyDiscretizedFunc[rps.length];
		EvenlyDiscretizedFunc[] meanFuncs = new EvenlyDiscretizedFunc[rps.length];
		EvenlyDiscretizedFunc[] medianFuncs = new EvenlyDiscretizedFunc[rps.length];
		
		EvenlyDiscretizedFunc[] minFuncs = new EvenlyDiscretizedFunc[rps.length];
		EvenlyDiscretizedFunc[] p2p5Funcs = new EvenlyDiscretizedFunc[rps.length];
		EvenlyDiscretizedFunc[] p16Funcs = new EvenlyDiscretizedFunc[rps.length];
		EvenlyDiscretizedFunc[] p84Funcs = new EvenlyDiscretizedFunc[rps.length];
		EvenlyDiscretizedFunc[] p97p5Funcs = new EvenlyDiscretizedFunc[rps.length];
		EvenlyDiscretizedFunc[] maxFuncs = new EvenlyDiscretizedFunc[rps.length];
		
		for (int r=0; r<rps.length; r++) {
			expectedFuncs[r] = new EvenlyDiscretizedFunc(windowFunc.getMinX(), windowFunc.size(), windowFunc.getDelta());
			meanFuncs[r] = new EvenlyDiscretizedFunc(windowFunc.getMinX(), windowFunc.size(), windowFunc.getDelta());
			medianFuncs[r] = new EvenlyDiscretizedFunc(windowFunc.getMinX(), windowFunc.size(), windowFunc.getDelta());
			
			minFuncs[r] = new EvenlyDiscretizedFunc(windowFunc.getMinX(), windowFunc.size(), windowFunc.getDelta());
			p2p5Funcs[r] = new EvenlyDiscretizedFunc(windowFunc.getMinX(), windowFunc.size(), windowFunc.getDelta());
			p16Funcs[r] = new EvenlyDiscretizedFunc(windowFunc.getMinX(), windowFunc.size(), windowFunc.getDelta());
			p84Funcs[r] = new EvenlyDiscretizedFunc(windowFunc.getMinX(), windowFunc.size(), windowFunc.getDelta());
			p97p5Funcs[r] = new EvenlyDiscretizedFunc(windowFunc.getMinX(), windowFunc.size(), windowFunc.getDelta());
			maxFuncs[r] = new EvenlyDiscretizedFunc(windowFunc.getMinX(), windowFunc.size(), windowFunc.getDelta());
		}
		
		for (int i=0; i<numWindows; i++) {
			double duration = windowFunc.getX(i);
			ExceedanceResult[] results = getRandomFractExceedances(duration, numCatalogs);
			
			for (int r=0; r<results.length; r++) {
				double[] exceeds = results[r].catalogFractSiteExceedances;
				
				double expected = calcExpectedExceedanceFract(rps[r], duration);
				expectedFuncs[r].set(i, expected);
				meanFuncs[r].set(i, StatUtils.mean(exceeds));
				medianFuncs[r].set(i, DataUtils.median(exceeds));
				
				minFuncs[r].set(i, StatUtils.min(exceeds));
				p2p5Funcs[r].set(i, StatUtils.percentile(exceeds, 2.5d));
				p16Funcs[r].set(i, StatUtils.percentile(exceeds, 16d));
				p84Funcs[r].set(i, StatUtils.percentile(exceeds, 84d));
				p97p5Funcs[r].set(i, StatUtils.percentile(exceeds, 97.5d));
				maxFuncs[r].set(i, StatUtils.max(exceeds));
				
				System.out.println(rps[r].label+" Map, "+oDF.format(duration)+"yr Observations");
				System.out.println("\tExpected: "+pDF.format(expected));
				printExceedStats(results[r].catalogFractSiteExceedances);
			}
		}
		
		for (int r=0; r<rps.length; r++) {
			List<DiscretizedFunc> funcs = new ArrayList<>();
			List<PlotCurveCharacterstics> chars = new ArrayList<>();
			
			expectedFuncs[r].setName("Expected");
			funcs.add(expectedFuncs[r]);
			chars.add(new PlotCurveCharacterstics(PlotLineType.DASHED, 3f, Color.GREEN.darker()));
			
			meanFuncs[r].setName("Mean");
			funcs.add(meanFuncs[r]);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.BLUE.darker()));
			
			medianFuncs[r].setName("Median");
			funcs.add(medianFuncs[r]);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.RED.darker()));
			
			UncertainArbDiscFunc extrema = new UncertainArbDiscFunc(medianFuncs[r], minFuncs[r], maxFuncs[r]);
			UncertainArbDiscFunc bounds95 = new UncertainArbDiscFunc(medianFuncs[r], p2p5Funcs[r], p97p5Funcs[r]);
			UncertainArbDiscFunc bounds68 = new UncertainArbDiscFunc(medianFuncs[r], p16Funcs[r], p84Funcs[r]);
			
			Color transColor = new Color(255, 0, 0, 60);
			extrema.setName("p0,2.5,16,84,97.5,100");
			funcs.add(extrema);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SHADED_UNCERTAIN, 1f, transColor));
			bounds68.setName(null);
			funcs.add(bounds68);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SHADED_UNCERTAIN, 1f, transColor));
			bounds95.setName(null);
			funcs.add(bounds95);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SHADED_UNCERTAIN, 1f, transColor));
			
			String title = rps[r].label+" Map";
			
			PlotSpec spec = new PlotSpec(funcs, chars, title, "Observation Window (yrs)",
					"Fraction of Sites Exceeding Map Value");
			spec.setLegendInset(RectangleAnchor.TOP_LEFT);
			
			String rpPrefix = prefix+"_"+rps[r].name();
			
			HeadlessGraphPanel gp = PlotUtils.initHeadless();
			gp.getPlotPrefs().scaleFontSizes(1.4);
			
			gp.setRenderingOrder(DatasetRenderingOrder.REVERSE);
			
			Range xRange = new Range(minWindowDuration, maxWindowDuration);
			Range yRange = new Range(0d, calcFractionalPlotMaxY(expectedFuncs[r].getMaxY()));
			gp.drawGraphPanel(spec, false, false, xRange, yRange);
			
			double xSpacing;
			if (xRange.getLength() > 400)
				xSpacing = 50;
			else if (xRange.getLength() > 150)
				xSpacing = 25;
			else
				xSpacing = 10;
			PlotUtils.setXTick(gp, xSpacing);
			
			PlotUtils.writePlots(outputDir, rpPrefix, gp, 850, 800, true, true, true);
		}
	}
	
	public ExceedanceResult[] getRandomFractExceedances(double duration, int numSamples) {
		return getRandomFractExceedances(duration, numSamples, rand);
	}
	
	public ExceedanceResult[] getRandomFractExceedances(double duration, int numSamples, Random r) {
		return getRandomFractExceedances(events, eventSiteExceedances, numSamples, duration, r);
	}
	
	public ExceedanceResult[] getShuffledRandomFractExceedances(double duration, int numSamples) {
		return getShuffledRandomFractExceedances(duration, numSamples, rand);
	}
	
	public ExceedanceResult[] getShuffledRandomFractExceedances(double duration, int numSamples, Random r) {
		List<ShuffledEvent> shuffled = new ArrayList<>(events.size());
		double curStart = getEventTimeYears(events.get(0));
		double curEnd = getEventTimeYears(events.get(events.size()-1));
		double fullDuration = curEnd - curStart;
		
		double aveRI = fullDuration/events.size();
		fullDuration += r.nextDouble()*aveRI;
		
		for (int i=0; i<events.size(); i++) {
			E e = events.get(i);
			double newTimeYears = r.nextDouble()*(fullDuration);
			shuffled.add(new ShuffledEvent(cloneNewTime(e, newTimeYears), eventSiteExceedances[i]));
		}
		Collections.sort(shuffled, new ShuffledEventCompare());
		
		List<E> shuffledEvents = new ArrayList<>(events.size());
		boolean[][][] shuffledEventSiteExceedances = new boolean[events.size()][sites.size()][rps.length];
		for (int i=0; i<events.size(); i++) {
			shuffledEvents.add(shuffled.get(i).event);
			shuffledEventSiteExceedances[i] = shuffled.get(i).siteExceedances;
		}
		
		return getRandomFractExceedances(shuffledEvents, shuffledEventSiteExceedances, numSamples, duration, r);
	}
	
	private ExceedanceResult[] getRandomFractExceedances(List<E> events, boolean[][][] eventSiteExceedances,
			int numSamples, double duration, Random r) {
		double minStart = getEventTimeYears(events.get(0));
		double maxStart = getEventTimeYears(events.get(events.size()-1))-duration;
		
		System.out.println("Building "+numSamples+" sub-catalogs with duration="+(float)duration);
		System.out.println("\tminStart="+(float)minStart);
		System.out.println("\tmaxStart="+(float)maxStart);
		
		double[] eventTimes = new double[events.size()];
		for (int i=0; i<eventTimes.length; i++)
			eventTimes[i] = getEventTimeYears(events.get(i));
		
		List<CompletableFuture<List<boolean[][]>>> catalogFutures = new ArrayList<>();
		for (int i=0; i<numSamples; i++) {
			double rand = r.nextDouble();
			double startTimeYears = minStart + rand*(maxStart-minStart);
			int searchIndex = Arrays.binarySearch(eventTimes, startTimeYears);
			if (searchIndex < 0)
				searchIndex = -(searchIndex + 1);
			int startIndex = searchIndex;
			catalogFutures.add(CompletableFuture.supplyAsync(new Supplier<List<boolean[][]>>() {

				@Override
				public List<boolean[][]> get() {
					return getFilteredEventExceeds(events, startTimeYears, duration, startIndex);
				}
			}));
		}

		MinMaxAveTracker sizeTrack = new MinMaxAveTracker();
		List<List<boolean[][]>> eventLists = new ArrayList<>(numSamples);
		for (CompletableFuture<List<boolean[][]>> future : catalogFutures) {
			List<boolean[][]> filteredEvents = future.join();
//			System.out.println("Sample "+i+" has "+filteredEvents.size()+" events");
			sizeTrack.addValue(filteredEvents.size());
			eventLists.add(filteredEvents);
		}
		System.out.println("\tsize distribution: "+sizeTrack);
		
		return calcFractExceedances(eventLists);
	}
	
	private List<boolean[][]> getFilteredEventExceeds(List<E> allEvents, double startTimeYears, double duration, int startIndex) {
		List<boolean[][]> filteredEvents = new ArrayList<>();
		double endTimeYears = startTimeYears+duration;
		for (int i=startIndex; i<events.size(); i++) {
			E event = allEvents.get(i);
			double time = getEventTimeYears(event);
			if (time < startTimeYears)
				continue;
			if (time > endTimeYears)
				break;
			filteredEvents.add(eventSiteExceedances[i]);
		}
		return filteredEvents;
	}
	
	public static class ExceedanceResult {
		public final ReturnPeriods rp;
		public final double[] catalogFractSiteExceedances;
		public final double[] siteFractExceedances;
		
		private ExceedanceResult(ReturnPeriods rp, boolean[][][] catalogSiteExceedances, int r) {
			this.rp = rp;
			
			int numCatalogs = catalogSiteExceedances.length;
			double[] catalogFractSiteExceedances = new double[numCatalogs];
			int numSites = catalogSiteExceedances[0][0].length;
			double[] siteFractExceedances = new double[numSites];
			
			for (int i=0; i<catalogSiteExceedances.length; i++) {
				int numSiteExceeds = 0;
				for (int s=0; s<numSites; s++) {
					if (catalogSiteExceedances[i][r][s]) {
						numSiteExceeds++;
						siteFractExceedances[s]++;
					}
				}
				catalogFractSiteExceedances[i] = (double)numSiteExceeds/(double)numSites;
			}
			// normalize site array
			for (int s=0; s<numSites; s++)
				siteFractExceedances[s] /= (double)numCatalogs;
			
			this.catalogFractSiteExceedances = catalogFractSiteExceedances;
			this.siteFractExceedances = siteFractExceedances;
		}
	}
	
	private ExceedanceResult[] calcFractExceedances(List<List<boolean[][]>> catalogEventSiteExceedances) {
		boolean[][][] catalogSiteExceedances = new boolean[catalogEventSiteExceedances.size()][rps.length][sites.size()];
		
		List<CompletableFuture<SiteExceedCounts>> siteFutures = new ArrayList<>();
		for (int s=0; s<sites.size(); s++) {
			int siteIndex = s;
			siteFutures.add(CompletableFuture.supplyAsync(new Supplier<SiteExceedCounts>() {

				@Override
				public SiteExceedCounts get() {
					int eventCount = 0;
					int[] eventExceedCounts = new int[rps.length];
					int[] catExceedCounts = new int[rps.length];
					for (int i=0; i<catalogEventSiteExceedances.size(); i++) {
						List<boolean[][]> eventSiteExceedances = catalogEventSiteExceedances.get(i);
						
						for (int e=0; e<eventSiteExceedances.size(); e++) {
							for (int r=0; r<rps.length; r++) {
								if (eventSiteExceedances.get(e)[siteIndex][r]) {
									catalogSiteExceedances[i][r][siteIndex] = true;
									eventExceedCounts[r]++;
								}
							}
							eventCount++;
						}
						for (int r=0; r<rps.length; r++)
							if (catalogSiteExceedances[i][r][siteIndex])
								catExceedCounts[r]++;
					}
					return new SiteExceedCounts(eventCount, eventExceedCounts, catalogEventSiteExceedances.size(), catExceedCounts);
				}
			}));
		}
		
		for (int s=0; s<sites.size(); s++) {
			siteFutures.get(s).join();
//			Site site = sites.get(s);
//			SiteExceedCounts counts = siteFutures.get(s).join();
//			System.out.println("Exceedances for site "+s+"/"+sites.size()+", "+site.getName());
//			counts.printStats();
		}
		
//		System.out.println("Converting to fractional exceedances");
		ExceedanceResult[] ret = new ExceedanceResult[rps.length];
		for (int r=0; r<ret.length; r++)
			ret[r] = new ExceedanceResult(rps[r], catalogSiteExceedances, r);
		System.out.println("Done");
		return ret;
	}
	
	private class SiteExceedCounts {
		final int eventCount;
		final int catalogCount;
		final int[] eventExceedCounts;
		final int[] catExceedCounts;
		
		public SiteExceedCounts(int eventCount, int[] eventExceedCounts, int catalogCount, int[] catExceedCounts) {
			super();
			this.eventCount = eventCount;
			this.eventExceedCounts = eventExceedCounts;
			this.catalogCount = catalogCount;
			this.catExceedCounts = catExceedCounts;
		}
		
		public void printStats() {
			for (int r=0; r<rps.length; r++)
				System.out.println("\t"+rps[r]+"\teventExceeds="+eventExceedCounts[r]+"/"+eventCount
						+" ("+pDF.format((double)eventExceedCounts[r]/eventCount)+");"
						+"\tcatExceeds="+catExceedCounts[r]+"/"+catalogCount
						+" ("+pDF.format((double)catExceedCounts[r]/catalogCount)+")");
		}
	}
	
	public static void printExceedStats(double[] exceedFracts) {
		double mean = StatUtils.mean(exceedFracts);
		double median = DataUtils.median(exceedFracts);
		double min = StatUtils.min(exceedFracts);
		double max = StatUtils.max(exceedFracts);
		double p2p5 = StatUtils.percentile(exceedFracts, 2.5d);
		double p16 = StatUtils.percentile(exceedFracts, 16d);
		double p84 = StatUtils.percentile(exceedFracts, 84d);
		double p97p5 = StatUtils.percentile(exceedFracts, 97.5d);
		System.out.println("\tMean:\t"+pDF.format(mean));
		System.out.println("\tMedian:\t"+pDF.format(median));
		System.out.println("\tRange:\t["+pDF.format(min)+", "+pDF.format(max)+"]");
		System.out.println("\t95% range:\t["+pDF.format(p2p5)+", "+pDF.format(p97p5)+"]");
		System.out.println("\t68% range:\t["+pDF.format(p16)+", "+pDF.format(p84)+"]");
	}

}
