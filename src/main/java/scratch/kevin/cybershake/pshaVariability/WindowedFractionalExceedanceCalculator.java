package scratch.kevin.cybershake.pshaVariability;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.apache.commons.math3.stat.StatUtils;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.util.DataUtils;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.commons.util.DataUtils.MinMaxAveTracker;
import org.opensha.sha.earthquake.faultSysSolution.util.SolHazardMapCalc.ReturnPeriods;
import org.opensha.sha.simulators.RSQSimEvent;

import scratch.kevin.simCompare.IMT;
import scratch.kevin.simCompare.SimulationHazardCurveCalc;
import scratch.kevin.simCompare.SimulationRotDProvider;

public class WindowedFractionalExceedanceCalculator {
	
	private List<RSQSimEvent> events;
	private IMT imt;
	private SimulationRotDProvider<RSQSimEvent> simProv;
	private List<? extends Site> sites;
	private ReturnPeriods[] rps;
	
	private List<DiscretizedFunc> curves;
	private double[][] siteGroundMotions;

	public WindowedFractionalExceedanceCalculator(List<RSQSimEvent> events, SimulationRotDProvider<RSQSimEvent> simProv,
			List<? extends Site> sites, IMT imt, ReturnPeriods... rps) {
		this.events = events;
		this.imt = imt;
		this.simProv = simProv;
		this.sites = sites;
		this.rps = rps;
		
		curves = new ArrayList<>();
		siteGroundMotions = new double[rps.length][sites.size()];
		
		SimulationHazardCurveCalc<RSQSimEvent> simCurveCalc = new SimulationHazardCurveCalc<>(simProv);
		
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
	}
	
	private static final DecimalFormat pDF = new DecimalFormat("0.00%");
	
	public ExceedanceResult[] getRandomFractExceedances(double duration, int numSamples) {
		return getRandomFractExceedances(duration, numSamples, new Random());
	}
	
	public ExceedanceResult[] getRandomFractExceedances(double duration, int numSamples, Random r) {
		return getRandomFractExceedances(events, numSamples, duration, r);
	}
	
	public ExceedanceResult[] getShuffledRandomFractExceedances(double duration, int numSamples) {
		return getShuffledRandomFractExceedances(duration, numSamples, new Random());
	}
	
	private List<RSQSimEvent> getShuffledCatalog(Random r) {
		List<RSQSimEvent> shuffled = new ArrayList<>();
		double startSecs = events.get(0).getTime();
		double endSecs = events.get(events.size()-1).getTime();
		double durationSecs = endSecs - startSecs;
		for (RSQSimEvent e : events) {
			double timeSeconds = startSecs + r.nextDouble()*(durationSecs);
			shuffled.add(e.cloneNewTime(timeSeconds, e.getID()));
		}
		Collections.sort(shuffled);
		return shuffled;
	}
	
	public ExceedanceResult[] getShuffledRandomFractExceedances(double duration, int numSamples, Random r) {
		List<RSQSimEvent> shuffled = getShuffledCatalog(r);
		
		return getRandomFractExceedances(shuffled, numSamples, duration, r);
	}
	
	private ExceedanceResult[] getRandomFractExceedances(List<RSQSimEvent> events, int numSamples, double duration, Random r) {
		double minStart = events.get(0).getTimeInYears();
		double maxStart = events.get(events.size()-1).getTimeInYears()-duration;
		
		System.out.println("Building "+numSamples+" sub-catalogs with duration="+(float)duration);
		System.out.println("\tminStart="+(float)minStart);
		System.out.println("\tmaxStart="+(float)maxStart);
		MinMaxAveTracker sizeTrack = new MinMaxAveTracker();
		List<List<RSQSimEvent>> eventLists = new ArrayList<>(numSamples);
		int largest = 10;
		for (int i=0; i<numSamples; i++) {
			double rand = r.nextDouble();
			double startTimeYears = minStart + rand*(maxStart-minStart);
			List<RSQSimEvent> filteredEvents = getFilteredEvents(events, startTimeYears, duration, largest);
			if (filteredEvents.size() > largest)
				largest = filteredEvents.size();
//			System.out.println("Sample "+i+" has "+filteredEvents.size()+" events");
			sizeTrack.addValue(filteredEvents.size());
			eventLists.add(filteredEvents);
		}
		System.out.println("\tsize distribution: "+sizeTrack);
		
		return calcFractExceedances(eventLists);
	}
	
	private static List<RSQSimEvent> getFilteredEvents(List<RSQSimEvent> allEvents, double startTimeYears,
			double duration, int initialCapacity) {
		List<RSQSimEvent> filteredEvents = new ArrayList<>(initialCapacity);
		double endTimeYears = startTimeYears+duration;
		for (RSQSimEvent event : allEvents) {
			double time = event.getTimeInYears();
			if (time < startTimeYears)
				continue;
			if (time > endTimeYears)
				break;
			filteredEvents.add(event);
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
	
	private ExceedanceResult[] calcFractExceedances(List<List<RSQSimEvent>> eventLists) {
		boolean[][][] catalogSiteExceedances = new boolean[eventLists.size()][rps.length][sites.size()];
		
		List<CompletableFuture<SiteExceedCounts>> siteFutures = new ArrayList<>();
		for (int s=0; s<sites.size(); s++) {
			Site site = sites.get(s);
			int siteIndex = s;
			siteFutures.add(CompletableFuture.supplyAsync(new Supplier<SiteExceedCounts>() {

				@Override
				public SiteExceedCounts get() {
					int eventCount = 0;
					int[] eventExceedCounts = new int[rps.length];
					int[] catExceedCounts = new int[rps.length];
					for (int i=0; i<eventLists.size(); i++) {
						List<RSQSimEvent> filteredEvents = eventLists.get(i);
						
						for (RSQSimEvent event : filteredEvents) {
							DiscretizedFunc eventIMs;
							try {
								eventIMs = simProv.getRotD50(site, event, 0);
							} catch (IOException e) {
								throw ExceptionUtils.asRuntimeException(e);
							}
							if (eventIMs != null) {
								double imVal = eventIMs.getY(imt.getPeriod());
								for (int r=0; r<rps.length; r++) {
									if ((float)imVal >= (float)siteGroundMotions[r][siteIndex]) {
										catalogSiteExceedances[i][r][siteIndex] = true;
										eventExceedCounts[r]++;
									}
								}
								eventCount++;
							}
						}
						for (int r=0; r<rps.length; r++)
							if (catalogSiteExceedances[i][r][siteIndex])
								catExceedCounts[r]++;
					}
					return new SiteExceedCounts(eventCount, eventExceedCounts, eventLists.size(), catExceedCounts);
				}
			}));
		}
		
		for (int s=0; s<sites.size(); s++) {
			Site site = sites.get(s);
			SiteExceedCounts counts = siteFutures.get(s).join();
			System.out.println("Exceedances for site "+s+"/"+sites.size()+", "+site.getName());
			counts.printStats();
		}
		
		System.out.println("Converting to fractional exceedances");
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
