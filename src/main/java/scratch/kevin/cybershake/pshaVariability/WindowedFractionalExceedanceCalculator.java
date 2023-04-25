package scratch.kevin.cybershake.pshaVariability;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.math3.stat.StatUtils;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.LightFixedXFunc;
import org.opensha.commons.geo.Location;
import org.opensha.commons.util.DataUtils;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.calc.mcer.CyberShakeSiteRun;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.HazardCurve2DB;
import org.opensha.sha.earthquake.faultSysSolution.util.SolHazardMapCalc.ReturnPeriods;
import org.opensha.sha.imr.param.IntensityMeasureParams.DurationTimeInterval;
import org.opensha.sha.simulators.RSQSimEvent;

import com.google.common.base.Preconditions;

import scratch.kevin.simCompare.IMT;
import scratch.kevin.simCompare.SimulationHazardCurveCalc;
import scratch.kevin.simCompare.SimulationRotDProvider;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimProbEqkRup;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimSectBundledSource;

public class WindowedFractionalExceedanceCalculator {
	
	private List<RSQSimEvent> events;
	private CyberShakeStudy study;
	private CybershakeIM im;
	private ReturnPeriods[] rps;
	
	private List<CyberShakeSiteRun> sites;
	private List<DiscretizedFunc> curves;
	private double[][] siteGroundMotions;
	
	private CachedPeakAmplitudesFromDB amps2DB;
	private RSQSimSectBundledERF erf;
	private Map<Integer, int[]> eventIDtoSrcRups;
	
	private static boolean RECALC_HIGH_RES_CURVES = true;

	public WindowedFractionalExceedanceCalculator(List<RSQSimEvent> events, CyberShakeStudy study,
			List<CybershakeRun> runs, CybershakeIM im, ReturnPeriods... rps) {
		this(events, study, runs, im, null, rps);
	}
	
	public WindowedFractionalExceedanceCalculator(List<RSQSimEvent> events, CyberShakeStudy study,
			List<CybershakeRun> runs, CybershakeIM im, File ampsCacheDir, ReturnPeriods... rps) {
		this.study = study;
		this.im = im;
		this.rps = rps;
		
		sites = CyberShakeSiteBuilder.buildSites(study, Vs30_Source.Simulation, runs);
		
		curves = new ArrayList<>();
		siteGroundMotions = new double[rps.length][sites.size()];
		
		HazardCurve2DB curves2DB = new HazardCurve2DB(study.getDB());
		
		erf = (RSQSimSectBundledERF)study.buildNewERF();
		eventIDtoSrcRups = new HashMap<>(events.size());
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			RSQSimSectBundledSource source = erf.getSource(sourceID);
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				RSQSimProbEqkRup rup = source.getRupture(rupID);
				int eventID = rup.getEventID();
				eventIDtoSrcRups.put(eventID, new int[] {sourceID, rupID});
			}
		}
		
		amps2DB = new CachedPeakAmplitudesFromDB(study.getDB(), ampsCacheDir, erf);
		
		List<RSQSimEvent> matchedEvents = new ArrayList<>(events.size());
		for (RSQSimEvent event : events)
			if (eventIDtoSrcRups.containsKey(event.getID()))
				matchedEvents.add(event);
		System.out.println("Retained "+matchedEvents.size()+"/"+events.size()+" events that exist in CS ERF");
		this.events = matchedEvents;
		
		SimulationHazardCurveCalc<RSQSimEvent> simCalc = null;
		IMT imt = null;
		if (RECALC_HIGH_RES_CURVES) {
			SimulationHazardCurveCalc.DEFAULT_X_VAL_MULT = 8;
			simCalc = new SimulationHazardCurveCalc<>(new SimRotDPRovider());
			imt = IMT.forPeriod(im.getVal());
		}
		
		for (int s=0; s<sites.size(); s++) {
			CyberShakeSiteRun site = sites.get(s);
			DiscretizedFunc curve;
			if (RECALC_HIGH_RES_CURVES) {
				System.out.println("Calculating curve for site "+site.name);
				try {
					curve = simCalc.calc(site, imt, 1d);
				} catch (IOException e) {
					throw ExceptionUtils.asRuntimeException(e);
				}
			} else {
				System.out.println("Fetching curve for site "+site.name);
				int id = curves2DB.getHazardCurveID(site.getCS_Run().getRunID(), im.getID());
				Preconditions.checkState(id >= 0);
				curve = curves2DB.getHazardCurve(id);
			}
			
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
	
	private class SimRotDPRovider implements SimulationRotDProvider<RSQSimEvent> {
		
		private double durationYears;
		private double[] x;
		
		public SimRotDPRovider() {
			durationYears = events.get(events.size()-1).getTimeInYears() - events.get(0).getTimeInYears();
			x = new double[] {im.getVal()};
		}

		@Override
		public String getName() {
			return "RSQSim/CyberShake";
		}

		@Override
		public DiscretizedFunc getRotD50(Site site, RSQSimEvent rupture, int index) throws IOException {
			double[][][] ims;
			try {
				ims = amps2DB.getAllIM_Values(((CyberShakeSiteRun)site).getCS_Run().getRunID(), im);
			} catch (SQLException e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
			int[] srcRup = eventIDtoSrcRups.get(rupture.getID());
			double[] y = { ims[srcRup[0]][srcRup[1]][0] / HazardCurveComputation.CONVERSION_TO_G };
			return new LightFixedXFunc(x, y);
		}

		@Override
		public DiscretizedFunc getRotD100(Site site, RSQSimEvent rupture, int index) throws IOException {
			throw new IllegalStateException();
		}

		@Override
		public DiscretizedFunc[] getRotD(Site site, RSQSimEvent rupture, int index) throws IOException {
			throw new IllegalStateException();
		}

		@Override
		public DiscretizedFunc getRotDRatio(Site site, RSQSimEvent rupture, int index) throws IOException {
			throw new IllegalStateException();
		}

		@Override
		public double getPGV(Site site, RSQSimEvent rupture, int index) throws IOException {
			throw new IllegalStateException();
		}

		@Override
		public double getDuration(Site site, RSQSimEvent rupture, DurationTimeInterval interval, int index)
				throws IOException {
			throw new IllegalStateException();
		}

		@Override
		public int getNumSimulations(Site site, RSQSimEvent rupture) {
			return 1;
		}

		@Override
		public Location getHypocenter(RSQSimEvent rupture, int index) {
			throw new IllegalStateException();
		}

		@Override
		public Collection<RSQSimEvent> getRupturesForSite(Site site) {
			Preconditions.checkState(site instanceof CyberShakeSiteRun);
			List<RSQSimEvent> eventsForSite = new ArrayList<>();
			try {
				double[][][] ims = amps2DB.getAllIM_Values(((CyberShakeSiteRun)site).getCS_Run().getRunID(), im);
				
				for (RSQSimEvent event : events) {
					int[] srcRup = eventIDtoSrcRups.get(event.getID());
					if (ims[srcRup[0]] != null && ims[srcRup[0]][srcRup[1]] != null)
						eventsForSite.add(event);
				}
			} catch (SQLException e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
			return eventsForSite;
		}

		@Override
		public boolean hasRotD50() {
			return true;
		}

		@Override
		public boolean hasRotD100() {
			return false;
		}

		@Override
		public boolean hasPGV() {
			return false;
		}

		@Override
		public boolean hasDurations() {
			return false;
		}

		@Override
		public double getAnnualRate(RSQSimEvent rupture) {
			return 1d/durationYears;
		}

		@Override
		public double getMinimumCurvePlotRate(Site site) {
			return getAnnualRate(null);
		}

		@Override
		public double getMagnitude(RSQSimEvent rupture) {
			return rupture.getMagnitude();
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
		
		System.out.println("Building "+numSamples+" sub-catalogs");
		List<List<RSQSimEvent>> eventLists = new ArrayList<>(numSamples);
		int largest = 10;
		for (int i=0; i<numSamples; i++) {
			double rand = r.nextDouble();
			double startTimeYears = minStart + rand*(maxStart-minStart);
			List<RSQSimEvent> filteredEvents = getFilteredEvents(events, startTimeYears, duration, largest);
			if (filteredEvents.size() > largest)
				largest = filteredEvents.size();
//			System.out.println("Sample "+i+" has "+filteredEvents.size()+" events");
			eventLists.add(filteredEvents);
		}
		
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
		
		for (int s=0; s<sites.size(); s++) {
			CyberShakeSiteRun site = sites.get(s);
			System.out.println("Calculating exceedances for site "+s+"/"+sites.size()+", "+site.getName());
			CybershakeRun run = site.getCS_Run();
			double[][][] ims;
			try {
				ims = amps2DB.getAllIM_Values(run.getRunID(), im);
			} catch (SQLException e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
			
			int eventCount = 0;
			int[] eventExceedCounts = new int[rps.length];
			int[] catExceedCounts = new int[rps.length];
			for (int i=0; i<eventLists.size(); i++) {
				List<RSQSimEvent> filteredEvents = eventLists.get(i);
				
				for (RSQSimEvent event : filteredEvents) {
					int[] srcRup = eventIDtoSrcRups.get(event.getID());
					int sourceID = srcRup[0];
					int rupID = srcRup[1];
					if (ims[sourceID] != null && ims[sourceID][rupID] != null) {
						eventCount++;
						Preconditions.checkState(ims[sourceID][rupID].length == 1);
						double imVal =  ims[sourceID][rupID][0];
						imVal /= HazardCurveComputation.CONVERSION_TO_G;
						for (int r=0; r<rps.length; r++) {
							if (imVal >= siteGroundMotions[r][s]) {
								catalogSiteExceedances[i][r][s] = true;
								eventExceedCounts[r]++;
							}
						}
					}
				}
				for (int r=0; r<rps.length; r++)
					if (catalogSiteExceedances[i][r][s])
						catExceedCounts[r]++;
			}
			for (int r=0; r<rps.length; r++)
				System.out.println("\t"+rps[r]+"\teventExceeds="+eventExceedCounts[r]+"/"+eventCount
						+" ("+pDF.format((double)eventExceedCounts[r]/eventCount)+");"
						+"\tcatExceeds="+catExceedCounts[r]+"/"+eventLists.size()
						+" ("+pDF.format((double)catExceedCounts[r]/eventLists.size())+")");
		}
		
		System.out.println("Converting to fractional exceedances");
		ExceedanceResult[] ret = new ExceedanceResult[rps.length];
		for (int r=0; r<ret.length; r++)
			ret[r] = new ExceedanceResult(rps[r], catalogSiteExceedances, r);
		System.out.println("Done");
		return ret;
	}
	
	public static void main(String[] args) throws IOException {
		CyberShakeStudy study = CyberShakeStudy.STUDY_22_3_RSQSIM_5413;
		int skipYears = 20000;
		List<RSQSimEvent> events = study.getRSQSimCatalog().loader().minMag(6.5).skipSlipsAndTimes().skipYears(skipYears).load();
		List<CybershakeRun> runs = study.runFetcher().fetch();
		CybershakeIM im = CybershakeIM.getSA(CyberShakeComponent.RotD50, 3d);
		
		ReturnPeriods[] rps = ReturnPeriods.values();
		File ampsCacheDir = new File("/data/kevin/cybershake/amps_cache/");
		
		WindowedFractionalExceedanceCalculator calc = new WindowedFractionalExceedanceCalculator(
				events, study, runs, im, ampsCacheDir, rps);
		
		int numSamples = 1000000;
		double windowDuration = 50d;
		Random rand = new Random(events.size());
		
		ExceedanceResult[] samples = calc.getRandomFractExceedances(windowDuration, numSamples, rand);
		ExceedanceResult[] poissonSamples = calc.getShuffledRandomFractExceedances(windowDuration, numSamples, rand);
		
		
		for (boolean catalog : new boolean[] {true,false}) {
			for (int r=0; r<rps.length; r++) {
				for (boolean poisson : new boolean[] {false,true}) {
					ExceedanceResult result = poisson ? poissonSamples[r] : samples[r];
					double[] exceedFracts = catalog ? result.catalogFractSiteExceedances : result.siteFractExceedances;
					
					double mean = StatUtils.mean(exceedFracts);
					double median = DataUtils.median(exceedFracts);
					double min = StatUtils.min(exceedFracts);
					double max = StatUtils.max(exceedFracts);
					double p2p5 = StatUtils.percentile(exceedFracts, 2.5d);
					double p16 = StatUtils.percentile(exceedFracts, 16d);
					double p84 = StatUtils.percentile(exceedFracts, 84d);
					double p97p5 = StatUtils.percentile(exceedFracts, 97.5d);
					System.out.println(rps[r]+", "+(catalog ? "Catalog" : "Single-Site")+(poisson ? ", Poisson" : ""));
					System.out.println("\tMean:\t"+pDF.format(mean));
					System.out.println("\tMedian:\t"+pDF.format(median));
					System.out.println("\tRange:\t["+pDF.format(min)+", "+pDF.format(max)+"]");
					System.out.println("\t95% range:\t["+pDF.format(p2p5)+", "+pDF.format(p97p5)+"]");
					System.out.println("\t68% range:\t["+pDF.format(p16)+", "+pDF.format(p84)+"]");
				}
			}
		}
		
		study.getDB().destroy();
	}

}
