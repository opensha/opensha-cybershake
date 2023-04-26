package scratch.kevin.cybershake.pshaVariability;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.LightFixedXFunc;
import org.opensha.commons.geo.Location;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.calc.mcer.CyberShakeSiteRun;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.earthquake.faultSysSolution.util.SolHazardMapCalc.ReturnPeriods;
import org.opensha.sha.imr.param.IntensityMeasureParams.DurationTimeInterval;
import org.opensha.sha.simulators.RSQSimEvent;

import com.google.common.base.Preconditions;

import scratch.kevin.cybershake.pshaVariability.WindowedFractionalExceedanceCalculator.ExceedanceResult;
import scratch.kevin.simCompare.IMT;
import scratch.kevin.simCompare.SimulationRotDProvider;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimProbEqkRup;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimSectBundledSource;

public class CyberShakeExceedanceCalc {
	
	public static WindowedFractionalExceedanceCalculator forStudy(CyberShakeStudy study, CybershakeIM im,
			File ampsCacheDir, List<RSQSimEvent> events, ReturnPeriods[] rps) {
		List<CybershakeRun> runs = study.runFetcher().fetch();
		
		List<CyberShakeSiteRun> sites = CyberShakeSiteBuilder.buildSites(study, Vs30_Source.Simulation, runs);

		RSQSimSectBundledERF erf = (RSQSimSectBundledERF)study.buildNewERF();
		Map<Integer, int[]> eventIDtoSrcRups = new HashMap<>(events.size());
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			RSQSimSectBundledSource source = erf.getSource(sourceID);
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				RSQSimProbEqkRup rup = source.getRupture(rupID);
				int eventID = rup.getEventID();
				eventIDtoSrcRups.put(eventID, new int[] {sourceID, rupID});
			}
		}

		CachedPeakAmplitudesFromDB amps2DB = new CachedPeakAmplitudesFromDB(study.getDB(), ampsCacheDir, erf);

		List<RSQSimEvent> matchedEvents = new ArrayList<>(events.size());
		for (RSQSimEvent event : events)
			if (eventIDtoSrcRups.containsKey(event.getID()))
				matchedEvents.add(event);
		System.out.println("Retained "+matchedEvents.size()+"/"+events.size()+" events that exist in CS ERF");
		events = matchedEvents;
		
		SimRotDPRovider simProv = new SimRotDPRovider(matchedEvents, im, amps2DB, eventIDtoSrcRups);
		
		IMT imt = IMT.forPeriod(im.getVal());
		return new WindowedFractionalExceedanceCalculator(matchedEvents, simProv, sites, imt, rps);
	}
	
	private static class SimRotDPRovider implements SimulationRotDProvider<RSQSimEvent> {
		
		private double durationYears;
		private double[] x;
		private List<RSQSimEvent> events;
		private CybershakeIM im;
		private CachedPeakAmplitudesFromDB amps2db;
		private Map<Integer, int[]> eventIDtoSrcRups;
		
		public SimRotDPRovider(List<RSQSimEvent> events, CybershakeIM im, CachedPeakAmplitudesFromDB amps2db,
				Map<Integer, int[]> eventIDtoSrcRups) {
			this.events = events;
			this.im = im;
			this.amps2db = amps2db;
			this.eventIDtoSrcRups = eventIDtoSrcRups;
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
				ims = amps2db.getAllIM_Values(((CyberShakeSiteRun)site).getCS_Run().getRunID(), im);
			} catch (SQLException e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
			int[] srcRup = eventIDtoSrcRups.get(rupture.getID());
			if (srcRup == null || ims[srcRup[0]] == null || ims[srcRup[0]][srcRup[1]] == null)
				return null;
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
				double[][][] ims = amps2db.getAllIM_Values(((CyberShakeSiteRun)site).getCS_Run().getRunID(), im);
				
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

	public static void main(String[] args) throws IOException {
		CyberShakeStudy study = CyberShakeStudy.STUDY_22_3_RSQSIM_5413;
		int skipYears = 20000;
		List<RSQSimEvent> events = study.getRSQSimCatalog().loader().minMag(6.5).skipSlipsAndTimes().skipYears(skipYears).load();
		CybershakeIM im = CybershakeIM.getSA(CyberShakeComponent.RotD50, 3d);
		
		ReturnPeriods[] rps = ReturnPeriods.values();
		File ampsCacheDir = new File("/data/kevin/cybershake/amps_cache/");
		
		WindowedFractionalExceedanceCalculator calc = forStudy(study, im, ampsCacheDir, events, rps);
		
		int numSamples = 100000;
		double windowDuration = 50d;
		Random rand = new Random(events.size());
		
		ExceedanceResult[] samples = calc.getRandomFractExceedances(windowDuration, numSamples, rand);
		ExceedanceResult[] poissonSamples = calc.getShuffledRandomFractExceedances(windowDuration, numSamples, rand);
		
		for (boolean map : new boolean[] {true,false}) {
			for (int r=0; r<rps.length; r++) {
				for (boolean poisson : new boolean[] {false,true}) {
					ExceedanceResult result = poisson ? poissonSamples[r] : samples[r];
					double[] exceedFracts = map ? result.catalogFractSiteExceedances : result.siteFractExceedances;

					System.out.println(rps[r]+", "+(map ? "Map" : "Single-Site")+(poisson ? ", Poisson" : ""));
					WindowedFractionalExceedanceCalculator.printExceedStats(exceedFracts);
				}
			}
		}
		
		study.getDB().destroy();
	}

}
