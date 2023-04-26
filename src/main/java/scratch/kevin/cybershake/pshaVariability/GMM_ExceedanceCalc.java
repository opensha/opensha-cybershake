package scratch.kevin.cybershake.pshaVariability;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.apache.commons.math3.stat.StatUtils;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.LightFixedXFunc;
import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.geo.Location;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.calc.mcer.CyberShakeSiteRun;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.earthquake.faultSysSolution.util.SolHazardMapCalc.ReturnPeriods;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.attenRelImpl.ngaw2.NGAW2_WrapperFullParam;
import org.opensha.sha.imr.attenRelImpl.ngaw2.ScalarGroundMotion;
import org.opensha.sha.imr.param.IntensityMeasureParams.DurationTimeInterval;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;
import org.opensha.sha.simulators.RSQSimEvent;

import com.google.common.base.Preconditions;

import Jama.Matrix;
import scratch.kevin.cybershake.pshaVariability.WindowedFractionalExceedanceCalculator.ExceedanceResult;
import scratch.kevin.simCompare.IMT;
import scratch.kevin.simCompare.SimulationRotDProvider;
import scratch.kevin.simulators.RSQSimCatalog;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimProbEqkRup;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimSectBundledSource;
import scratch.kevin.spatialVar.SpatialVarCalc;

public class GMM_ExceedanceCalc {
	
	private static double SIGMA_TRUNCATION = 3;
	
	public static WindowedFractionalExceedanceCalculator forStudy(CyberShakeStudy study, RSQSimCatalog catalog, double period,
			List<RSQSimEvent> events, AttenRelRef gmmRef, ReturnPeriods[] rps, boolean randFields) throws IOException {
		List<CybershakeRun> runs = study.runFetcher().fetch();
		
		List<CyberShakeSiteRun> sites = CyberShakeSiteBuilder.buildSites(study, Vs30_Source.Simulation, runs);
		
		SpatialVarCalc randFieldCalc = null;
		if (randFields)
			randFieldCalc = new SpatialVarCalc(new double[] {period}, sites);
		
		Random rng = new Random(sites.size()*events.size());
		
		GMM_SimProv simProv = new GMM_SimProv(catalog, events, gmmRef, period, sites, randFieldCalc, rng);
		
		IMT imt = IMT.forPeriod(period);
		return new WindowedFractionalExceedanceCalculator(simProv.events, simProv, sites, imt, rps);
	}
	
	private static class GMM_SimProv implements SimulationRotDProvider<RSQSimEvent> {
		
		private double durationYears;
		private double[] x;
		private AttenRelRef gmmRef;
		private double period;
		private List<RSQSimEvent> events;
		private List<? extends Site> sites;
		private Random rng;
		
		private double maxSourceDist = 200d;
		
		private RSQSimSectBundledERF erf;
		private Map<Integer, RSQSimEvent> eventIDMap;
		private Map<Integer, RSQSimProbEqkRup> eventIDRupMap;
		private SpatialVarCalc randFieldCalc;
		
		private Map<Integer, GeoDataSet> eventShakeMaps;
		
		public GMM_SimProv(RSQSimCatalog catalog, List<RSQSimEvent> events, AttenRelRef gmmRef, double period,
				List<? extends Site> sites, SpatialVarCalc randFieldCalc, Random rng) throws IOException {
			this.gmmRef = gmmRef;
			this.period = period;
			this.sites = sites;
			this.randFieldCalc = randFieldCalc;
			this.rng = rng;
			x = new double[] {period};
			
			double minFractForInclusion = 0.2;
			double srfPointCullDist = 100;
			double dt = 0.1d;
			
			// doesn't matter, just for meta that we won't use
			double minMag = 0d;
			double skipYears = 20000;
			
			eventIDMap = new HashMap<>();
			for (RSQSimEvent event : events)
				eventIDMap.put(event.getID(), event);
			durationYears = events.get(events.size()-1).getTimeInYears() - events.get(0).getTimeInYears();
			
			erf = new RSQSimSectBundledERF(catalog.getElements(), events, catalog.getFaultModel(),
					catalog.getDeformationModel(), catalog.getSubSects(), minMag, minFractForInclusion,
					srfPointCullDist, dt, skipYears);
			erf.updateForecast();
			
			List<RSQSimEvent> filteredEvents = new ArrayList<>();
			
			eventIDRupMap = new HashMap<>();
			for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
				RSQSimSectBundledSource source = erf.getSource(sourceID);
//				boolean includeSource = false;
//				for (Site site : sites) {
//					if (source.getMinDistance(site) <= maxSourceDist) {
//						includeSource = true;
//						break;
//					}
//				}
//				if (!includeSource)
//					break;
				for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
					RSQSimProbEqkRup rup = source.getRupture(rupID);
					boolean includeRup = false;
					for (Site site : sites) {
						if (rup.getRuptureSurface().getQuickDistance(site.getLocation()) <= maxSourceDist) {
							includeRup = true;
							break;
						}
					}
					eventIDRupMap.put(rup.getEventID(), rup);
					if (includeRup)
						filteredEvents.add(eventIDMap.get(rup.getEventID()));
				}
			}
			Collections.sort(filteredEvents);
			this.events = filteredEvents;
			System.out.println("Filtered down to "+filteredEvents.size()+"/"+events.size()+" events");
		}
		
		public void calcShakeMaps() {
			System.out.println("Calculating GMM shakemaps for "+events.size()+" events and "+sites.size()+" sites");
			
			List<CompletableFuture<ScalarGroundMotion[]>> siteGroundMotionFutures = new ArrayList<>();
			for (int s=0; s<sites.size(); s++) {
				ScalarIMR gmm = gmmRef.get();
				gmm.setIntensityMeasure(SA_Param.NAME);
				SA_Param.setPeriodInSA_Param(gmm.getIntensityMeasure(), period);
				gmm.setSite(sites.get(s));
				Preconditions.checkState(gmm instanceof NGAW2_WrapperFullParam);
				NGAW2_WrapperFullParam ngaGMM = (NGAW2_WrapperFullParam)gmm;
				siteGroundMotionFutures.add(CompletableFuture.supplyAsync(new Supplier<ScalarGroundMotion[]>() {

					@Override
					public ScalarGroundMotion[] get() {
						ScalarGroundMotion[] ret = new ScalarGroundMotion[events.size()];
						for (int i=0; i<events.size(); i++) {
							RSQSimEvent event = events.get(i);
							RSQSimProbEqkRup rup = eventIDRupMap.get(event.getID());
							ngaGMM.setEqkRupture(rup);
							ret[i] = ngaGMM.getGroundMotion();
						}
						return ret;
					}
				}));
			}
			
			ArbDiscrGeoDataSet emptyMap = new ArbDiscrGeoDataSet(false);
			for (Site site : sites)
				emptyMap.set(site.getLocation(), 0d);
			List<GeoDataSet> maps = new ArrayList<>();
			
			for (int i=0; i<events.size(); i++)
				maps.add(emptyMap.copy());
			
			// now fill them in
			double[] avgSigmas = new double[events.size()];
			double[] avgPhis = new double[events.size()];
			double[] avgTaus = new double[events.size()];
			for (int s=0; s<sites.size(); s++) {
				ScalarGroundMotion[] siteGMs = siteGroundMotionFutures.get(s).join();
				for (int i=0; i<events.size(); i++) {
					maps.get(i).set(s, Math.exp(siteGMs[i].mean()));
					avgSigmas[i] += siteGMs[i].stdDev();
					avgPhis[i] += siteGMs[i].phi();
					avgTaus[i] += siteGMs[i].tau();
				}
			}
			for (int i=0; i<events.size(); i++) {
				avgSigmas[i] /= sites.size();
				avgPhis[i] /= sites.size();
				avgTaus[i] /= sites.size();
			}
			double avgSigma = StatUtils.mean(avgSigmas);
			System.out.println("GMM sigma: avg="+(float)avgSigma+", range=["
					+(float)StatUtils.min(avgSigmas)+", "+(float)StatUtils.max(avgSigmas)+"]");
			double avgPhi = StatUtils.mean(avgPhis);
			System.out.println("GMM phi: avg="+(float)avgPhi+", range=["
					+(float)StatUtils.min(avgPhis)+", "+(float)StatUtils.max(avgPhis)+"]");
			double avgTau = StatUtils.mean(avgTaus);
			System.out.println("GMM tau: avg="+(float)avgTau+", range=["
					+(float)StatUtils.min(avgTaus)+", "+(float)StatUtils.max(avgTaus)+"]");
			for (int i=0; i<events.size(); i++) {
				// now add a random sigma
				double gaussian = rng.nextGaussian();
				if (gaussian >= 0d)
					gaussian = Math.min(gaussian, SIGMA_TRUNCATION);
				else
					gaussian = Math.max(gaussian, -SIGMA_TRUNCATION);
				double randSigma;
				if (randFieldCalc != null)
					// we're adding random fields, just add tau as we'll add within later
					randSigma = gaussian*avgTaus[i];
				else
					// no random fields, draw a sigma for the whole thing
					randSigma = gaussian*avgSigmas[i];
				
				GeoDataSet map = maps.get(i);
				for (int s=0; s<sites.size(); s++) {
					double orig = map.get(s);
					map.set(s, Math.exp(Math.log(orig) + randSigma));
				}
			}
			
			if (randFieldCalc != null) {
				System.out.println("Calculating "+events.size()+" random fields");
				
				System.out.println("Using average phi="+(float)avgPhi);
				
				Matrix[] fields = randFieldCalc.computeRandWithinEventResiduals(rng, avgPhi, events.size());
				
				double truncation = avgPhi*SIGMA_TRUNCATION;
				
				System.out.println("Done building, merging in "+events.size()+" fields");
				for (int i=0; i<events.size(); i++)
					maps.set(i, randFieldCalc.calcRandomShakeMap(maps.get(i), fields[i], 0, truncation));
			}
			System.out.println("DONE");
			Map<Integer, GeoDataSet> eventShakeMaps = new HashMap<>();
			for (int i=0; i<events.size(); i++)
				eventShakeMaps.put(events.get(i).getID(), maps.get(i));
			this.eventShakeMaps = eventShakeMaps;
		}

		@Override
		public String getName() {
			return "GMM";
		}

		@Override
		public DiscretizedFunc getRotD50(Site site, RSQSimEvent rupture, int index) throws IOException {
			if (eventShakeMaps == null) {
				synchronized (this) {
					if (eventShakeMaps == null)
						calcShakeMaps();
				}
			}
			GeoDataSet map = eventShakeMaps.get(rupture.getID());
			Preconditions.checkNotNull(map);
			double[] y = { map.get(site.getLocation()) };
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
			List<RSQSimEvent> rups = new ArrayList<>();
			for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
				RSQSimSectBundledSource source = erf.getSource(sourceID);
				
//				if (source.getMinDistance(site) > maxSourceDist)
//					continue;
				for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
					RSQSimProbEqkRup rup = source.getRupture(rupID);
					if (rup.getRuptureSurface().getQuickDistance(site.getLocation()) <= maxSourceDist)
						rups.add(eventIDMap.get(rup.getEventID()));
				}
			}
			return rups;
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
		double skipYears = 20000;
//		double period = 3d;
		double period = 0.1;
		double minMag = 6d;
		AttenRelRef gmmRef = AttenRelRef.ASK_2014;
		ReturnPeriods[] rps = ReturnPeriods.values();
		boolean randFields = true;
		
		RSQSimCatalog catalog = study.getRSQSimCatalog();
		List<RSQSimEvent> events = catalog.loader().minMag(minMag).skipYears(skipYears).load();
		System.out.println("Loaded "+events.size()+" events");
		
		WindowedFractionalExceedanceCalculator calc = forStudy(study, catalog, period, events, gmmRef, rps, randFields);
		
		study.getDB().destroy();
		
		int numSamples = 100000;
		double windowDuration = 50d;
		Random rand = new Random(events.size());
		
		ExceedanceResult[] samples = calc.getRandomFractExceedances(windowDuration, numSamples, rand);
		ExceedanceResult[] poissonSamples = calc.getShuffledRandomFractExceedances(windowDuration, numSamples, rand);
		
		System.out.println("DONE, randFields="+randFields);
		
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
	}

}
