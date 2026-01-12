package scratch.kevin.cybershake.pshaVariability;

import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import org.apache.commons.math3.stat.StatUtils;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.IntegerPDF_FunctionSampler;
import org.opensha.commons.data.function.LightFixedXFunc;
import org.opensha.commons.data.function.XY_DataSet;
import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.FixedLocationArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.data.xyz.GriddedGeoDataSet;
import org.opensha.commons.geo.BorderType;
import org.opensha.commons.geo.GriddedRegion;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.geo.Region;
import org.opensha.commons.mapping.PoliticalBoundariesData;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.calc.mcer.CyberShakeSiteRun;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.earthquake.faultSysSolution.FaultSystemSolution;
import org.opensha.sha.earthquake.faultSysSolution.modules.GridSourceProvider;
import org.opensha.sha.earthquake.faultSysSolution.util.SolHazardMapCalc.ReturnPeriods;
import org.opensha.sha.earthquake.observedEarthquake.ObsEqkRupList;
import org.opensha.sha.earthquake.observedEarthquake.ObsEqkRupture;
import org.opensha.sha.earthquake.param.IncludeBackgroundOption;
import org.opensha.sha.earthquake.param.IncludeBackgroundParam;
import org.opensha.sha.earthquake.param.ProbabilityModelOptions;
import org.opensha.sha.earthquake.param.ProbabilityModelParam;
import org.opensha.sha.earthquake.rupForecastImpl.nshm23.logicTree.NSHM23_DeclusteringAlgorithms;
import org.opensha.sha.earthquake.rupForecastImpl.nshm23.logicTree.NSHM23_SeisSmoothingAlgorithms;
import org.opensha.sha.earthquake.rupForecastImpl.nshm23.util.NSHM23_RegionLoader.SeismicityRegions;
import org.opensha.sha.faultSurface.PointSurface;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.attenRelImpl.ngaw2.NGAW2_WrapperFullParam;
import org.opensha.sha.imr.attenRelImpl.ngaw2.ScalarGroundMotion;
import org.opensha.sha.imr.param.IntensityMeasureParams.DurationTimeInterval;
import org.opensha.sha.imr.param.IntensityMeasureParams.PGA_Param;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;
import org.opensha.sha.magdist.GutenbergRichterMagFreqDist;
import org.opensha.sha.magdist.IncrementalMagFreqDist;
import org.opensha.sha.simulators.RSQSimEvent;
import org.opensha.sha.simulators.utils.RSQSimUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Doubles;

import Jama.Matrix;
import scratch.UCERF3.erf.FaultSystemSolutionERF;
import scratch.UCERF3.erf.utils.ProbabilityModelsCalc;
import scratch.kevin.cybershake.pshaVariability.WindowedFractionalExceedanceCalculator.ExceedanceResult;
import scratch.kevin.simCompare.IMT;
import scratch.kevin.simCompare.SimulationRotDProvider;
import scratch.kevin.simulators.RSQSimCatalog;
import scratch.kevin.simulators.RSQSimCatalog.Catalogs;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimProbEqkRup;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimSectBundledSource;
import scratch.kevin.spatialVar.SpatialVarCalc;

public class GMM_ExceedanceCalc {
	
	private static double SIGMA_TRUNCATION = 3;
	
	public static WindowedFractionalExceedanceCalculator<ObsEqkRupture> forRSQSim(List<? extends Site> sites, RSQSimCatalog catalog, double period,
			List<RSQSimEvent> events, AttenRelRef gmmRef, ReturnPeriods[] rps, boolean randFields) throws IOException {
		
		SpatialVarCalc randFieldCalc = null;
		if (randFields) {
			List<Location> siteLocs = new ArrayList<>(sites.size());
			for (Site site : sites)
				siteLocs.add(site.getLocation());
			randFieldCalc = new SpatialVarCalc(new double[] {period}, siteLocs);
		}
		
		Random rng = new Random(sites.size()*events.size());
		
		// params
		double minFractForInclusion = 0.2;
		double srfPointCullDist = 100;
		double maxSourceDist = 200d;
		// these don't matter, just for meta that we won't use
		double minMag = 0d;
		double skipYears = 20000;
		double dt = 0.1d;
		
		Map<Integer, RSQSimEvent> idToEventMap = new HashMap<>();
		for (RSQSimEvent event : events)
			idToEventMap.put(event.getID(), event);
		
		RSQSimSectBundledERF erf = new RSQSimSectBundledERF(catalog.getElements(), events, catalog.getFaultModel(),
				catalog.getDeformationModel(), catalog.getSubSects(), minMag, minFractForInclusion,
				srfPointCullDist, dt, skipYears);
		erf.updateForecast();
		
		ObsEqkRupList obsRups = new ObsEqkRupList();
		
		double firstEventTimeYears = events.get(0).getTimeInYears();
		
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			RSQSimSectBundledSource source = erf.getSource(sourceID);
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				RSQSimProbEqkRup rup = source.getRupture(rupID);
				boolean includeRup = false;
				for (Site site : sites) {
					if (rup.getRuptureSurface().getQuickDistance(site.getLocation()) <= maxSourceDist) {
						includeRup = true;
						break;
					}
				}
				if (includeRup) {
					RSQSimEvent event = idToEventMap.get(rup.getEventID());
					double deltaTimeYears = event.getTimeInYears() - firstEventTimeYears;
					long epochMillis = (long)(deltaTimeYears*ProbabilityModelsCalc.MILLISEC_PER_YEAR + 0.5);
					
					ObsEqkRupture obsRup = new ObsEqkRupture(event.getID()+"", epochMillis,
							RSQSimUtils.getHypocenter(event), event.getMagnitude());
					obsRup.setRuptureSurface(rup.getRuptureSurface());
					
					obsRups.add(obsRup);
				}
			}
		}
		
		obsRups.sortByOriginTime();
		System.out.println("Filtered down to "+obsRups.size()+"/"+events.size()+" events");
		
		GMM_SimProv simProv = new GMM_SimProv(obsRups, gmmRef, period, sites, randFieldCalc, rng);
		
		IMT imt = IMT.forPeriod(period);
		return new WindowedFractionalExceedanceCalculator.ObsRup(obsRups, simProv, sites, imt, rps);
	}
	
	public static WindowedFractionalExceedanceCalculator<ObsEqkRupture> forSourceRegion(GriddedRegion siteReg,
			GriddedRegion sourceReg, IncrementalMagFreqDist mfd, double durationYears, AttenRelRef gmmRef,
			double period, ReturnPeriods[] rps, boolean randFields, double[] seisPDF) {
		double totRate = mfd.getTotalIncrRate();
		int numEvents = (int)(durationYears*totRate + 0.5);
		System.out.println("Creating "+numEvents+" events in "+(float)durationYears+" for totRate="+(float)totRate);
		double[] mfdRates = new double[mfd.size()];
		for (int i=0; i<mfdRates.length; i++)
			mfdRates[i] = mfd.getY(i);
		
		Random rng = new Random(numEvents*sourceReg.getNodeCount());
		
		IntegerPDF_FunctionSampler magIndexSampler = new IntegerPDF_FunctionSampler(mfdRates);
		double[] randMags = new double[numEvents];
		for (int i=0; i<numEvents; i++) {
			int magIndex = magIndexSampler.getRandomInt(rng);
			randMags[i] = mfd.getX(magIndex);
		}
		
		IntegerPDF_FunctionSampler pdfSampler = null;
		if (seisPDF != null) {
			Preconditions.checkState(seisPDF.length == sourceReg.getNodeCount());
			pdfSampler = new IntegerPDF_FunctionSampler(seisPDF);
		}
		
		// now build them with random locations and times
		ObsEqkRupList obsRups = new ObsEqkRupList();
		for (int i=0; i<numEvents; i++) {
			String id = i+"";
			double mag = randMags[i];
			
			int locIndex;
			if (seisPDF == null) {
				locIndex = rng.nextInt(sourceReg.getNodeCount());
			} else {
				locIndex = pdfSampler.getRandomInt(rng);
			}
			Location loc = sourceReg.getLocation(locIndex);
			
			long timeMillis = (long)(rng.nextDouble()*durationYears*ProbabilityModelsCalc.MILLISEC_PER_YEAR + 0.5);
			
			ObsEqkRupture rup = new ObsEqkRupture(id, timeMillis, loc, mag);
			rup.setAveRake(0d);
			PointSurface surf = new PointSurface(loc);
			surf.setAveStrike(0d);
			surf.setAveDip(90d);
			if (mag > 6.5d) {
				surf.setAveWidth(12d);
				surf.setDepth(0d);
			} else {
				surf.setAveWidth(5d);
				surf.setDepth(5d);
			}
			rup.setRuptureSurface(surf);
			
			obsRups.add(rup);
		}
		obsRups.sortByOriginTime();
		
		// now build sites
		ScalarIMR gmm = gmmRef.get();
		List<Site> sites = new ArrayList<>();
		for (int i=0; i<siteReg.getNodeCount(); i++) {
			Site site = new Site(siteReg.getLocation(i));
			site.setName("Site "+i);
			site.addParameterList(gmm.getSiteParams());
			sites.add(site);
		}
		
		SpatialVarCalc randFieldCalc = null;
		if (randFields) {
			List<Location> siteLocs = new ArrayList<>(sites.size());
			for (Site site : sites)
				siteLocs.add(site.getLocation());
			randFieldCalc = new SpatialVarCalc(new double[] {period}, siteLocs);
		}
		
		GMM_SimProv simProv = new GMM_SimProv(obsRups, gmmRef, period, sites, randFieldCalc, rng);
		
		IMT imt = IMT.forPeriod(period);
		return new WindowedFractionalExceedanceCalculator.ObsRup(obsRups, simProv, sites, imt, rps);
	}
	
	public static WindowedFractionalExceedanceCalculator<ObsEqkRupture> forFSS(GriddedRegion siteReg,
			FaultSystemSolution sol, double durationYears, AttenRelRef gmmRef,
			double period, ReturnPeriods[] rps, boolean randFields, boolean filterRups, boolean randomizeSiteLocs) {
		System.out.println("Building ERF");
		FaultSystemSolutionERF erf = new FaultSystemSolutionERF(sol);
		erf.setParameter(IncludeBackgroundParam.NAME, IncludeBackgroundOption.INCLUDE);
		erf.setParameter(ProbabilityModelParam.NAME, ProbabilityModelOptions.POISSON);
		erf.getTimeSpan().setDuration(1d);
		erf.updateForecast();
		
		double totRate = 0d;
		
		List<Double> rupRates = new ArrayList<>(erf.getTotNumRups());
		
		if (filterRups) {
			GriddedRegion downsampled = new GriddedRegion(siteReg, 1d, GriddedRegion.ANCHOR_0_0);
			List<Site> downsampledSites = new ArrayList<>();
			for (int i=0; i<downsampled.getNodeCount(); i++)
				downsampledSites.add(new Site(downsampled.getLocation(i)));
			
			double includeDist = 300d;
			int numKept = 0;
			
			for (ProbEqkSource source : erf) {
				boolean include = false;
				for (Site site : downsampledSites) {
					if (source.getMinDistance(site) <= includeDist) {
						include = true;
						break;
					}
				}
				for (ProbEqkRupture rup : source) {
					double rate = 0d;
					if (include) {
						rate = rup.getMeanAnnualRate(1d);
						numKept++;
					}
					rupRates.add(rate);
					totRate += rate;
				}
			}
			
			System.out.println("Kept "+numKept+" ("+pDF.format((double)numKept/(double)rupRates.size())
				+") ruptures within "+oDF.format(includeDist)+" of downsampled site reg.");
		} else {
			for (int i=0; i<erf.getTotNumRups(); i++) {
				double rate = erf.getNthRupture(i).getMeanAnnualRate(1d);
				rupRates.add(rate);
				totRate += rate;
			}
		}
		
		int numRups = rupRates.size();
		
		IntegerPDF_FunctionSampler rupSampler = new IntegerPDF_FunctionSampler(Doubles.toArray(rupRates));
		
		int numEvents = (int)(durationYears*totRate + 0.5);
		System.out.println("Creating "+numEvents+" events in "+(float)durationYears+" for totRate="+(float)totRate);
		
		Random rng = new Random(numEvents*numRups);
		
		// now build them with random locations and times
		ObsEqkRupList obsRups = new ObsEqkRupList();
		for (int i=0; i<numEvents; i++) {
			String id = i+"";
			int index = rupSampler.getRandomInt(rng);
			
			ProbEqkRupture probRup = erf.getNthRupture(index);
			
			long timeMillis = (long)(rng.nextDouble()*durationYears*ProbabilityModelsCalc.MILLISEC_PER_YEAR + 0.5);
			
			ObsEqkRupture rup = new ObsEqkRupture(id, timeMillis, probRup.getHypocenterLocation(), probRup.getMag());
			rup.setAveRake(probRup.getAveRake());
			rup.setRuptureSurface(probRup.getRuptureSurface());
			
			obsRups.add(rup);
		}
		obsRups.sortByOriginTime();
		
		// now build sites
		ScalarIMR gmm = gmmRef.get();
		List<Site> sites = new ArrayList<>();
		for (int i=0; i<siteReg.getNodeCount(); i++) {
			Location loc = siteReg.getLocation(i);
			if (randomizeSiteLocs) {
				double latSpacing = siteReg.getLatSpacing();
				double lonSpacing = siteReg.getLonSpacing();
				double lat = loc.getLatitude() + latSpacing*(rng.nextDouble()-0.5);
				double lon = loc.getLongitude() + lonSpacing*(rng.nextDouble()-0.5);
				loc = new Location(latSpacing, lonSpacing);
			}
			Site site = new Site(loc);
			site.setName("Site "+i);
			site.addParameterList(gmm.getSiteParams());
			sites.add(site);
		}
		
		SpatialVarCalc randFieldCalc = null;
		if (randFields) {
			List<Location> siteLocs = new ArrayList<>(sites.size());
			for (Site site : sites)
				siteLocs.add(site.getLocation());
			randFieldCalc = new SpatialVarCalc(new double[] {period}, siteLocs);
		}
		
		GMM_SimProv simProv = new GMM_SimProv(obsRups, gmmRef, period, sites, randFieldCalc, rng);
		
		simProv.calcShakeMaps();
		
		IMT imt = IMT.forPeriod(period);
		return new WindowedFractionalExceedanceCalculator.ObsRup(obsRups, simProv, sites, imt, rps);
	}
	
	private static class GMM_SimProv implements SimulationRotDProvider<ObsEqkRupture> {
		
		private double[] x;
		private AttenRelRef gmmRef;
		private double period;
		private List<? extends ObsEqkRupture> events;
		private List<? extends Site> sites;
		private Random rng;
		
		private SpatialVarCalc randFieldCalc;
		
		private Map<String, GeoDataSet> eventShakeMaps;
		private double durationYears;
		
		public GMM_SimProv(List<? extends ObsEqkRupture> events, AttenRelRef gmmRef, double period,
				List<? extends Site> sites, SpatialVarCalc randFieldCalc, Random rng) {
			this.gmmRef = gmmRef;
			this.period = period;
			this.sites = sites;
			this.randFieldCalc = randFieldCalc;
			this.rng = rng;
			this.events = events;
			long firstMillis = events.get(0).getOriginTime();
			long lastMillis = events.get(events.size()-1).getOriginTime();
			long deltaMillis = lastMillis - firstMillis;
			this.durationYears = (double)deltaMillis/ProbabilityModelsCalc.MILLISEC_PER_YEAR;
			x = new double[] {period};
		}
		
		public void calcShakeMaps() {
			System.out.println("Calculating GMM shakemaps for "+events.size()+" events and "+sites.size()+" sites");
			
			List<CompletableFuture<ScalarGroundMotion[]>> siteGroundMotionFutures = new ArrayList<>();
			for (int s=0; s<sites.size(); s++) {
				ScalarIMR gmm = gmmRef.get();
				if (period > 0d) {
					gmm.setIntensityMeasure(SA_Param.NAME);
					SA_Param.setPeriodInSA_Param(gmm.getIntensityMeasure(), period);
				} else {
					Preconditions.checkState(period == 0d);
					gmm.setIntensityMeasure(PGA_Param.NAME);
				}
				gmm.setSite(sites.get(s));
				Preconditions.checkState(gmm instanceof NGAW2_WrapperFullParam);
				NGAW2_WrapperFullParam ngaGMM = (NGAW2_WrapperFullParam)gmm;
				siteGroundMotionFutures.add(CompletableFuture.supplyAsync(new Supplier<ScalarGroundMotion[]>() {

					@Override
					public ScalarGroundMotion[] get() {
						ScalarGroundMotion[] ret = new ScalarGroundMotion[events.size()];
						for (int i=0; i<events.size(); i++) {
							ObsEqkRupture event = events.get(i);
							ngaGMM.setEqkRupture(event);
							ret[i] = ngaGMM.getGroundMotion();
						}
						return ret;
					}
				}));
			}
			
			System.out.println("Initializing shakemaps...");
			ImmutableList.Builder<Location> siteLocBuilder = ImmutableList.builderWithExpectedSize(sites.size());
			ImmutableMap.Builder<Location, Integer> siteLocIndexMapBuilder = ImmutableMap.builderWithExpectedSize(sites.size());
			for (int s=0; s<sites.size(); s++) {
				Location loc = sites.get(s).getLocation();
				siteLocIndexMapBuilder.put(loc, s);
				siteLocBuilder.add(loc);
			}
			ImmutableList<Location> siteLocs = siteLocBuilder.build();
			ImmutableMap<Location, Integer> siteLocIndexMap = siteLocIndexMapBuilder.build();
			
			List<GeoDataSet> maps = new ArrayList<>();
			
			for (int i=0; i<events.size(); i++)
				maps.add(new FixedLocationArbDiscrGeoDataSet(siteLocs, siteLocIndexMap));
			
			System.out.println("Waiting on ground motion calculations and filling in maps...");
			// now fill them in
			double[] avgSigmas = new double[events.size()];
			double[] avgPhis = new double[events.size()];
			double[] avgTaus = new double[events.size()];
			for (int s=0; s<sites.size(); s++) {
				ScalarGroundMotion[] siteGMs = siteGroundMotionFutures.get(s).join();
				
				System.out.println("Done with site "+s+"/"+sites.size());
				for (int i=0; i<events.size(); i++) {
					maps.get(i).set(s, Math.exp(siteGMs[i].mean()));
					avgSigmas[i] += siteGMs[i].stdDev();
					avgPhis[i] += siteGMs[i].phi();
					avgTaus[i] += siteGMs[i].tau();
				}
				// clear it from memory
				siteGroundMotionFutures.set(s, null);
			}
			siteGroundMotionFutures = null;
			System.gc();
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
			System.out.println("Adding random between-event variability...");
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
				
				List<CompletableFuture<Matrix[]>> fieldFutures = new ArrayList<>();
				int numCalculating = 0;
				while (numCalculating < events.size()) {
					int bundleSize = Integer.min(1000, events.size()-numCalculating);
					
					Random subRand = new Random(rng.nextLong());
					
					fieldFutures.add(CompletableFuture.supplyAsync(new Supplier<Matrix[]>() {

						@Override
						public Matrix[] get() {
							return randFieldCalc.computeRandWithinEventResiduals(subRand, avgPhi, bundleSize);
						}
					}));
					
					numCalculating += bundleSize;
				}
				
				double truncation = avgPhi*SIGMA_TRUNCATION;
				
				int mapIndex = 0;
				for (CompletableFuture<Matrix[]> future : fieldFutures) {
					Matrix[] fields = future.join();
					for (int i=0; i<fields.length; i++) {
						maps.set(mapIndex, randFieldCalc.calcRandomShakeMap(maps.get(i), fields[i], 0, truncation));
						mapIndex++;
					}
					System.out.println("Done with "+mapIndex+"/"+events.size()+" fields");
				}
			}
			System.out.println("DONE");
			Map<String, GeoDataSet> eventShakeMaps = new HashMap<>();
			for (int i=0; i<events.size(); i++) {
				ObsEqkRupture event = events.get(i);
				String id = event.getEventId();
				Preconditions.checkState(id != null && !id.isBlank(), "ObsEqkRupture eventID not populated: %s", id);
				Preconditions.checkState(!eventShakeMaps.containsKey(id), "ObsEqkRupture eventID not unique: %s", id);
				eventShakeMaps.put(id, maps.get(i));
			}
			this.eventShakeMaps = eventShakeMaps;
		}

		@Override
		public String getName() {
			return "GMM";
		}

		@Override
		public DiscretizedFunc getRotD50(Site site, ObsEqkRupture rupture, int index) throws IOException {
			Preconditions.checkState(period > 0d);
			double y = doLoad(site, rupture);
			double[] ys = { y };
			return new LightFixedXFunc(x, ys);
		}
		
		@Override
		public double getPGA(Site site, ObsEqkRupture rupture, int index) throws IOException {
			Preconditions.checkState(period == 0d);
			return doLoad(site, rupture);
		}
		
		private double doLoad(Site site, ObsEqkRupture rupture) {
			if (eventShakeMaps == null) {
				synchronized (this) {
					if (eventShakeMaps == null) {
						try {
							calcShakeMaps();
						} catch (Throwable t) {
							t.printStackTrace();
							System.err.flush();
							System.exit(1);
						}
					}
				}
			}
			GeoDataSet map = eventShakeMaps.get(rupture.getEventId());
			Preconditions.checkNotNull(map);
			return map.get(site.getLocation());
		}

		@Override
		public DiscretizedFunc getRotD100(Site site, ObsEqkRupture rupture, int index) throws IOException {
			throw new IllegalStateException();
		}

		@Override
		public DiscretizedFunc[] getRotD(Site site, ObsEqkRupture rupture, int index) throws IOException {
			throw new IllegalStateException();
		}

		@Override
		public DiscretizedFunc getRotDRatio(Site site, ObsEqkRupture rupture, int index) throws IOException {
			throw new IllegalStateException();
		}

		@Override
		public double getPGV(Site site, ObsEqkRupture rupture, int index) throws IOException {
			throw new IllegalStateException();
		}

		@Override
		public double getDuration(Site site, ObsEqkRupture rupture, DurationTimeInterval interval, int index)
				throws IOException {
			throw new IllegalStateException();
		}

		@Override
		public int getNumSimulations(Site site, ObsEqkRupture rupture) {
			return 1;
		}

		@Override
		public Location getHypocenter(ObsEqkRupture rupture, int index) {
			return rupture.getHypocenterLocation();
		}

		@Override
		public Collection<? extends ObsEqkRupture> getRupturesForSite(Site site) {
			return events;
		}

		@Override
		public boolean hasRotD50() {
			return period > 0d;
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
		public boolean hasPGA() {
			return period == 0d;
		}

		@Override
		public boolean hasDurations() {
			return false;
		}

		@Override
		public double getAnnualRate(ObsEqkRupture rupture) {
			return 1d/durationYears;
		}

		@Override
		public double getMinimumCurvePlotRate(Site site) {
			return getAnnualRate(null);
		}

		@Override
		public double getMagnitude(ObsEqkRupture rupture) {
			return rupture.getMag();
		}

		@Override
		public double getRake(ObsEqkRupture rupture) {
			return rupture.getAveRake();
		}
		
	}
	
	private static WindowedFractionalExceedanceCalculator<ObsEqkRupture> initRSQSimCyberShakeMap(AttenRelRef gmmRef, double period,
			ReturnPeriods[] rps, boolean randFields) throws IOException {
		CyberShakeStudy study = CyberShakeStudy.STUDY_22_3_RSQSIM_5413;
		double skipYears = 20000;
		double minMag = 6d;
		
		RSQSimCatalog catalog = study.getRSQSimCatalog();
		List<RSQSimEvent> events = catalog.loader().minMag(minMag).skipYears(skipYears).load();
		System.out.println("Loaded "+events.size()+" events");
		
		List<CybershakeRun> runs = study.runFetcher().fetch();
		
		List<CyberShakeSiteRun> sites = CyberShakeSiteBuilder.buildSites(study, Vs30_Source.Simulation, runs);
		
		study.getDB().destroy();
		
		WindowedFractionalExceedanceCalculator<ObsEqkRupture> calc = forRSQSim(sites, catalog, period, events, gmmRef, rps, randFields);
		
		return calc;
	}
	
	private static WindowedFractionalExceedanceCalculator<ObsEqkRupture> initRSQSimStatewide(AttenRelRef gmmRef, double period,
			ReturnPeriods[] rps, boolean randFields) throws IOException {
		RSQSimCatalog catalog = Catalogs.BRUCE_5413.instance();
		double skipYears = 20000;
		double minMag = 6d;
		
		List<RSQSimEvent> events = catalog.loader().minMag(minMag).skipYears(skipYears).load();
		System.out.println("Loaded "+events.size()+" events");
		
		double siteSpacing = 0.5;
		GriddedRegion siteRegion = new GriddedRegion(getMainlandCA(), siteSpacing, GriddedRegion.ANCHOR_0_0);
		System.out.println(siteRegion.getNodeCount()+" sites");
		ScalarIMR gmm = gmmRef.get();
		List<Site> sites = new ArrayList<>();
		for (int i=0; i<siteRegion.getNodeCount(); i++) {
			Site site = new Site(siteRegion.getLocation(i));
			site.setName("Site "+i);
			site.addParameterList(gmm.getSiteParams());
			sites.add(site);
		}
		
		WindowedFractionalExceedanceCalculator<ObsEqkRupture> calc = forRSQSim(sites, catalog, period, events, gmmRef, rps, randFields);
		
		return calc;
	}
	
	private static WindowedFractionalExceedanceCalculator<ObsEqkRupture> initUniform(AttenRelRef gmmRef, double period,
			ReturnPeriods[] rps, boolean randFields, boolean useSeisPDF) throws IOException {
//		GutenbergRichterMagFreqDist mfd = new GutenbergRichterMagFreqDist(6.05d, 20, 0.1);
//		double totRate = 0.5d;
//		double bVal = 1d;
		
		GutenbergRichterMagFreqDist mfd = new GutenbergRichterMagFreqDist(4.05d, 31, 0.1);
		double totRate = 10d;
		double bVal = 0.97d;
		
		mfd.setAllButTotMoRate(mfd.getMinX(), mfd.getMaxX(), totRate, bVal);
		
		int targetNumEvents = 250000;
		double durationYears = targetNumEvents/totRate;
		
		Location regCenter = new Location(36, -117);
		double siteRadius = 150d;
		double sourceRadius = 300d;
		double siteSpacing = 0.1;
		double sourceSpacing = 0.05;
		
		GriddedRegion siteRegion = new GriddedRegion(regCenter, siteRadius, siteSpacing, GriddedRegion.ANCHOR_0_0);
		GriddedRegion sourceRegion = new GriddedRegion(regCenter, sourceRadius, sourceSpacing, GriddedRegion.ANCHOR_0_0);
		System.out.println(siteRegion.getNodeCount()+" sites and "+sourceRegion.getNodeCount()+" sources");
		
		double[] seisPDF = null;
		if (useSeisPDF) {
			SeismicityRegions seisReg = SeismicityRegions.CONUS_WEST;
			GriddedGeoDataSet rawPDF = NSHM23_SeisSmoothingAlgorithms.ADAPTIVE.loadXYZ(
					seisReg, NSHM23_DeclusteringAlgorithms.NN);
			seisPDF = new double[sourceRegion.getNodeCount()];
			for (int i=0; i<seisPDF.length; i++) {
				Location loc = sourceRegion.getLocation(i);
				int pdfRegIndex = rawPDF.indexOf(loc);
				Preconditions.checkState(pdfRegIndex >= 0);
				seisPDF[i] = rawPDF.get(pdfRegIndex);
			}
		}
		
		return forSourceRegion(siteRegion, sourceRegion, mfd, durationYears, gmmRef, period, rps, randFields, seisPDF);
	}
	
	private static Region getMainlandCA() throws IOException {
		XY_DataSet[] caOutlines = PoliticalBoundariesData.loadCAOutlines();
		
		Region largest = null;
		for (XY_DataSet outlineXY : caOutlines) {
			LocationList outline = new LocationList();
			for (Point2D pt : outlineXY)
				outline.add(new Location(pt.getY(), pt.getX()));
			Region region = new Region(outline, BorderType.MERCATOR_LINEAR);
			if (largest == null || region.getExtent() > largest.getExtent())
				largest = region;
		}
		
		return largest;
	}
	
	private static WindowedFractionalExceedanceCalculator<ObsEqkRupture> initU3(AttenRelRef gmmRef, double period,
			ReturnPeriods[] rps, boolean randFields) throws IOException {
		FaultSystemSolution sol = FaultSystemSolution.load(new File("/home/kevin/OpenSHA/UCERF3/rup_sets/modular/FM3_1_branch_averaged.zip"));
		
		double durationYears = 100000;
		double siteSpacing = 0.2;
		boolean filter = false;
		boolean randomizeSiteLocs = false;
		
		GriddedRegion siteRegion = new GriddedRegion(getMainlandCA(), siteSpacing, GriddedRegion.ANCHOR_0_0);
		System.out.println(siteRegion.getNodeCount()+" sites");
		
		return forFSS(siteRegion, sol, durationYears, gmmRef, period, rps, randFields, filter, randomizeSiteLocs);
	}
	
	private static WindowedFractionalExceedanceCalculator<ObsEqkRupture> initU3forLA(AttenRelRef gmmRef, double period,
			ReturnPeriods[] rps, boolean randFields) throws IOException {
		FaultSystemSolution sol = FaultSystemSolution.load(new File("/home/kevin/OpenSHA/UCERF3/rup_sets/modular/FM3_1_branch_averaged.zip"));
		
		double durationYears = 50000;
		double siteSpacing = 0.025;
		boolean filter = true;
		boolean randomizeSiteLocs = false;
		
		GriddedRegion siteRegion = new GriddedRegion(new CaliforniaRegions.CYBERSHAKE_MAP_REGION(), siteSpacing, GriddedRegion.ANCHOR_0_0);
		System.out.println(siteRegion.getNodeCount()+" sites");
		
		return forFSS(siteRegion, sol, durationYears, gmmRef, period, rps, randFields, filter, randomizeSiteLocs);
	}

	public static void main(String[] args) throws IOException {
		boolean randFields = true;
		AttenRelRef gmmRef = AttenRelRef.ASK_2014;
		ReturnPeriods[] rps = ReturnPeriods.values();
//		double period = 3d;
		double period = 0d;
		
		File mainOutputDir = new File("/home/kevin/OpenSHA/psha_variability_study");
		
//		String dirName = "rsqsim_gmm_la";
//		WindowedFractionalExceedanceCalculator<ObsEqkRupture> calc = initRSQSimCyberShakeMap(gmmRef, period, rps, randFields);
		
//		String dirName = "rsqsim_gmm_ca";
//		WindowedFractionalExceedanceCalculator<ObsEqkRupture> calc = initRSQSimStatewide(gmmRef, period, rps, randFields);
		
//		boolean seisPDF = false;
//		WindowedFractionalExceedanceCalculator<ObsEqkRupture> calc = initUniform(gmmRef, period, rps, randFields, seisPDF);
		
		String dirName = "ucerf3_gmm_ca";
		WindowedFractionalExceedanceCalculator<ObsEqkRupture> calc = initU3(gmmRef, period, rps, randFields);
		
//		String dirName = "ucerf3_gmm_la";
//		WindowedFractionalExceedanceCalculator<ObsEqkRupture> calc = initU3forLA(gmmRef, period, rps, randFields);
		
		File outputDir = new File(mainOutputDir, dirName);
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		int numSamples = 10000;
		double[] windowDurations = {50d, 100d, 200d, 500d};
		Random rand = new Random(calc.getEvents().size()*numSamples);
		
		calc.setRandom(rand);
		
		String imtPrefix = period > 0 ? oDF.format(period)+"s" : "pga";
		
		for (double duration : windowDurations)
			calc.plotCatalogExceedanceFractDists(duration, numSamples, outputDir, imtPrefix+"_"+oDF.format(duration)+"yr");
		
		calc.plotCatalogExceedanceFractsVsDuration(50d, 250d, 15, numSamples, outputDir, imtPrefix+"_fract_vs_windown_len");
		
//		ExceedanceResult[] samples = calc.getRandomFractExceedances(windowDuration, numSamples, rand);
//		ExceedanceResult[] poissonSamples = null;
//		if (doPoisson)
//			poissonSamples = calc.getShuffledRandomFractExceedances(windowDuration, numSamples, rand);
//		
//		System.out.println("DONE, randFields="+randFields);
//		
//		for (boolean map : new boolean[] {true,false}) {
//			for (int r=0; r<rps.length; r++) {
//				for (boolean poisson : new boolean[] {false,true}) {
//					if (poisson && !doPoisson)
//						continue;
//					ExceedanceResult result = poisson ? poissonSamples[r] : samples[r];
//					double[] exceedFracts = map ? result.catalogFractSiteExceedances : result.siteFractExceedances;
//
//					System.out.println(rps[r]+", "+(map ? "Map" : "Single-Site")+(poisson ? ", Poisson" : ""));
//					WindowedFractionalExceedanceCalculator.printExceedStats(exceedFracts);
//					
//					if (map) {
//						String prefix, title;
//						prefix = "map_fract_hist_"+rps[r].name();
//						title = rps[r].label+" Map, "+oDF.format(windowDuration)+"yr Observations";
//						
//						if (poisson) {
//							prefix += "_poisson";
//							title += ", Suffled";
//						}
//						double expected = WindowedFractionalExceedanceCalculator.calcExpectedExceedanceFract(rps[r], windowDuration);
//						calc.plotCatalogExceedanceFractDist(result, expected, false, outputDir, prefix, title);
//						prefix += "_log";
//						calc.plotCatalogExceedanceFractDist(result, expected, true, outputDir, prefix, title);
//					}
//				}
//			}
//		}
	}

	public static DecimalFormat pDF = new DecimalFormat("0.00%");
	public static DecimalFormat oDF = new DecimalFormat("0.##");

}
