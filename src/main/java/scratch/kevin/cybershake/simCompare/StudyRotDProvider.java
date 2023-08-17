package scratch.kevin.cybershake.simCompare;

import java.awt.geom.Point2D;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;

import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.LightFixedXFunc;
import org.opensha.commons.geo.Location;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.calc.mcer.CyberShakeSiteRun;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeIM.IMType;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.ERF2DB;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.imr.param.IntensityMeasureParams.DurationTimeInterval;
import org.opensha.sha.imr.param.IntensityMeasureParams.PGV_Param;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;
import org.opensha.sha.imr.param.IntensityMeasureParams.SignificantDurationParam;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.primitives.Doubles;

import scratch.kevin.simCompare.IMT;
import scratch.kevin.simCompare.SimulationRotDProvider;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimProbEqkRup;

public class StudyRotDProvider implements SimulationRotDProvider<CSRupture> {
	
	private CachedPeakAmplitudesFromDB amps2db;
	private ERF2DB erf2db;
	private IMT[] imts;
	private CybershakeIM[] ims;
	private CybershakeIM[] sa_rd50_ims;
	private CybershakeIM[] sa_rd100_ims;
	private String name;
	private CSRupture[][] csRups;
	private CybershakeIM pgvIM;
	
	private Map<IMT, CybershakeIM[]> durComponents;
	
	private AbstractERF erf;
	
//	public static int DEFAULT_MAX_CACHE_SIZE = 100000;
	public static int DEFAULT_MAX_CACHED_SITES = 20;
	
	private LoadingCache<Site, CybershakeRun> runsCache;
	private LoadingCache<Site, List<CSRupture>> siteRupsCache;
	
	private LoadingCache<Site, DiscretizedFunc[][][]> rd50Cache;
	private LoadingCache<Site, double[][][]> pgvCache;
	private LoadingCache<Site, DiscretizedFunc[][][][]> rd100Cache;
	private LoadingCache<Site, DiscretizedFunc[][][]> rdRatioCache;
	private LoadingCache<Site, Map<DurationTimeInterval, double[][][]>> durationCache;
	
	private File spectraCacheDir;

	public StudyRotDProvider(CyberShakeStudy study, CachedPeakAmplitudesFromDB amps2db, IMT[] imts, String name) {
		this(study.getERF(), new CacheLoader<Site, CybershakeRun>() {

			@Override
			public CybershakeRun load(Site site) throws Exception {
				if (site instanceof CyberShakeSiteRun) {
					return ((CyberShakeSiteRun)site).getCS_Run();
				}
				Preconditions.checkNotNull(site.getName() != null, "Must supply site, or site with name set to CS Short Name");
				List<CybershakeRun> runs = study.runFetcher().forSiteNames(site.getName()).fetch();
				Preconditions.checkState(!runs.isEmpty(), "No runs found for Study %s, Site '%s'", study.getName(), site.getName());
				return runs.get(0);
			}
			
		}, amps2db, imts, name);
	}

	public StudyRotDProvider(AbstractERF erf, CacheLoader<Site, CybershakeRun> runCacheLoader, CachedPeakAmplitudesFromDB amps2db,
			IMT[] imts, String name) {
		this.amps2db = amps2db;
		this.erf2db = new ERF2DB(amps2db.getDBAccess());
		this.imts = imts;
		this.name = name;
		this.erf = erf;
		
		ims = new CybershakeIM[imts.length];
		sa_rd50_ims = ims;
		sa_rd100_ims = new CybershakeIM[imts.length];
		for (int i=0; i<ims.length; i++) {
			IMT imt = imts[i];
			if (imt.getParamName().equals(SA_Param.NAME)) {
				ims[i] = CybershakeIM.getSA(CyberShakeComponent.RotD50, imt.getPeriod());
				sa_rd100_ims[i] = CybershakeIM.getSA(CyberShakeComponent.RotD100, imt.getPeriod());
			} else if (imt.getParamName().equals(SignificantDurationParam.NAME)) {
				// need to goe-mean them ourselves
				if (durComponents == null)
					durComponents = new HashMap<>();
				DurationTimeInterval interval = imt.getDurationInterval();
				CybershakeIM xIM = CybershakeIM.getDuration(CyberShakeComponent.X, interval);
				CybershakeIM yIM = CybershakeIM.getDuration(CyberShakeComponent.Y, interval);
				durComponents.put(imt, new CybershakeIM[] { xIM, yIM });
			} else if (imt.getParamName().equals(PGV_Param.NAME)){
				pgvIM = CybershakeIM.PGV;
			} else {
				throw new IllegalStateException(imt+" is not yet supported here");
			}
		}
		
//		int totNumRuptures = 0;
//		for (ProbEqkSource source : erf)
//			totNumRuptures += source.getNumRuptures();
//		System.out.println("ERF has "+totNumRuptures);
//		int cacheSize = Integer.max(DEFAULT_MAX_CACHE_SIZE, totNumRuptures * 3);
		int cacheSize = DEFAULT_MAX_CACHED_SITES;
		System.out.println("Max cache size: "+cacheSize+" sites");
		
		rd50Cache = new PrevValueQuickLoadingCache<>(CacheBuilder.newBuilder().maximumSize(cacheSize).build(new RD50Loader()));
		if (hasRotD100()) {
			rd100Cache = new PrevValueQuickLoadingCache<>(CacheBuilder.newBuilder().maximumSize(cacheSize).build(new RD100Loader()));
			rdRatioCache = new PrevValueQuickLoadingCache<>(CacheBuilder.newBuilder().maximumSize(cacheSize).build(new RDRatioLoader()));
		}
		
		if (hasDurations())
			durationCache = new PrevValueQuickLoadingCache<>(CacheBuilder.newBuilder().maximumSize(cacheSize).build(new DurationLoader()));
		
		if (hasPGV())
			pgvCache = new PrevValueQuickLoadingCache<>(CacheBuilder.newBuilder().maximumSize(cacheSize).build(new PGVLoader()));
		
		runsCache = new PrevValueQuickLoadingCache<>(CacheBuilder.newBuilder().build(runCacheLoader));
		
		csRups = new CSRupture[erf.getNumSources()][];
		
		final CybershakeIM refIM;
		if (ims[0] == null) {
			if (pgvIM != null) {
				refIM = pgvIM;
			} else {
				Preconditions.checkNotNull(durComponents);
				refIM = durComponents.values().iterator().next()[0];
			}
		} else {
			refIM = ims[0];
		}
		
		siteRupsCache = new PrevValueQuickLoadingCache<>(CacheBuilder.newBuilder().build(new CacheLoader<Site, List<CSRupture>>() {

			@Override
			public List<CSRupture> load(Site site) throws Exception {
				System.out.println("Fetching sources for "+site.getName());
				CybershakeRun run = runsCache.get(site);
				double[][][] allAmps = amps2db.getAllIM_Values(run.getRunID(), refIM);
				List<CSRupture> siteRups = new ArrayList<>();
				for (int sourceID=0; sourceID<allAmps.length; sourceID++) {
					if (allAmps[sourceID] != null) {
						for (int rupID=0; rupID<allAmps[sourceID].length; rupID++) {
							if (allAmps[sourceID][rupID] != null) {
								int numRVs = allAmps[sourceID][rupID].length;
								siteRups.add(getCSRupture(sourceID, rupID, run.getERFID(), run.getRupVarScenID(), numRVs));
							}
						}
					}
				}
				return siteRups;
			}
			
		}));
	}
	
	public AbstractERF getERF() {
		return erf;
	}
	
	public void setSpectraCacheDir(File spectraCacheDir) {
		this.spectraCacheDir = spectraCacheDir;
	}
	
	public CSRupture getCSRupture(int sourceID, int rupID, int erfID, int rvScenID, int numRVs) {
		if (csRups[sourceID] != null && csRups[sourceID][rupID] != null)
			return csRups[sourceID][rupID];
		synchronized (csRups) {
			if (csRups[sourceID] == null)
				csRups[sourceID] = new CSRupture[erf.getNumRuptures(sourceID)];
			ProbEqkRupture rup = erf.getRupture(sourceID, rupID);
			if (rup instanceof RSQSimProbEqkRup)
				csRups[sourceID][rupID] = new CSRupture(erfID, rvScenID, sourceID, rupID, rup, numRVs,
						((RSQSimProbEqkRup)rup).getTimeYears());
			else
				csRups[sourceID][rupID] = new CSRupture(erfID, rvScenID, sourceID, rupID, rup, numRVs);
		}
		return csRups[sourceID][rupID];
	}
	
	protected StudyRotDProvider(StudyRotDProvider o, String name) {
		this.amps2db = o.amps2db;
		this.erf2db = o.erf2db;
		this.imts = o.imts;
		this.ims = o.ims;
		this.sa_rd50_ims = o.sa_rd50_ims;
		this.sa_rd100_ims = o.sa_rd100_ims;
		this.name = name;
		this.csRups = o.csRups;
		this.erf = o.erf;
		this.runsCache = o.runsCache;
		this.siteRupsCache = o.siteRupsCache;
		this.rd50Cache = o.rd50Cache;
		this.rd100Cache = o.rd100Cache;
		this.rdRatioCache = o.rdRatioCache;
		this.spectraCacheDir = o.spectraCacheDir;
	}
	
	public CybershakeRun getRun(Site site) {
		try {
			Preconditions.checkNotNull(site);
			return runsCache.get(site);
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
	}

	@Override
	public String getName() {
		return name;
	}
	
	private static class PrevValueQuickLoadingCache<K,V> implements LoadingCache<K,V> {
		
		private LoadingCache<K, V> cache;
		private Object prevKey;
		private V prevVal;

		public PrevValueQuickLoadingCache(LoadingCache<K,V> cache) {
			this.cache = cache;
		}

		@Override
		public void cleanUp() {
			cache.cleanUp();
		}
		
		private V checkGetPrev(Object key) {
			if (key.equals(prevKey)) {
				synchronized (this) {
					if (key.equals(prevKey)) {
						return prevVal;
					}
				}
			}
			return null;
		}
		
		private synchronized void setPrevVal(Object key, V val) {
			prevKey = key;
			prevVal = val;
		}

		@Override
		public V get(K key, Callable<? extends V> arg1) throws ExecutionException {
			V cached = checkGetPrev(key);
			if (cached != null)
				return cached;
			V val = cache.get(key, arg1);
			setPrevVal(key, val);
			return val;
		}

		@Override
		public ImmutableMap<K, V> getAllPresent(Iterable<? extends Object> arg0) {
			return cache.getAllPresent(arg0);
		}

		@Override
		public V getIfPresent(Object key) {
			V cached = checkGetPrev(key);
			if (cached != null)
				return cached;
			V val = cache.getIfPresent(key);
			if (val != null)
				setPrevVal(key, val);
			return val;
		}

		@Override
		public void invalidate(Object arg0) {
			if (arg0.equals(prevKey))
				setPrevVal(null, null);
			cache.invalidate(arg0);
		}

		@Override
		public void invalidateAll() {
			setPrevVal(null, null);
			cache.invalidateAll();
		}

		@Override
		public void invalidateAll(Iterable<? extends Object> arg0) {
			setPrevVal(null, null);
			cache.invalidateAll(arg0);
		}

		@Override
		public void put(K arg0, V arg1) {
			cache.put(arg0, arg1);
		}

		@Override
		public void putAll(Map<? extends K, ? extends V> arg0) {
			cache.putAll(arg0);
		}

		@Override
		public long size() {
			return cache.size();
		}

		@Override
		public CacheStats stats() {
			return cache.stats();
		}

		@Override
		public V apply(K arg0) {
			return cache.apply(arg0);
		}

		@Override
		public ConcurrentMap<K, V> asMap() {
			return cache.asMap();
		}

		@Override
		public V get(K key) throws ExecutionException {
			V cached = checkGetPrev(key);
			if (cached != null)
				return cached;
			V val = cache.get(key);
			setPrevVal(key, val);
			return val;
		}

		@Override
		public ImmutableMap<K, V> getAll(Iterable<? extends K> arg0) throws ExecutionException {
			return cache.getAll(arg0);
		}

		@Override
		public V getUnchecked(K key) {
			V cached = checkGetPrev(key);
			if (cached != null)
				return cached;
			V val = cache.getUnchecked(key);
			setPrevVal(key, val);
			return val;
		}

		@Override
		public void refresh(K key) {
			if (key.equals(prevKey))
				setPrevVal(null, null);
			cache.refresh(key);
		}
		
	}
	
	private class RD50Loader extends CacheLoader<Site, DiscretizedFunc[][][]> {

		@Override
		public DiscretizedFunc[][][] load(Site site) throws Exception {
			return getSpectras(site, ims);
		}
		
	}
	
	private class RD100Loader extends CacheLoader<Site, DiscretizedFunc[][][][]> {

		@Override
		public DiscretizedFunc[][][][] load(Site site) throws Exception {
			DiscretizedFunc[][][] spectra50 = getSpectras(site, sa_rd50_ims);
			DiscretizedFunc[][][] spectra100 = getSpectras(site, sa_rd100_ims);
			Preconditions.checkState(spectra50.length == spectra100.length);
			DiscretizedFunc[][][][] spectras = new DiscretizedFunc[spectra50.length][][][];
			for (int sourceID=0; sourceID<spectras.length; sourceID++) {
				if (spectra50[sourceID] != null) {
					Preconditions.checkNotNull(spectra100[sourceID]);
					int rups = spectra50[sourceID].length;
					Preconditions.checkState(rups == spectra100[sourceID].length);
					spectras[sourceID] = new DiscretizedFunc[rups][][];
					for (int rupID=0; rupID<rups; rupID++) {
						if (spectra50[sourceID][rupID] != null) {
							Preconditions.checkNotNull(spectra100[sourceID][rupID]);
							int rvs = spectra50[sourceID][rupID].length;
							Preconditions.checkState(rvs == spectra100[sourceID][rupID].length);
							spectras[sourceID][rupID] = new DiscretizedFunc[rvs][2];
							for (int rvID=0; rvID<rvs; rvID++) {
								spectras[sourceID][rupID][rvID][0] = spectra50[sourceID][rupID][rvID];
								spectras[sourceID][rupID][rvID][1] = spectra100[sourceID][rupID][rvID];
							}
						} else {
							Preconditions.checkState(spectra100[sourceID][rupID] == null);
						}
					}
				} else {
					Preconditions.checkState(spectra100[sourceID] == null);
				}
			}
			return spectras;
		}
		
	}
	
	private DiscretizedFunc[][][] getSpectras(Site site, CybershakeIM[] ims) {
		DiscretizedFunc[][][] ret = new DiscretizedFunc[erf.getNumSources()][][];
		int runID;
		try {
			runID = runsCache.get(site).getRunID();
		} catch (ExecutionException e1) {
			throw ExceptionUtils.asRuntimeException(e1);
		}
		List<Double> saPeriods = new ArrayList<>();
		List<CybershakeIM> saIMs = new ArrayList<>();
		
		for (CybershakeIM im : ims) {
			if (im != null) {
				saPeriods.add(im.getVal());
				saIMs.add(im);
			}
		}
		
		double[] periods = Doubles.toArray(saPeriods);
		for (int i=0; i<periods.length; i++) {
			CybershakeIM im = saIMs.get(i);
			try {
				double[][][] amps = amps2db.getAllIM_Values(runID, im);
				for (int sourceID=0; sourceID<amps.length; sourceID++) {
					if (amps[sourceID] != null) {
						int rups = erf.getNumRuptures(sourceID);
						if (ret[sourceID] == null)
							ret[sourceID] = new DiscretizedFunc[rups][];
						for (int rupID=0; rupID<rups; rupID++) {
							if (amps[sourceID][rupID] != null) {
								int rvs = amps[sourceID][rupID].length;
								if (ret[sourceID][rupID] == null) {
									ret[sourceID][rupID] = new DiscretizedFunc[rvs];
									for (int rvID=0; rvID<rvs; rvID++)
										ret[sourceID][rupID][rvID] = new LightFixedXFunc(periods, new double[periods.length]);
								} else {
									Preconditions.checkState(ret[sourceID][rupID].length == rvs);
								}
								for (int rvID=0; rvID<rvs; rvID++)
									ret[sourceID][rupID][rvID].set(i, amps[sourceID][rupID][rvID] / HazardCurveComputation.CONVERSION_TO_G);
							}
						}
					}
				}
			} catch (SQLException e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
		}
		return ret;
	}
	
	private class DurationLoader extends CacheLoader<Site, Map<DurationTimeInterval, double[][][]>> {

		@Override
		public Map<DurationTimeInterval, double[][][]> load(Site site) throws Exception {
			int runID;
			try {
				runID = runsCache.get(site).getRunID();
			} catch (ExecutionException e1) {
				throw ExceptionUtils.asRuntimeException(e1);
			}
			Map<DurationTimeInterval, double[][][]> ret = new HashMap<>();
			for (IMT imt : durComponents.keySet()) {
				DurationTimeInterval interval = imt.getDurationInterval();
				CybershakeIM[] compIMs = durComponents.get(imt);
				Preconditions.checkState(compIMs.length == 2);
				
				double[][][] geoMeans = new double[erf.getNumSources()][][];
				// one is x component, one is y
				double[][][] amps1 = amps2db.getAllIM_Values(runID, compIMs[0]);
				double[][][] amps2 = amps2db.getAllIM_Values(runID, compIMs[0]);
				
				for (int sourceID=0; sourceID<geoMeans.length; sourceID++) {
					if (amps1[sourceID] != null) {
						Preconditions.checkNotNull(amps2[sourceID]);
						int rups = amps1[sourceID].length;
						Preconditions.checkState(rups == amps2[sourceID].length);
						geoMeans[sourceID] = new double[rups][];
						for (int rupID=0; rupID<rups; rupID++) {
							if (amps1[sourceID][rupID] != null) {
								Preconditions.checkNotNull(amps2[sourceID][rupID]);
								int rvs = amps1[sourceID][rupID].length;
								Preconditions.checkState(rvs == amps2[sourceID][rupID].length);
								geoMeans[sourceID][rupID] = new double[rvs];
								for (int rvID=0; rvID<rvs; rvID++)
									geoMeans[sourceID][rupID][rvID] = Math.sqrt(
											amps1[sourceID][rupID][rvID]*amps2[sourceID][rupID][rvID]);
							}
						}
					} else {
						Preconditions.checkState(amps2[sourceID] == null);
					}
				}
				ret.put(interval, geoMeans);
			}
			return ret;
		}
		
	}
	
	private class PGVLoader extends CacheLoader<Site, double[][][]> {

		@Override
		public double[][][] load(Site site) throws Exception {
			int runID;
			try {
				runID = runsCache.get(site).getRunID();
			} catch (ExecutionException e1) {
				throw ExceptionUtils.asRuntimeException(e1);
			}
			
			return amps2db.getAllIM_Values(runID, pgvIM);
		}
		
	}

	@Override
	public DiscretizedFunc getRotD50(Site site, CSRupture rupture, int index) throws IOException {
		if (rd100Cache != null) {
			DiscretizedFunc[][][][] rds = rd100Cache.getIfPresent(site);
			if (rds != null)
				return rds[rupture.getSourceID()][rupture.getRupID()][index][0];
		}
		DiscretizedFunc[][][] spectra = rd50Cache.getIfPresent(site);
		if (spectra != null)
			return spectra[rupture.getSourceID()][rupture.getRupID()][index];
		try {
			if (spectraCacheDir != null) {
				checkLoadCacheForSite(site, CyberShakeComponent.RotD50);
				spectra = rd50Cache.getIfPresent(site);
				if (spectra != null)
					return spectra[rupture.getSourceID()][rupture.getRupID()][index];
			}
			return rd50Cache.get(site)[rupture.getSourceID()][rupture.getRupID()][index];
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
	}

	@Override
	public DiscretizedFunc getRotD100(Site site, CSRupture rupture, int index) throws IOException {
		return getRotD(site, rupture, index)[1];
	}

	@Override
	public DiscretizedFunc[] getRotD(Site site, CSRupture rupture, int index) throws IOException {
		DiscretizedFunc[][][][] spectra = rd100Cache.getIfPresent(site);
		if (spectra != null)
			return spectra[rupture.getSourceID()][rupture.getRupID()][index];
		try {
			if (spectraCacheDir != null) {
				checkLoadCacheForSite(site, CyberShakeComponent.RotD100);
				spectra = rd100Cache.getIfPresent(site);
				if (spectra != null)
					return spectra[rupture.getSourceID()][rupture.getRupID()][index];
			}
			return rd100Cache.get(site)[rupture.getSourceID()][rupture.getRupID()][index];
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
	}
	
	private class RDRatioLoader extends CacheLoader<Site, DiscretizedFunc[][][]> {

		@Override
		public DiscretizedFunc[][][] load(Site key) throws Exception {
			DiscretizedFunc[][][][] spectras = rd100Cache.get(key);
			DiscretizedFunc[][][] ratios = new DiscretizedFunc[spectras.length][][];
			int sources = spectras.length;
			for (int sourceID=0; sourceID<sources; sourceID++) {
				int rups = spectras[sourceID].length;
				if (spectras[sourceID] != null) {
					ratios[sourceID] = new DiscretizedFunc[spectras[sourceID].length][];
					for (int rupID=0; rupID<rups; rupID++) {
						int rvs = spectras[sourceID][rupID].length;
						ratios[sourceID][rupID] = new DiscretizedFunc[rvs];
						for (int rvID=0; rvID<rvs; rvID++)
							ratios[sourceID][rupID][rvID] = SimulationRotDProvider.calcRotDRatio(spectras[sourceID][rupID][rvID]);
					}
				}
			}
			return ratios;
		}
		
	}

	@Override
	public DiscretizedFunc getRotDRatio(Site site, CSRupture rupture, int index) throws IOException {
		try {
			return rdRatioCache.get(site)[rupture.getSourceID()][rupture.getRupID()][index];
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
	}

	@Override
	public double getPGV(Site site, CSRupture rupture, int index) throws IOException {
		try {
			return pgvCache.get(site)[rupture.getSourceID()][rupture.getRupID()][index];
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
	}

	@Override
	public double getPGA(Site site, CSRupture rupture, int index) throws IOException {
		throw new UnsupportedOperationException();
	}

	@Override
	public double getDuration(Site site, CSRupture rupture, DurationTimeInterval interval, int index)
			throws IOException {
		try {
			return durationCache.get(site).get(interval)[rupture.getSourceID()][rupture.getRupID()][index];
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
	}

	@Override
	public int getNumSimulations(Site site, CSRupture rupture) {
		return rupture.getNumRVs();
	}

	@Override
	public Collection<CSRupture> getRupturesForSite(Site site) {
		List<CSRupture> ruptures = siteRupsCache.getIfPresent(site);
		if (ruptures != null)
			return ruptures;
		if (spectraCacheDir != null) {
			try {
				checkLoadCacheForSite(site, CyberShakeComponent.RotD50);
			} catch (ExecutionException | IOException e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
			ruptures = siteRupsCache.getIfPresent(site);
			if (ruptures != null)
				return ruptures;
		}
		try {
			return siteRupsCache.get(site);
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
	}

	@Override
	public boolean hasRotD50() {
		return true;
	}

	@Override
	public boolean hasRotD100() {
		return sa_rd100_ims != null;
	}

	@Override
	public boolean hasPGV() {
		return pgvIM != null;
	}

	@Override
	public boolean hasPGA() {
		return false;
	}

	@Override
	public boolean hasDurations() {
		return durComponents != null;
	}

	@Override
	public double getAnnualRate(CSRupture rupture) {
		return rupture.getRate();
	}

	@Override
	public double getMagnitude(CSRupture rupture) {
		return rupture.getRup().getMag();
	}

	@Override
	public synchronized double getMinimumCurvePlotRate(Site site) {
		double minNonZeroRate = Double.POSITIVE_INFINITY;
		for (CSRupture rup : getRupturesForSite(site)) {
			double rate = getAnnualRate(rup)/getNumSimulations(site, rup);
			if (rate > 0)
				minNonZeroRate = Math.min(minNonZeroRate, rate);
		}
		return minNonZeroRate;
	}

	@Override
	public Location getHypocenter(CSRupture rupture, int index) {
		return rupture.getHypocenter(index, erf2db);
	}
	
	private File getCacheFile(Site site, CyberShakeComponent comp) throws ExecutionException {
		Preconditions.checkNotNull(spectraCacheDir);
		CybershakeRun run = runsCache.get(site);
		String fName = "run_"+run.getRunID()+"_spectra_"+comp.getShortName();
		CybershakeIM[] ims;
		if (comp == CyberShakeComponent.RotD50)
			ims = sa_rd50_ims;
		else if (comp == CyberShakeComponent.RotD100)
			ims = sa_rd100_ims;
		else
			throw new IllegalStateException("Only support RD50/100");
		for (CybershakeIM im : ims)
			fName += "_"+im.getID();
		return new File(spectraCacheDir, fName+".bin");
	}
	
	public void checkWriteCacheForSite(Site site, CyberShakeComponent comp) throws ExecutionException, IOException {
		File cacheFile = getCacheFile(site, comp);
		if (cacheFile.exists())
			return;
		
		List<CSRupture> rups = siteRupsCache.get(site);
		
		System.out.println("Writing site spectra cache to "+cacheFile.getName());
		
		File tempCacheFile = new File(cacheFile.getName()+"_temp"+System.nanoTime());
		
		DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(tempCacheFile)));
		
//		int sourceID, int rupID, int erfID, int rvScenID, int numRVs
		out.writeInt(rups.size()); // num ruptures
		out.writeInt(ims.length); // num periods
		for (CSRupture rup : rups) {
			out.writeInt(rup.getSourceID());
			out.writeInt(rup.getRupID());
			if (comp == CyberShakeComponent.RotD50) {
				List<DiscretizedFunc> spectra = getRotD50s(site, rup);
				
				Preconditions.checkState(spectra.size() == rup.getNumRVs());
				out.writeInt(spectra.size());
				for (DiscretizedFunc spectrum : spectra) {
					for (Point2D pt : spectrum)
						out.writeDouble(pt.getY());
				}
			} else if (comp == CyberShakeComponent.RotD100) {
				List<DiscretizedFunc[]> spectra = getRotDs(site, rup);
				
				Preconditions.checkState(spectra.size() == rup.getNumRVs());
				out.writeInt(spectra.size());
				for (DiscretizedFunc[] spectrum : spectra) {
					for (DiscretizedFunc s : spectrum)
						for (Point2D pt : s)
							out.writeDouble(pt.getY());
				}
			} else {
				out.close();
				throw new IllegalStateException("Only support RD50/100");
			}
		}
		
		out.close();
		
		synchronized (spectraCacheDir) {
			Files.move(tempCacheFile, cacheFile);
		}
	}
	
	public void checkLoadCacheForSite(Site site, CyberShakeComponent comp)
			throws ExecutionException, IOException {
		File cacheFile = getCacheFile(site, comp);
		
		if (!cacheFile.exists())
			return;
		
		synchronized (spectraCacheDir) {
			List<CSRupture> cachedRups = siteRupsCache.getIfPresent(site);
			if (cachedRups != null) {
				// might have already been cached in another thread
				if (comp == CyberShakeComponent.RotD50) {
					if (rd50Cache.getIfPresent(site) != null)
						return;
				} else if (comp == CyberShakeComponent.RotD100) {
					if (rd100Cache.getIfPresent(site) != null)
						return;
				}
			}
			CybershakeRun run = runsCache.get(site);
			
			System.out.println("Loading site spectra cache from "+cacheFile.getName());
			DataInputStream din = new DataInputStream(new BufferedInputStream(new FileInputStream(cacheFile)));
			
			int numRups = din.readInt();
			int numPeriods = din.readInt();
			
			List<CSRupture> rups = new ArrayList<>();
			
			double[] periods = new double[sa_rd50_ims.length];
			for (int i=0; i<periods.length; i++)
				periods[i] = sa_rd50_ims[i].getVal();
			
			DiscretizedFunc[][][] rd50Spectra = comp == CyberShakeComponent.RotD50 ? new DiscretizedFunc[erf.getNumSources()][][] : null;
			DiscretizedFunc[][][][] rd100Spectra = comp == CyberShakeComponent.RotD100 ? new DiscretizedFunc[erf.getNumSources()][][][] : null;
			
			for (int r=0; r<numRups; r++) {
				int sourceID = din.readInt();
				Preconditions.checkState(sourceID >= 0 && sourceID < csRups.length);
				int rupID = din.readInt();
				int numRVs = din.readInt();
				CSRupture rup = getCSRupture(sourceID, rupID, run.getERFID(), run.getRupVarScenID(), numRVs);
				rups.add(rup);
				if (comp == CyberShakeComponent.RotD50) {
					if (rd50Spectra[sourceID] == null)
						rd50Spectra[sourceID] = new DiscretizedFunc[erf.getNumRuptures(sourceID)][];
					rd50Spectra[sourceID][rupID] = new DiscretizedFunc[numRVs];
					for (int i=0; i<numRVs; i++) {
						double[] yVals = new double[numPeriods];
						for (int p=0; p<numPeriods; p++)
							yVals[p] = din.readDouble();
						rd50Spectra[sourceID][rupID][i] = new LightFixedXFunc(periods, yVals);
					}
				} else if (comp == CyberShakeComponent.RotD100) {
					if (rd100Spectra[sourceID] == null)
						rd100Spectra[sourceID] = new DiscretizedFunc[erf.getNumRuptures(sourceID)][][];
					rd100Spectra[sourceID][rupID] = new DiscretizedFunc[numRVs][2];
					for (int i=0; i<numRVs; i++) {
						double[] yVals = new double[numPeriods];
						for (int p=0; p<numPeriods; p++)
							yVals[p] = din.readDouble();
						rd100Spectra[sourceID][rupID][i][0] = new LightFixedXFunc(periods, yVals);
						yVals = new double[numPeriods];
						for (int p=0; p<numPeriods; p++)
							yVals[p] = din.readDouble();
						rd100Spectra[sourceID][rupID][i][1] = new LightFixedXFunc(periods, yVals);
					}
				}
			}
			if (comp == CyberShakeComponent.RotD50)
				rd50Cache.put(site, rd50Spectra);
			else if (comp == CyberShakeComponent.RotD100)
				rd100Cache.put(site, rd100Spectra);
			siteRupsCache.put(site, rups);
			din.close();
		}
	}

	@Override
	public double getRake(CSRupture rupture) {
		return rupture.getRup().getAveRake();
	}

}
