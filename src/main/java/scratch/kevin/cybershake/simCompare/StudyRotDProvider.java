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
import com.google.common.cache.LoadingCache;
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
	
	public static int DEFAULT_MAX_CACHE_SIZE = 100000;
	
	private LoadingCache<Site, CybershakeRun> runsCache;
	private LoadingCache<Site, List<CSRupture>> siteRupsCache;
	private LoadingCache<CacheKey, DiscretizedFunc[]> rd50Cache;
	private LoadingCache<CacheKey, double[]> pgvCache;
	private LoadingCache<CacheKey, DiscretizedFunc[][]> rd100Cache;
	private LoadingCache<CacheKey, DiscretizedFunc[]> rdRatioCache;
	private LoadingCache<CacheKey, Map<DurationTimeInterval, double[]>> durationCache;
	
	private File spectraCacheDir;
	
	private static class CacheKey {
		private CSRupture rup;
		private Site site;
		
		public CacheKey(CSRupture rup, Site site) {
			super();
			this.rup = rup;
			this.site = site;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((rup == null) ? 0 : rup.hashCode());
			result = prime * result + ((site == null) ? 0 : site.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			CacheKey other = (CacheKey) obj;
			if (rup == null) {
				if (other.rup != null)
					return false;
			} else if (!rup.equals(other.rup))
				return false;
			if (site == null) {
				if (other.site != null)
					return false;
			} else if (!site.equals(other.site))
				return false;
			return true;
		}
		
	}

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
		
		int totNumRuptures = 0;
		for (ProbEqkSource source : erf)
			totNumRuptures += source.getNumRuptures();
		System.out.println("ERF has "+totNumRuptures);
		int cacheSize = Integer.max(DEFAULT_MAX_CACHE_SIZE, totNumRuptures * 3);
		System.out.println("Max cache size: "+cacheSize);
		
		rd50Cache = CacheBuilder.newBuilder().maximumSize(cacheSize).build(new RD50Loader());
		if (hasRotD100()) {
			rd100Cache = CacheBuilder.newBuilder().maximumSize(cacheSize).build(new RD100Loader());
			rdRatioCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build(new RDRatioLoader());
		}
		
		if (hasDurations())
			durationCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build(new DurationLoader());
		
		if (hasPGV())
			pgvCache = CacheBuilder.newBuilder().maximumSize(cacheSize).build(new PGVLoader());
		
		runsCache = CacheBuilder.newBuilder().build(runCacheLoader);
		
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
		
		siteRupsCache = CacheBuilder.newBuilder().build(new CacheLoader<Site, List<CSRupture>>() {

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
			
		});
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
	
	private class RD50Loader extends CacheLoader<CacheKey, DiscretizedFunc[]> {

		@Override
		public DiscretizedFunc[] load(CacheKey key) throws Exception {
			return getSpectras(key.site, key.rup, ims);
		}
		
	}
	
	private class RD100Loader extends CacheLoader<CacheKey, DiscretizedFunc[][]> {

		@Override
		public DiscretizedFunc[][] load(CacheKey key) throws Exception {
			DiscretizedFunc[] spectra50 = getSpectras(key.site, key.rup, sa_rd50_ims);
			DiscretizedFunc[] spectra100 = getSpectras(key.site, key.rup, sa_rd100_ims);
			Preconditions.checkState(spectra50.length == spectra100.length);
			DiscretizedFunc[][] spectras = new DiscretizedFunc[spectra50.length][2];
			for (int i=0; i<spectras.length; i++) {
				spectras[i][0] = spectra50[i];
				spectras[i][1] = spectra100[i];
			}
			return spectras;
		}
		
	}
	
	private DiscretizedFunc[] getSpectras(Site site, CSRupture rup, CybershakeIM[] ims) {
		Preconditions.checkNotNull(rup);
		DiscretizedFunc[] ret = null;
		int runID;
		try {
			runID = runsCache.get(site).getRunID();
		} catch (ExecutionException e1) {
			throw ExceptionUtils.asRuntimeException(e1);
		}
		double[] periods = new double[ims.length];
		for (int i=0; i<periods.length; i++) {
			if (ims[i] == null)
				periods[i] = -1;
			else
				periods[i] = ims[i].getVal();
		}
		for (int i=0; i<ims.length; i++) {
			if (periods[i] < 0)
				continue;
			try {
				double[][][] amps = amps2db.getAllIM_Values(runID, ims[i]);
				double[] myAmps = amps[rup.getSourceID()][rup.getRupID()];
				Preconditions.checkNotNull(myAmps);
				if (ret == null) {
					ret = new DiscretizedFunc[myAmps.length];
					for (int j=0; j<ret.length; j++)
						ret[j] = new LightFixedXFunc(periods, new double[periods.length]);
				}
				for (int j=0; j<myAmps.length; j++)
					ret[j].set(i, myAmps[j] / HazardCurveComputation.CONVERSION_TO_G);
			} catch (SQLException e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
		}
		return ret;
	}
	
	private class DurationLoader extends CacheLoader<CacheKey, Map<DurationTimeInterval, double[]>> {

		@Override
		public Map<DurationTimeInterval, double[]> load(CacheKey key) throws Exception {
			CSRupture rup = key.rup;
			Site site = key.site;
			Preconditions.checkNotNull(rup);
			int runID;
			try {
				runID = runsCache.get(site).getRunID();
			} catch (ExecutionException e1) {
				throw ExceptionUtils.asRuntimeException(e1);
			}
			Map<DurationTimeInterval, double[]> ret = new HashMap<>();
			for (IMT imt : durComponents.keySet()) {
				DurationTimeInterval interval = imt.getDurationInterval();
				CybershakeIM[] compIMs = durComponents.get(imt);
				Preconditions.checkState(compIMs.length == 2);
				
				double[][] vals = null;
				for (int i=0; i<compIMs.length; i++) {
					double[][][] amps = amps2db.getAllIM_Values(runID, compIMs[i]);
					double[] myAmps = amps[rup.getSourceID()][rup.getRupID()];
					Preconditions.checkNotNull(myAmps);
					if (vals == null)
						vals = new double[myAmps.length][compIMs.length];
					for (int j=0; j<myAmps.length; j++)
						vals[j][i] = myAmps[j];
				}
				double[] geoMeans = new double[vals.length];
				for (int i=0; i<geoMeans.length; i++)
					geoMeans[i] = Math.sqrt(vals[i][0]*vals[i][1]);
				ret.put(interval, geoMeans);
			}
			return ret;
		}
		
	}
	
	private class PGVLoader extends CacheLoader<CacheKey, double[]> {

		@Override
		public double[] load(CacheKey key) throws Exception {
			CSRupture rup = key.rup;
			Site site = key.site;
			Preconditions.checkNotNull(rup);
			int runID;
			try {
				runID = runsCache.get(site).getRunID();
			} catch (ExecutionException e1) {
				throw ExceptionUtils.asRuntimeException(e1);
			}
			
			double[][][] amps = amps2db.getAllIM_Values(runID, pgvIM);
			double[] myAmps = amps[rup.getSourceID()][rup.getRupID()];
			Preconditions.checkNotNull(myAmps);
			return myAmps;
		}
		
	}

	@Override
	public DiscretizedFunc getRotD50(Site site, CSRupture rupture, int index) throws IOException {
		CacheKey key = new CacheKey(rupture, site);
		if (rd100Cache != null) {
			DiscretizedFunc[][] rds = rd100Cache.getIfPresent(key);
			if (rds != null)
				return rds[index][0];
		}
		DiscretizedFunc[] spectra = rd50Cache.getIfPresent(key);
		if (spectra != null)
			return spectra[index];
		try {
			if (spectraCacheDir != null) {
				checkLoadCacheForSite(site, CyberShakeComponent.RotD50);
				spectra = rd50Cache.getIfPresent(key);
				if (spectra != null)
					return spectra[index];
			}
			return rd50Cache.get(key)[index];
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
		CacheKey key = new CacheKey(rupture, site);
		DiscretizedFunc[][] spectra = rd100Cache.getIfPresent(key);
		if (spectra != null)
			return spectra[index];
		try {
			if (spectraCacheDir != null) {
				checkLoadCacheForSite(site, CyberShakeComponent.RotD100);
				spectra = rd100Cache.getIfPresent(key);
				if (spectra != null)
					return spectra[index];
			}
			return rd100Cache.get(key)[index];
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
	}
	
	private class RDRatioLoader extends CacheLoader<CacheKey, DiscretizedFunc[]> {

		@Override
		public DiscretizedFunc[] load(CacheKey key) throws Exception {
			DiscretizedFunc[][] spectras = rd100Cache.get(key);
			DiscretizedFunc[] ratios = new DiscretizedFunc[spectras.length];
			for (int i=0; i<ratios.length; i++)
				ratios[i] = SimulationRotDProvider.calcRotDRatio(spectras[i]);
			return ratios;
		}
		
	}

	@Override
	public DiscretizedFunc getRotDRatio(Site site, CSRupture rupture, int index) throws IOException {
		try {
			return rdRatioCache.get(new CacheKey(rupture, site))[index];
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
	}

	@Override
	public double getPGV(Site site, CSRupture rupture, int index) throws IOException {
		try {
			return pgvCache.get(new CacheKey(rupture, site))[index];
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
	}

	@Override
	public double getDuration(Site site, CSRupture rupture, DurationTimeInterval interval, int index)
			throws IOException {
		try {
			return durationCache.get(new CacheKey(rupture, site)).get(interval)[index];
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
					if (rd50Cache.getIfPresent(new CacheKey(cachedRups.get(0), site)) != null
							&& rd50Cache.getIfPresent(new CacheKey(cachedRups.get(cachedRups.size()-1), site)) != null)
						return;
				} else if (comp == CyberShakeComponent.RotD100) {
					if (rd100Cache.getIfPresent(new CacheKey(cachedRups.get(0), site)) != null
							&& rd100Cache.getIfPresent(new CacheKey(cachedRups.get(cachedRups.size()-1), site)) != null)
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
			
			for (int r=0; r<numRups; r++) {
				int sourceID = din.readInt();
				Preconditions.checkState(sourceID >= 0 && sourceID < csRups.length);
				int rupID = din.readInt();
				int numRVs = din.readInt();
				CSRupture rup = getCSRupture(sourceID, rupID, run.getERFID(), run.getRupVarScenID(), numRVs);
				rups.add(rup);
				if (comp == CyberShakeComponent.RotD50) {
					DiscretizedFunc[] spectra = new DiscretizedFunc[numRVs];
					for (int i=0; i<numRVs; i++) {
						double[] yVals = new double[numPeriods];
						for (int p=0; p<numPeriods; p++)
							yVals[p] = din.readDouble();
						spectra[i] = new LightFixedXFunc(periods, yVals);
					}
					rd50Cache.put(new CacheKey(rup, site), spectra);
				} else if (comp == CyberShakeComponent.RotD100) {
					DiscretizedFunc[][] spectra = new DiscretizedFunc[numRVs][2];
					for (int i=0; i<numRVs; i++) {
						double[] yVals = new double[numPeriods];
						for (int p=0; p<numPeriods; p++)
							yVals[p] = din.readDouble();
						spectra[i][0] = new LightFixedXFunc(periods, yVals);
						yVals = new double[numPeriods];
						for (int p=0; p<numPeriods; p++)
							yVals[p] = din.readDouble();
						spectra[i][1] = new LightFixedXFunc(periods, yVals);
					}
					rd100Cache.put(new CacheKey(rup, site), spectra);
				}
			}
			siteRupsCache.put(site, rups);
			din.close();
		}
	}

}
