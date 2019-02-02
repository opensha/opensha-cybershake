package scratch.kevin.cybershake.simCompare;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.primitives.Doubles;

import scratch.kevin.simCompare.SimulationRotDProvider;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimProbEqkRup;

public class StudyRotDProvider implements SimulationRotDProvider<CSRupture> {
	
	private CachedPeakAmplitudesFromDB amps2db;
	private ERF2DB erf2db;
	private double[] periods;
	private CybershakeIM[] rd50_ims;
	private CybershakeIM[] rd100_ims;
	private String name;
	private CSRupture[][] csRups;
	
	private AbstractERF erf;
	
	public static int MAX_CACHE_SIZE = 100000;
	
	private LoadingCache<Site, CybershakeRun> runsCache;
	private LoadingCache<Site, List<CSRupture>> siteRupsCache;
	private LoadingCache<CacheKey, DiscretizedFunc[]> rd50Cache;
	private LoadingCache<CacheKey, DiscretizedFunc[][]> rd100Cache;
	private LoadingCache<CacheKey, DiscretizedFunc[]> rdRatioCache;
	
	private class CacheKey {
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
			result = prime * result + getOuterType().hashCode();
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
			if (!getOuterType().equals(other.getOuterType()))
				return false;
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

		private StudyRotDProvider getOuterType() {
			return StudyRotDProvider.this;
		}
		
	}

	public StudyRotDProvider(CyberShakeStudy study, CachedPeakAmplitudesFromDB amps2db, double[] periods, String name) {
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
			
		}, amps2db, periods, name);
	}

	public StudyRotDProvider(AbstractERF erf, CacheLoader<Site, CybershakeRun> runCacheLoader, CachedPeakAmplitudesFromDB amps2db,
			double[] periods, String name) {
		this.amps2db = amps2db;
		this.erf2db = new ERF2DB(amps2db.getDBAccess());
		this.periods = periods;
		this.name = name;
		this.erf = erf;
		
		rd50_ims = amps2db.getIMs(Doubles.asList(periods),
				IMType.SA, CyberShakeComponent.RotD50).toArray(new CybershakeIM[0]);
		rd100_ims = amps2db.getIMs(Doubles.asList(periods),
				IMType.SA, CyberShakeComponent.RotD100).toArray(new CybershakeIM[0]);
		
		rd50Cache = CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE).build(new RD50Loader());
		if (hasRotD100()) {
			rd100Cache = CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE).build(new RD100Loader());
			rdRatioCache = CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE).build(new RDRatioLoader());
		}
		
		runsCache = CacheBuilder.newBuilder().build(runCacheLoader);
		
		csRups = new CSRupture[erf.getNumSources()][];
		
		siteRupsCache = CacheBuilder.newBuilder().build(new CacheLoader<Site, List<CSRupture>>() {

			@Override
			public List<CSRupture> load(Site site) throws Exception {
				System.out.println("Fetching sources for "+site.getName());
				CybershakeRun run = runsCache.get(site);
				double[][][] allAmps = amps2db.getAllIM_Values(run.getRunID(), rd50_ims[0]);
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
	
	private CSRupture getCSRupture(int sourceID, int rupID, int erfID, int rvScenID, int numRVs) {
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
		this.periods = o.periods;
		this.rd50_ims = o.rd50_ims;
		this.rd100_ims = o.rd100_ims;
		this.name = name;
		this.csRups = o.csRups;
		this.erf = o.erf;
		this.runsCache = o.runsCache;
		this.siteRupsCache = o.siteRupsCache;
		this.rd50Cache = o.rd50Cache;
		this.rd100Cache = o.rd100Cache;
		this.rdRatioCache = o.rdRatioCache;
	}
	
	public CybershakeRun getRun(Site site) {
		try {
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
			return getSpectras(key.site, key.rup, rd50_ims);
		}
		
	}
	
	private class RD100Loader extends CacheLoader<CacheKey, DiscretizedFunc[][]> {

		@Override
		public DiscretizedFunc[][] load(CacheKey key) throws Exception {
			DiscretizedFunc[] spectra50 = getSpectras(key.site, key.rup, rd50_ims);
			DiscretizedFunc[] spectra100 = getSpectras(key.site, key.rup, rd100_ims);
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
		DiscretizedFunc[] ret = null;
		int runID;
		try {
			runID = runsCache.get(site).getRunID();
		} catch (ExecutionException e1) {
			throw ExceptionUtils.asRuntimeException(e1);
		}
		for (int i=0; i<ims.length; i++) {
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

	@Override
	public DiscretizedFunc getRotD50(Site site, CSRupture rupture, int index) throws IOException {
		if (rd100Cache != null)
			return getRotD(site, rupture, index)[0];
		try {
			return rd50Cache.get(new CacheKey(rupture, site))[index];
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
		try {
			return rd100Cache.get(new CacheKey(rupture, site))[index];
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
	public int getNumSimulations(Site site, CSRupture rupture) {
		return rupture.getNumRVs();
	}

	@Override
	public Collection<CSRupture> getRupturesForSite(Site site) {
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
		return rd100_ims != null;
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
		try {
			for (CSRupture rup : siteRupsCache.get(site)) {
				double rate = getAnnualRate(rup)/getNumSimulations(site, rup);
				if (rate > 0)
					minNonZeroRate = Math.min(minNonZeroRate, rate);
			}
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
		return minNonZeroRate;
	}

	@Override
	public Location getHypocenter(CSRupture rupture, int index) {
		return rupture.getHypocenter(index, erf2db);
	}

}
