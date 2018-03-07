package scratch.kevin.cybershake.simCompare;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.LightFixedXFunc;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;

import scratch.kevin.simCompare.SimulationRotDProvider;

public class StudyRotDProvider implements SimulationRotDProvider<CSRupture> {
	
	private CachedPeakAmplitudesFromDB amps2db;
	private Map<Site, Integer> runIDsMap;
	private Map<Site, List<CSRupture>> siteRups;
	private double[] periods;
	private CybershakeIM[] rd50_ims;
	private CybershakeIM[] rd100_ims;
	private String name;
	
	public static int MAX_CACHE_SIZE = 100000;
	
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
	
	private Map<CSRupture, Integer> rvCountMap;

	public StudyRotDProvider(CachedPeakAmplitudesFromDB amps2db, Map<Site, Integer> runIDsMap,
			Map<Site, List<CSRupture>> siteRups, double[] periods, CybershakeIM[] rd50_ims,
			CybershakeIM[] rd100_ims, String name) {
		this.amps2db = amps2db;
		this.runIDsMap = runIDsMap;
		this.siteRups = siteRups;
		this.periods = periods;
		this.rd50_ims = rd50_ims;
		this.rd100_ims = rd100_ims;
		this.name = name;

		Preconditions.checkState(periods.length == rd50_ims.length);
		Preconditions.checkState(rd100_ims == null || rd50_ims.length == rd100_ims.length);
		for (int i=0; i<rd50_ims.length; i++) {
			Preconditions.checkState((float)rd50_ims[i].getVal() == (float)rd50_ims[i].getVal());
			Preconditions.checkState((float)rd50_ims[i].getVal() == (float)periods[i]);
		}
		
		rd50Cache = CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE).build(new RD50Loader());
		if (hasRotD100()) {
			rd100Cache = CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE).build(new RD100Loader());
			rdRatioCache = CacheBuilder.newBuilder().maximumSize(MAX_CACHE_SIZE).build(new RDRatioLoader());
		}
		
		rvCountMap = new HashMap<>();
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
		int runID = runIDsMap.get(site);
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
	public synchronized int getNumSimulations(Site site, CSRupture rupture) {
		Integer count = rvCountMap.get(rupture);
		
		if (count == null) {
			double[][][] vals;
			try {
				vals = amps2db.getAllIM_Values(runIDsMap.get(site), rd50_ims[0]);
			} catch (SQLException e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
			count = vals[rupture.getSourceID()][rupture.getRupID()].length;
			rvCountMap.put(rupture, count);
		}
		
		return count;
	}

	@Override
	public Collection<CSRupture> getRupturesForSite(Site site) {
		return siteRups.get(site);
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
	public double getMinimumCurvePlotRate() {
		return 0;
	}

}
