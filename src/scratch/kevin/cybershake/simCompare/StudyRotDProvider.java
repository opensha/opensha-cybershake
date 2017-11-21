package scratch.kevin.cybershake.simCompare;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.LightFixedXFunc;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;

import com.google.common.base.Preconditions;
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
	
	private Table<CSRupture, Site, DiscretizedFunc[]> rd50Table;
	private Table<CSRupture, Site, DiscretizedFunc[]> rdRatioTable;
	private Table<CSRupture, Site, DiscretizedFunc[][]> rdTable;
	
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
			Preconditions.checkState((float)rd50_ims[i].getVal() == (float)rd100_ims[i].getVal());
			Preconditions.checkState((float)rd50_ims[i].getVal() == (float)periods[i]);
		}
		
		rd50Table = HashBasedTable.create();
		if (hasRotD100()) {
			rdTable = HashBasedTable.create();
			rdRatioTable = HashBasedTable.create();
		}
		
		rvCountMap = new HashMap<>();
	}

	@Override
	public String getName() {
		return name;
	}
	
	private DiscretizedFunc getSpectra(Site site, CSRupture rup, CybershakeIM[] ims, int index) {
		double[] rds = new double[periods.length];
		int runID = runIDsMap.get(site);
		for (int i=0; i<rds.length; i++) {
			try {
				double[][][] amps = amps2db.getAllIM_Values(runID, ims[i]);
				double[] myAmps = amps[rup.getSourceID()][rup.getRupID()];
				Preconditions.checkNotNull(myAmps);
				Preconditions.checkState(index < myAmps.length);
				rds[i] = myAmps[index] / HazardCurveComputation.CONVERSION_TO_G;
			} catch (SQLException e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
		}
		return new LightFixedXFunc(periods, rds);
	}

	@Override
	public DiscretizedFunc getRotD50(Site site, CSRupture rupture, int index) throws IOException {
		if (rdTable != null)
			return getRotD(site, rupture, index)[0];
		if (rd50Table.contains(rupture, site))
			return rd50Table.get(rupture, site)[index];
		synchronized (rd50Table) {
			// repeat here as could have been populated while waiting for the lock
			if (rd50Table.contains(rupture, site))
				return rd50Table.get(rupture, site)[index];
			DiscretizedFunc[] spectras = new DiscretizedFunc[getNumSimulations(site, rupture)];
			for (int i=0; i<spectras.length; i++)
				spectras[i] = getSpectra(site, rupture, rd50_ims, i);
			rd50Table.put(rupture, site, spectras);
			return spectras[index];
		}
	}

	@Override
	public DiscretizedFunc getRotD100(Site site, CSRupture rupture, int index) throws IOException {
		return getRotD(site, rupture, index)[1];
	}

	@Override
	public DiscretizedFunc[] getRotD(Site site, CSRupture rupture, int index) throws IOException {
		if (rdTable.contains(rupture, site))
			return rdTable.get(rupture, site)[index];
		synchronized (rd50Table) {
			// repeat here as could have been populated while waiting for the lock
			if (rdTable.contains(rupture, site))
				return rdTable.get(rupture, site)[index];
			DiscretizedFunc[][] spectras = new DiscretizedFunc[getNumSimulations(site, rupture)][2];
			for (int i=0; i<spectras.length; i++) {
				spectras[i][0] = getSpectra(site, rupture, rd50_ims, i);
				spectras[i][1] = getSpectra(site, rupture, rd100_ims, i);
			}
			rdTable.put(rupture, site, spectras);
			return spectras[index];
		}
	}

	@Override
	public DiscretizedFunc getRotDRatio(Site site, CSRupture rupture, int index) throws IOException {
		if (rdRatioTable.contains(rupture, site))
			return rdRatioTable.get(rupture, site)[index];
		synchronized (rdTable) {
			// repeat here as could have been populated while waiting for the lock
			if (rdRatioTable.contains(rupture, site))
				return rdRatioTable.get(rupture, site)[index];
			DiscretizedFunc[] ratios = new DiscretizedFunc[getNumSimulations(site, rupture)];
			for (int i=0; i<ratios.length; i++) {
				DiscretizedFunc[] spectras = getRotD(site, rupture, i);
				ratios[i] = SimulationRotDProvider.calcRotDRatio(spectras);
			}
			rdRatioTable.put(rupture, site, ratios);
			return ratios[index];
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

}
