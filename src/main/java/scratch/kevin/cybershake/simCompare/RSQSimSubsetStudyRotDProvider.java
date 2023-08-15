package scratch.kevin.cybershake.simCompare;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.geo.Location;
import org.opensha.sha.imr.param.IntensityMeasureParams.DurationTimeInterval;

import com.google.common.base.Preconditions;

import scratch.kevin.simCompare.SimulationRotDProvider;
import scratch.kevin.simulators.RSQSimCatalog;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimProbEqkRup;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimSectBundledSource;

public class RSQSimSubsetStudyRotDProvider implements SimulationRotDProvider<CSRupture> {
	
	private SimulationRotDProvider<CSRupture> prov;
	private RSQSimSectBundledERF erf;
	private int minEventID;
	
	private double catDurationYears;
	private double rateEach;

	public RSQSimSubsetStudyRotDProvider(SimulationRotDProvider<CSRupture> prov, RSQSimSectBundledERF erf,
			RSQSimCatalog catalog, double skipYears) throws IOException {
		this.prov = prov;
		this.erf = erf;
		minEventID = catalog.loader().skipYears(skipYears).maxDuration(10000).load().get(0).getID();
		System.out.println("minEventID="+minEventID);
		double firstKeptEventTime = Double.POSITIVE_INFINITY;
		double lastKeptEventTime = Double.NEGATIVE_INFINITY;
		int numOrigEvents = 0;
		int numKept = 0;
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			RSQSimSectBundledSource source = erf.getSource(sourceID);
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				numOrigEvents++;
				RSQSimProbEqkRup rup = source.getRupture(rupID);
				if (rup.getEventID() >= minEventID) {
					numKept++;
					double time = rup.getTimeYears();
					firstKeptEventTime = Math.min(firstKeptEventTime, time);
					lastKeptEventTime = Math.max(lastKeptEventTime, time);
				}
			}
		}
		catDurationYears = lastKeptEventTime - firstKeptEventTime;
		System.out.println("Kept "+numKept+"/"+numOrigEvents+" events");
		System.out.println("New duration: "+(float)catDurationYears);
		rateEach = 1d/catDurationYears;
	}
	
	public RSQSimSectBundledERF getERF() {
		return erf;
	}

	@Override
	public String getName() {
		return prov.getName();
	}
	
	private boolean included(CSRupture rupture) {
		return erf.getRupture(rupture.getSourceID(), rupture.getRupID()).getEventID() >= minEventID;
	}

	@Override
	public DiscretizedFunc getRotD50(Site site, CSRupture rupture, int index) throws IOException {
		Preconditions.checkState(included(rupture));
		return prov.getRotD50(site, rupture, index);
	}

	@Override
	public DiscretizedFunc getRotD100(Site site, CSRupture rupture, int index) throws IOException {
		Preconditions.checkState(included(rupture));
		return prov.getRotD100(site, rupture, index);
	}

	@Override
	public DiscretizedFunc[] getRotD(Site site, CSRupture rupture, int index) throws IOException {
		Preconditions.checkState(included(rupture));
		return prov.getRotD(site, rupture, index);
	}

	@Override
	public DiscretizedFunc getRotDRatio(Site site, CSRupture rupture, int index) throws IOException {
		Preconditions.checkState(included(rupture));
		return prov.getRotDRatio(site, rupture, index);
	}

	@Override
	public double getPGV(Site site, CSRupture rupture, int index) throws IOException {
		Preconditions.checkState(included(rupture));
		return prov.getPGV(site, rupture, index);
	}

	@Override
	public double getPGA(Site site, CSRupture rupture, int index) throws IOException {
		Preconditions.checkState(included(rupture));
		return prov.getPGA(site, rupture, index);
	}

	@Override
	public double getDuration(Site site, CSRupture rupture, DurationTimeInterval interval, int index)
			throws IOException {
		Preconditions.checkState(included(rupture));
		return prov.getDuration(site, rupture, interval, index);
	}

	@Override
	public int getNumSimulations(Site site, CSRupture rupture) {
		Preconditions.checkState(included(rupture));
		return prov.getNumSimulations(site, rupture);
	}

	@Override
	public Location getHypocenter(CSRupture rupture, int index) {
		Preconditions.checkState(included(rupture));
		return prov.getHypocenter(rupture, index);
	}

	@Override
	public Collection<CSRupture> getRupturesForSite(Site site) {
		List<CSRupture> ret = new ArrayList<>();
		for (CSRupture rup : prov.getRupturesForSite(site)) {
			if (included(rup)) {
				rup.setRate(rateEach);
				ret.add(rup);
			}
		}
		return ret;
	}

	@Override
	public boolean hasRotD50() {
		return prov.hasRotD50();
	}

	@Override
	public boolean hasRotD100() {
		return prov.hasRotD100();
	}

	@Override
	public boolean hasPGV() {
		return prov.hasPGV();
	}

	@Override
	public boolean hasPGA() {
		return prov.hasPGA();
	}

	@Override
	public boolean hasDurations() {
		return prov.hasDurations();
	}

	@Override
	public double getAnnualRate(CSRupture rupture) {
		Preconditions.checkState(included(rupture));
		return rateEach;
	}

	@Override
	public double getMinimumCurvePlotRate(Site site) {
		return rateEach;
	}

	@Override
	public double getMagnitude(CSRupture rupture) {
		Preconditions.checkState(included(rupture));
		return prov.getMagnitude(rupture);
	}

	@Override
	public double getRake(CSRupture rupture) {
		Preconditions.checkState(included(rupture));
		return prov.getRake(rupture);
	}

}
