package scratch.kevin.cybershake.simulators.ruptures;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.geo.Location;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeRun;

import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import scratch.kevin.cybershake.simCompare.CSRupture;
import scratch.kevin.cybershake.simCompare.StudyRotDProvider;
import scratch.kevin.simCompare.SimulationRotDProvider;
import scratch.kevin.simulators.erf.RSQSimRotatedRuptureFakeERF;
import scratch.kevin.simulators.erf.RSQSimRotatedRuptureFakeERF.RSQSimRotatedRuptureSource;
import scratch.kevin.simulators.ruptures.rotation.RotatedRupVariabilityConfig.RotationSpec;

public class CSRotatedRupSimProv implements SimulationRotDProvider<RotationSpec> {
	
	private StudyRotDProvider prov;
	private RSQSimRotatedRuptureFakeERF erf;
	private BiMap<CSRupture, RotationSpec> rupRotationMap;
	
	public CSRotatedRupSimProv(CyberShakeStudy study, CachedPeakAmplitudesFromDB amps2db, double[] periods) {
		prov = new StudyRotDProvider(study, amps2db, periods, study.getName());
		
		Preconditions.checkState(study.getERF() instanceof RSQSimRotatedRuptureFakeERF);
		erf = (RSQSimRotatedRuptureFakeERF)study.getERF();
	}
	
	private synchronized void checkInitRotationMap(Site site) {
		if (rupRotationMap == null) {
			rupRotationMap = HashBiMap.create();
			
			CybershakeRun run = prov.getRun(site);
			
			System.out.println("Initializing rupture rotation mapping for "+erf.getNumSources()+" sources...");
			for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
//				System.out.println("Source "+sourceID);
				RSQSimRotatedRuptureSource source = erf.getSource(sourceID);
				List<RotationSpec> rotations = source.getRotations();
				for (int rupID=0; rupID<rotations.size(); rupID++) {
					CSRupture csRup = prov.getCSRupture(sourceID, rupID, run.getERFID(), run.getRupVarScenID(), 1);
					rupRotationMap.put(csRup, rotations.get(rupID));
				}
			}
			System.out.println("DONE initializing rupture rotation mapping");
		}
	}
	
	private RotationSpec rotationForRupture(Site site, CSRupture csRup) {
		checkInitRotationMap(site);
		return rupRotationMap.get(csRup);
	}
	
	private CSRupture rupForRotation(Site site, RotationSpec rotation) {
		if (site == null)
			site = rotation.site;
		checkInitRotationMap(site);
		CSRupture rup = rupRotationMap.inverse().get(rotation);
//		if (rup == null) {
//			System.out.println("NO RUP FOUND FOR ROTATION: "+rotation);
//			System.exit(0);
//		}
		return rup;
	}

	@Override
	public String getName() {
		return prov.getName();
	}

	@Override
	public DiscretizedFunc getRotD50(Site site, RotationSpec rupture, int index) throws IOException {
		return prov.getRotD50(site, rupForRotation(site, rupture), index);
	}

	@Override
	public DiscretizedFunc getRotD100(Site site, RotationSpec rupture, int index) throws IOException {
		return prov.getRotD100(site, rupForRotation(site, rupture), index);
	}

	@Override
	public DiscretizedFunc[] getRotD(Site site, RotationSpec rupture, int index) throws IOException {
		return prov.getRotD(site, rupForRotation(site, rupture), index);
	}

	@Override
	public DiscretizedFunc getRotDRatio(Site site, RotationSpec rupture, int index) throws IOException {
		return prov.getRotDRatio(site, rupForRotation(site, rupture), index);
	}

	@Override
	public int getNumSimulations(Site site, RotationSpec rupture) {
		return 1;
	}

	@Override
	public Location getHypocenter(RotationSpec rupture, int index) {
		return prov.getHypocenter(rupForRotation(null, rupture), index);
	}

	@Override
	public Collection<RotationSpec> getRupturesForSite(Site site) {
		List<RotationSpec> rotations = new ArrayList<>();
		for (CSRupture rup : prov.getRupturesForSite(site))
			rotations.add(rotationForRupture(site, rup));
		return rotations;
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
	public double getAnnualRate(RotationSpec rupture) {
		return prov.getAnnualRate(rupForRotation(null, rupture));
	}

	@Override
	public double getMinimumCurvePlotRate(Site site) {
		return prov.getMinimumCurvePlotRate(site);
	}

	@Override
	public double getMagnitude(RotationSpec rupture) {
		return prov.getMagnitude(rupForRotation(null, rupture));
	}

}
