package scratch.kevin.cybershake;

import java.io.File;
import java.io.IOException;

import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.commons.util.DataUtils.MinMaxAveTracker;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.ERF2DB;

import scratch.kevin.simulators.RSQSimCatalog;
import scratch.kevin.simulators.RSQSimCatalog.Catalogs;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimProbEqkRup;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimSectBundledSource;

public class RSQSimHypocenterCheck {

	public static void main(String[] args) throws IOException {
		File localBaseDir = new File("/home/kevin/Simulators/catalogs");
		RSQSimCatalog catalog = Catalogs.BRUCE_2457.instance(localBaseDir);
		double minMag = 6.5;
		File mappingFile = new File(catalog.getCatalogDir(), "erf_mappings.bin");
		RSQSimSectBundledERF erf = new RSQSimSectBundledERF(mappingFile, null,
				catalog.getFaultModel(), catalog.getDeformationModel(), catalog.getU3SubSects(), catalog.getElements());
		
		int erfID = 42;
		int rupVarScenID = 8;
		
		erf.updateForecast();
		
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
		ERF2DB erf2db = new ERF2DB(db);
		
		double maxDist = 0d;
		
		MinMaxAveTracker track = new MinMaxAveTracker();
		
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			RSQSimSectBundledSource source = erf.getSource(sourceID);
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				RSQSimProbEqkRup rup = source.getRupture(rupID);
				Location hypo = rup.getHypocenterLocation();
				Location dbHypo = erf2db.getHypocenters(erfID, sourceID, rupID, rupVarScenID).values().iterator().next();
				
				if (!hypo.equals(dbHypo)) {
					double dist = LocationUtils.linearDistance(hypo, dbHypo);
					if (dist > maxDist) {
						maxDist = dist;
						System.out.println("New worst!");
						System.out.println("\tEvent: M"+(float)rup.getMag()+", ID: "+rup.getEventID());
						System.out.println("\tDistance: "+dist);
						System.out.println("\tActual: "+hypo);
						System.out.println("\tCalculated: "+dbHypo);
						System.out.println();
					}
					track.addValue(dist);
				}
			}
		}
		
		System.out.println("DONE. Max dist: "+maxDist);
		System.out.println(track);
		
		db.destroy();
	}

}
