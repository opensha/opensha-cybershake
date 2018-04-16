package scratch.kevin.cybershake;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.geo.Location;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.ERF2DB;

import scratch.kevin.simulators.RSQSimCatalog;
import scratch.kevin.simulators.RSQSimCatalog.Catalogs;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimProbEqkRup;
import scratch.kevin.simulators.erf.RSQSimSectBundledERF.RSQSimSectBundledSource;

public class RSQSimRuptureVariationInsert {

	public static void main(String[] args) throws IOException {
		File localBaseDir = new File("/home/kevin/Simulators/catalogs");
		RSQSimCatalog catalog = Catalogs.BRUCE_2585_1MYR.instance(localBaseDir);
		double minMag = 6.5;
		File mappingFile = new File(catalog.getCatalogDir(), "erf_mappings.bin");
		RSQSimSectBundledERF erf = new RSQSimSectBundledERF(mappingFile, null,
				catalog.getFaultModel(), catalog.getDeformationModel(), catalog.getU3SubSects(), catalog.getElements());
		
		int erfID = 48;
		int rupVarScenID = 8;
		
		erf.updateForecast();
		
		DBAccess db = Cybershake_OpenSHA_DBApplication.getAuthenticatedDBAccess(
				true, true, Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
		
		int bundleSize = 1000;
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			RSQSimSectBundledSource source = erf.getSource(sourceID);
			List<List<Integer>> bundles = new ArrayList<>();
			List<Integer> curBundle = null;
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				if (curBundle == null) {
					curBundle = new ArrayList<>();
					bundles.add(curBundle);
				}
				curBundle.add(rupID);
				if (curBundle.size() == bundleSize)
					curBundle = null;
			}
			System.out.println("Inserting "+bundles.size()+" bundle with "+source.getNumRuptures()
				+" ruptures for source "+sourceID);
			for (List<Integer> bundle : bundles) {
				StringBuffer sql = new StringBuffer();
				sql.append("INSERT INTO Rupture_Variations (ERF_ID,Rup_Var_Scenario_ID,Source_ID,Rupture_ID,"
						+ "Rup_Var_ID,Rup_Var_LFN,Hypocenter_Lat,Hypocenter_Lon,Hypocenter_Depth) VALUES");
				boolean first = true;
				for (int rupID : bundle) {
					RSQSimProbEqkRup rup = source.getRupture(rupID);
					String lfn = "e"+erfID+"_rv"+rupVarScenID+"_"+sourceID+"_"+rupID+"_event"+rup.getEventID()+".srf";
					Location hypo = rup.getHypocenterLocation();
					if (!first)
						sql.append(",");
					sql.append("\n('"+erfID+"','"+rupVarScenID+"','"+sourceID+"','"+rupID+"','"+0+"','"+lfn+"','"
							+hypo.getLatitude()+"','"+hypo.getLongitude()+"','"+hypo.getDepth()+"')");
					first = false;
				}
				try {
					db.insertUpdateOrDeleteData(sql.toString());
				} catch (SQLException e) {
					e.printStackTrace();
					db.destroy();
					System.exit(1);
				}
			}
		}
		
		db.destroy();
		System.exit(0);
	}

}
