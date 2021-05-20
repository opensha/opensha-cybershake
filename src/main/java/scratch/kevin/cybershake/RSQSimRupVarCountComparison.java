package scratch.kevin.cybershake;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.opensha.commons.data.function.HistogramFunction;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.ERF2DB;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.earthquake.ERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.magdist.IncrementalMagFreqDist;
import org.opensha.sha.simulators.RSQSimEvent;
import org.opensha.sha.simulators.utils.SimulatorUtils;

import com.google.common.base.Preconditions;

import scratch.kevin.simulators.RSQSimCatalog;
import scratch.kevin.simulators.RSQSimCatalog.Catalogs;

public class RSQSimRupVarCountComparison {

	public static void main(String[] args) throws IOException {
		RSQSimCatalog catalog = Catalogs.BRUCE_2585.instance(new File("/home/kevin/Simulators/catalogs"));
		
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
		int erfID = 36;
		int rupVarScenID = 7;
		
		ERF erf = MeanUCERF2_ToDB.createUCERF2_200mERF(true);
		ERF2DB erf2db = new ERF2DB(db);
		
		double minMag = 6.55;
		double deltaMag = 0.1;
		int numMag = (int)((8.55 - minMag) / deltaMag) + 1;
		HistogramFunction csRvCounts = new HistogramFunction(minMag, numMag, deltaMag);
		
		double minMagBinEdge = minMag - 0.5*deltaMag;
		double maxMagBinEdge = csRvCounts.getMaxX() + 0.5*deltaMag;
		
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			ProbEqkSource source = erf.getSource(sourceID);
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				ProbEqkRupture rup = source.getRupture(rupID);
				if (rup.getMag() < minMagBinEdge || rup.getMag() > maxMagBinEdge)
					continue;
				String sql = "SELECT max(Rup_Var_ID) FROM Rupture_Variations WHERE ERF_ID="+erfID+" AND Rup_Var_Scenario_ID="+rupVarScenID
						+" AND Source_ID="+sourceID+" AND Rupture_ID="+rupID;
				int numRVs;
				try {
					ResultSet rs = db.selectData(sql);
					rs.next();
					numRVs = rs.getInt(1);
				} catch (SQLException e1) {
					db.destroy();
					throw ExceptionUtils.asRuntimeException(e1);
				}
//				int numRVs = erf2db.getHypocenters(erfID, sourceID, rupID, rupVarScenID).size();
				Preconditions.checkState(numRVs > 0, "No RVs for source=%s rup=%s. SQL: %s", sourceID, rupID, sql);
				csRvCounts.add(csRvCounts.getClosestXIndex(rup.getMag()), (double)numRVs);
			}
		}
		db.destroy();
		
		System.out.println("CyberShake counts:\n"+csRvCounts);
		
		IncrementalMagFreqDist rsCounts = new IncrementalMagFreqDist(minMag, numMag, deltaMag);
		
		List<RSQSimEvent> events = catalog.loader().minMag(minMagBinEdge).load();
		double durationYears = SimulatorUtils.getSimulationDurationYears(events);
		for (RSQSimEvent e : events) {
			if (e.getMagnitude() > maxMagBinEdge)
				continue;
			rsCounts.add(rsCounts.getClosestXIndex(e.getMagnitude()), 1d);
		}
		
		System.out.println("RSQSim counts:\n"+rsCounts);
		
		System.out.println();
		
		System.out.println("Mag\t# CS\t# RSQSim\tRS Years Needed");
		for (int i=0; i<numMag; i++) {
			float mag = (float)(csRvCounts.getX(i) - 0.5*deltaMag);
			int numCS = (int)csRvCounts.getY(i);
			int numRS = (int)rsCounts.getY(i);
			double rsRate = (double)numRS/durationYears;
			float lenForParity = (float)((double)numCS/rsRate);
			
			System.out.println(mag+"\t"+numCS+"\t"+numRS+"\t"+lenForParity+" yrs");
		}
	}

}
