package scratch.kevin.cybershake;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.commons.util.ClassUtils;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.CybershakeSiteInfo2DB;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;
import org.opensha.sha.faultSurface.EvenlyGridCenteredSurface;
import org.opensha.sha.faultSurface.EvenlyGriddedSurface;
import org.opensha.sha.faultSurface.FaultTrace;

public class U2_ERF_ChangeSearch {

	public static void main(String[] args) throws SQLException {
//		CyberShakeStudy study = CyberShakeStudy.STUDY_22_12_LF;
//		AbstractERF erf = study.getERF();
//		int erfID = study.runFetcher().fetch().get(0).getERFID();
		
//		int erfID = 35;
//		AbstractERF erf = MeanUCERF2_ToDB.createUCERF2ERF(true);
		int erfID = 36;
		AbstractERF erf = MeanUCERF2_ToDB.createUCERF2_200mERF(true);
//		AbstractERF erf = MeanUCERF2_ToDB.createUCERF2ERF(true);
		
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.ARCHIVE_HOST_NAME);
		
		int sourcesPerfectMatch = 0;
		int rupsPerfectMatch = 0;
		
		int totNumRups = 0;
		
		int numTraceFails = 0;
		int numProbFails = 0;
		int numMagFails = 0;
		
		List<Integer> badSourceIDs = new ArrayList<>();
		List<Double> badSourceDists = new ArrayList<>();
		List<Integer> badSourceNumBads = new ArrayList<>();
		
		Map<String, int[]> sourceTypeTracks = new LinkedHashMap<>();
		Map<String, Double> sourceMaxDistance = new LinkedHashMap<>();
		
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			ProbEqkSource source = erf.getSource(sourceID);
			
			ResultSet rs = db.selectData("SELECT max(Rupture_ID) FROM Ruptures WHERE ERF_ID="+erfID+" AND Source_ID="+sourceID);
			
			rs.next();
			int dbRupCount = rs.getInt(1)+1;
			
			totNumRups += source.getNumRuptures();
			if (dbRupCount != source.getNumRuptures()) {
				System.out.println("Rupture count mismatch for source "+sourceID+". "+source.getName());
				System.out.println("\tERF: "+source.getNumRuptures()+"; DB: "+dbRupCount);
				continue;
			}
			
			int numPerfect = 0;
			
			double maxDist = 0d;
			
			for (int rupID=0; rupID<Integer.min(source.getNumRuptures(), dbRupCount); rupID++) {
				rs = db.selectData("SELECT Mag,Prob,Start_Lat,Start_Lon,Start_Depth,End_Lat,End_Lon,End_Depth FROM Ruptures WHERE ERF_ID="
						+erfID+" AND Source_ID="+sourceID+" AND Rupture_ID="+rupID);
				rs.next();
				
				ProbEqkRupture rup = source.getRupture(rupID);
				
				double prob = rs.getDouble("Prob");
				double mag = rs.getDouble("Mag");
				
				boolean match = true;
				if ((float)mag != (float)rup.getMag()) {
					System.out.println("Rupture mag mismatch for source "+sourceID+". "+source.getName()
							+" Rupture "+rupID+": erf="+(float)rup.getMag()+", db="+(float)mag);
					match = false;
					numMagFails++;
				}
				
				if ((float)prob != (float)rup.getProbability()) {
					System.out.println("Rupture prob mismatch for source "+sourceID+". "+source.getName()
							+" Rupture "+rupID+": erf="+(float)rup.getProbability()+", db="+(float)prob);
					match = false;
					numProbFails++;
				}
				
				EvenlyGriddedSurface surf = (EvenlyGriddedSurface) rup.getRuptureSurface();
				surf = new EvenlyGridCenteredSurface(surf);
				
				Location myFirst = surf.get(0, 0);
				Location myLast = surf.get(0, surf.getNumCols()-1);
				
				Location dbFirst = new Location(rs.getDouble("Start_Lat"), rs.getDouble("Start_Lon"), rs.getDouble("Start_Depth"));
				Location dbLast = new Location(rs.getDouble("End_Lat"), rs.getDouble("End_Lon"), rs.getDouble("End_Depth"));
				
				maxDist = Math.max(LocationUtils.horzDistanceFast(myFirst, dbFirst), maxDist);
				maxDist = Math.max(LocationUtils.horzDistanceFast(myLast, dbLast), maxDist);
				
				if (!areSimilar(myFirst, dbFirst) || !areSimilar(myLast, dbLast)) {
					System.out.println("Trace mismatch for source "+sourceID+". "+source.getName()
						+" Rupture "+rupID+":"
								+ "\n\terf="+myFirst+", "+myLast
								+"\n\tdb="+dbFirst+", "+dbLast);
					match = false;
					numTraceFails++;
				}
				
				if (match)
					numPerfect++;
			}
			
			String sourceType = source.getClass().getSimpleName();
			if (sourceType.isBlank())
				sourceType = ClassUtils.getClassNameWithoutPackage(source.getClass());
			int[] sourceTrack = sourceTypeTracks.get(sourceType);
			if (sourceTrack == null) {
				sourceTrack = new int[2];
				sourceTypeTracks.put(sourceType, sourceTrack);
				sourceMaxDistance.put(sourceType, 0d);
			}
			sourceMaxDistance.put(sourceType, Math.max(sourceMaxDistance.get(sourceType), maxDist));
			if (numPerfect == source.getNumRuptures()) {
				sourcesPerfectMatch++;
				sourceTrack[1]++;
			} else {
				badSourceIDs.add(sourceID);
				badSourceNumBads.add(source.getNumRuptures() - numPerfect);
				badSourceDists.add(maxDist);
				sourceTrack[0]++;
				sourceTrack[1]++;
			}
			rupsPerfectMatch += numPerfect;
			
		}
		
		System.out.println("ERF "+erfID+" Results");
		
		DecimalFormat pDF = new DecimalFormat("0.##%");
		System.out.println(sourcesPerfectMatch+"/"+erf.getNumSources()+" sources are perfect ("
				+pDF.format((double)sourcesPerfectMatch/(double)erf.getNumSources())+")");
		System.out.println(rupsPerfectMatch+"/"+totNumRups+" ruptures are perfect ("
				+pDF.format((double)rupsPerfectMatch/(double)totNumRups)+")");
		
		System.out.println(numMagFails+"/"+totNumRups+" rupture magnitude failures ("
				+pDF.format((double)numMagFails/(double)totNumRups)+")");
		System.out.println(numProbFails+"/"+totNumRups+" rupture probability failures ("
				+pDF.format((double)numProbFails/(double)totNumRups)+")");
		System.out.println(numTraceFails+"/"+totNumRups+" rupture trace failures ("
				+pDF.format((double)numTraceFails/(double)totNumRups)+")");
		
		System.out.println("Failure rate by source type:");
		for (String sourceType : sourceTypeTracks.keySet()) {
			int[] sourceTrack = sourceTypeTracks.get(sourceType);
			double maxDist = sourceMaxDistance.get(sourceType);
			System.out.println("\t"+sourceType+": "+sourceTrack[0]+"/"+sourceTrack[1]+" failures ("
					+pDF.format((double)sourceTrack[0]/(double)sourceTrack[1])
					+"); furthest surface mismatch: "+(float)maxDist+" km away");
		}
		
		System.out.println("Bad sources:");
		for (int i=0; i<badSourceIDs.size(); i++) {
			int sourceID = badSourceIDs.get(i);
			ProbEqkSource source = erf.getSource(sourceID);
			int numBad = badSourceNumBads.get(i);
			double maxDist = badSourceDists.get(i);
			System.out.println(sourceID+". "+source.getName()+": "+numBad+"/"+source.getNumRuptures()+" ("
					+pDF.format((double)numBad/(double)source.getNumRuptures())
					+"); furthest surface mismatch: "+(float)maxDist+" km away");
		}
		
		db.destroy();
	}
	
	private static boolean areSimilar(Location loc1, Location loc2) {
		return LocationUtils.horzDistanceFast(loc1, loc2) < 2d;
	}

}
