package scratch.kevin.cybershake;

import org.opensha.commons.geo.Location;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.ERF2DB;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.cybershake.openshaAPIs.CyberShakeEqkRupture;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.faultSurface.EvenlyGriddedSurface;
import org.opensha.sha.faultSurface.RuptureSurface;

import com.google.common.base.Preconditions;

public class PVSurfaceDebug {

	public static void main(String[] args) {
		int sourceID = 232;
//		int rupID = 344;
		int rupID = 459;
		
		AbstractERF origU2 = MeanUCERF2_ToDB.createUCERF2ERF();
		ProbEqkSource source35 = origU2.getSource(sourceID);
		EvenlyGriddedSurface surf35 = (EvenlyGriddedSurface)source35.getRupture(rupID).getRuptureSurface();
		ProbEqkSource source36 = MeanUCERF2_ToDB.createUCERF2_200mERF(true).getSource(sourceID);
		EvenlyGriddedSurface surf36 = (EvenlyGriddedSurface)source36.getRupture(rupID).getRuptureSurface();
		
		System.out.println("Surface for 35");
		for (Location loc : surf35.getUpperEdge())
			System.out.println("\t"+loc);
		System.out.println("\n\nSurface for 36");
		for (Location loc : surf36.getUpperEdge())
			System.out.println("\t"+loc);
		
		System.out.println("(0,0) for 35:");
		System.out.println("\t"+surf35.get(0, 0));
		
		System.out.println("(0,0) for 36:");
		System.out.println("\t"+surf36.get(0, 0));
		
		System.out.println("35 name: "+source35.getName()+" ("+source35.getNumRuptures()+") "+source35.getClass().getName());
		System.out.println("36 name: "+source36.getName()+" ("+source36.getNumRuptures()+") "+source36.getClass().getName());
		
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
		
		ERF2DB erf2db = new ERF2DB(db);
		
		for (int s=0; s<origU2.getNumSources(); s++) {
			System.out.println("Checking source "+s+". "+origU2.getSource(s).getName());
			for (int r=0; r<origU2.getNumRuptures(s); r++) {
				CyberShakeEqkRupture rup35 = erf2db.getRupture(35, s, r);
				CyberShakeEqkRupture rup36 = erf2db.getRupture(36, s, r);
				
				Preconditions.checkState((float)rup35.getMag() == (float)rup36.getMag());
				Preconditions.checkState((float)rup35.getProbability() == (float)rup36.getProbability());
				Preconditions.checkState((float)rup35.getAveRake() == (float)rup36.getAveRake());
			}
		}
		System.out.println("DONE.");
		db.destroy();
	}

}
