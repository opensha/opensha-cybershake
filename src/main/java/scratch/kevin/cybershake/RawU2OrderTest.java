package scratch.kevin.cybershake;

import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.UCERF2;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;
import org.opensha.sha.faultSurface.AbstractEvenlyGriddedSurface;
import org.opensha.sha.faultSurface.EvenlyGridCenteredSurface;
import org.opensha.sha.faultSurface.EvenlyGriddedSurface;
import org.opensha.sha.faultSurface.FaultTrace;

public class RawU2OrderTest {

	public static void main(String[] args) {
		MeanUCERF2 u2 = new MeanUCERF2();
		u2.setParameter(UCERF2.BACK_SEIS_NAME, UCERF2.BACK_SEIS_EXCLUDE);
		u2.setParameter(MeanUCERF2.CYBERSHAKE_DDW_CORR_PARAM_NAME, true);
		u2.updateForecast();
//		AbstractERF u2 = MeanUCERF2_ToDB.createUCERF2ERF(true);
		
		EvenlyGriddedSurface surf = (EvenlyGriddedSurface) u2.getRupture(188, 33).getRuptureSurface();
		surf = new EvenlyGridCenteredSurface(surf);
		FaultTrace trace = surf.getUpperEdge();
		System.out.println("Trace start: "+trace.first());
		System.out.println("Trace end: "+trace.last());
	}

}
