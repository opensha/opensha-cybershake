package scratch.kevin.cybershake;

import org.opensha.commons.geo.Location;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.UCERF2;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;
import org.opensha.sha.faultSurface.RuptureSurface;

public class SourceDistDebug {

	public static void main(String[] args) {
//		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
//		AbstractERF erf = MeanUCERF2_ToDB.createUCERF2ERF();
//		
//		int siteID = 68;
//		Location loc = new SiteInfo2DB(db).getLocationForSiteID(siteID);
//		
//		ProbEqkRupture rup = erf.getSource(232).getRupture(8);
//		System.out.println("Mag: "+rup.getMag());
//		RuptureSurface surf = rup.getRuptureSurface();
//		System.out.println("Trace: "+surf.getUpperEdge());
//		System.out.println("rJB: "+surf.getDistanceJB(loc));
//		System.out.println("rRup: "+surf.getDistanceRup(loc));
//		
//		db.destroy();
		
		Location loc = new Location(33.93088, -118.17881);
		
		AbstractERF eqkRupForecast = new MeanUCERF2();
		
		// exclude Background seismicity
		eqkRupForecast.getAdjustableParameterList().getParameter(
				UCERF2.BACK_SEIS_NAME).setValue(UCERF2.BACK_SEIS_EXCLUDE);

		// Rup offset
		eqkRupForecast.getAdjustableParameterList().getParameter(
				MeanUCERF2.RUP_OFFSET_PARAM_NAME).setValue(
						Double.valueOf(5.0));

		// Cybershake DDW(down dip correction) correction
		eqkRupForecast.getAdjustableParameterList().getParameter(
				MeanUCERF2.CYBERSHAKE_DDW_CORR_PARAM_NAME).setValue(
						Boolean.valueOf(true));

		// Set Poisson Probability model
		eqkRupForecast.getAdjustableParameterList().getParameter(
				UCERF2.PROB_MODEL_PARAM_NAME).setValue(
						UCERF2.PROB_MODEL_POISSON);

		// duration
		eqkRupForecast.getTimeSpan().setDuration(1.0);

		System.out.println("Updating Forecast...");
		eqkRupForecast.updateForecast();
		
		ProbEqkSource source = eqkRupForecast.getSource(232);
		System.out.println("Source: "+source.getName());
		System.out.println("source is a "+source.getClass().getName());
		ProbEqkRupture rup = source.getRupture(8);
		System.out.println("rup is a "+rup.getClass().getName());
		
		System.out.println("Mag: "+rup.getMag());
		RuptureSurface surf = rup.getRuptureSurface();
		System.out.println("Trace: "+surf.getUpperEdge());
		System.out.println("rJB: "+surf.getDistanceJB(loc));
		System.out.println("rRup: "+surf.getDistanceRup(loc));
		
		
	}

}
