package org.opensha.sha.cybershake.db;



import org.opensha.sha.earthquake.rupForecastImpl.Frankel02.Frankel02_AdjustableEqkRupForecast;


/**
 * This Class creates an instances of Frankel02/NHSMP02 ERF to insert into the database
 * @author vipingupta
 *
 */
public class NSHMP2002_ToDB extends ERF2DB {
	
	public NSHMP2002_ToDB(DBAccess db){
		super(db);
		createFrankel02ERF();
	}
	
	 /**
	  * Create NSHMP 02 ERF instance
	  *
	  */
	  private void createFrankel02ERF() {

	    
		eqkRupForecast = new Frankel02_AdjustableEqkRupForecast();
		
		// exclude Background seismicity
	    eqkRupForecast.getAdjustableParameterList().getParameter(
	        Frankel02_AdjustableEqkRupForecast.
	        BACK_SEIS_NAME).setValue(Frankel02_AdjustableEqkRupForecast.
	                                 BACK_SEIS_EXCLUDE);
	    // Stirling's representation
	    eqkRupForecast.getAdjustableParameterList().getParameter(
	      Frankel02_AdjustableEqkRupForecast.FAULT_MODEL_NAME).setValue(
	    		  Frankel02_AdjustableEqkRupForecast.FAULT_MODEL_STIRLING);
	    // Rup offset
	    eqkRupForecast.getAdjustableParameterList().getParameter(
	      Frankel02_AdjustableEqkRupForecast.RUP_OFFSET_PARAM_NAME).setValue(
	        Double.valueOf(5.0));
	    // duration
	    eqkRupForecast.getTimeSpan().setDuration(1.0);
	    eqkRupForecast.updateForecast();
	  }
}
