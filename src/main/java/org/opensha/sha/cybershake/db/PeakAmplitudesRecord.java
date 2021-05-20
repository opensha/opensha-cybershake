package org.opensha.sha.cybershake.db;

import java.sql.ResultSet;
import java.sql.SQLException;

public class PeakAmplitudesRecord {
	
	private int runID;
	private int sourceID;
	private int rupID;
	private int rupVarID;
	private int imTypeID;
	private double value;
	
	public PeakAmplitudesRecord(int runID, int sourceID, int rupID,
			int rupVarID, int imTypeID, double value) {
		super();
		this.runID = runID;
		this.sourceID = sourceID;
		this.rupID = rupID;
		this.rupVarID = rupVarID;
		this.imTypeID = imTypeID;
		this.value = value;
	}

	public int getRunID() {
		return runID;
	}

	public int getSourceID() {
		return sourceID;
	}

	public int getRupID() {
		return rupID;
	}

	public int getRupVarID() {
		return rupVarID;
	}

	public int getImTypeID() {
		return imTypeID;
	}

	public double getValue() {
		return value;
	}
	
	public static PeakAmplitudesRecord fromResultSet(ResultSet rs) throws SQLException {
		int runID = rs.getInt("Run_ID");
		int sourceID = rs.getInt("Source_ID");
		int rupID = rs.getInt("Rupture_ID");
		int rupVarID = rs.getInt("Rup_Var_ID");
		int imTypeID = rs.getInt("IM_Type_ID");
		double value = rs.getDouble("IM_Value");
		
		return new PeakAmplitudesRecord(runID, sourceID, rupID, rupVarID, imTypeID, value);
	}

}
