/*******************************************************************************
 * Copyright 2009 OpenSHA.org in partnership with
 * the Southern California Earthquake Center (SCEC, http://www.scec.org)
 * at the University of Southern California and the UnitedStates Geological
 * Survey (USGS; http://www.usgs.gov)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package org.opensha.sha.cybershake.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import com.google.common.base.Preconditions;

public class CybershakeRun {
	
	public static final DateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private int runID;
	private int siteID;
	private int erfID;
	private int sgtVarID;
	private int rupVarScenID;
	private int velModelID;
	private Timestamp sgtTime;
	private Timestamp ppTime;
	private String sgtHost;
	private String ppHost;
	private double maxFreq;
	private double lowFreqCutof;
	private Double modelVs30; // m/s, UCVM input to CyberShake, with floor applied
	private Double meshVs30; // m/s, as discretized in CyberShake mesh
	private String vs30Source;
	private Double z10; // m
	private Double z25; // m
	
	public CybershakeRun(int runID, int siteID, int erfID, int sgtVarID, int rupVarScenID, int velModelID,
			Timestamp sgtTime, Timestamp ppTime, String sgtHost, String ppHost, double maxFreq, double lowFreqCutoff,
			Double modelVs30, Double meshVs30, String vs30Source, Double z10, Double z25) {
		this.runID = runID;
		this.siteID = siteID;
		this.erfID = erfID;
		this.sgtVarID = sgtVarID;
		this.rupVarScenID = rupVarScenID;
		this.velModelID = velModelID;
		this.sgtTime = sgtTime;
		this.ppTime = ppTime;
		this.sgtHost = sgtHost;
		this.ppHost = ppHost;
		this.maxFreq = maxFreq;
		this.lowFreqCutof = lowFreqCutoff;
		this.modelVs30 = modelVs30;
		this.meshVs30 = meshVs30;
		this.vs30Source = vs30Source;
		this.z10 = z10;
		this.z25 = z25;
	}

	public int getRunID() {
		return runID;
	}

	public int getSiteID() {
		return siteID;
	}
	
	public int getERFID() {
		return erfID;
	}

	public int getSgtVarID() {
		return sgtVarID;
	}

	public int getRupVarScenID() {
		return rupVarScenID;
	}

	public Timestamp getSGTTimestamp() {
		return sgtTime;
	}
	
	public Timestamp getPPTimestamp() {
		return ppTime;
	}

	public String getSGTHost() {
		return sgtHost;
	}

	public String getPPHost() {
		return ppHost;
	}
	
	public int getVelModelID() {
		return velModelID;
	}
	
	public double getMaxFreq() {
		return maxFreq;
	}
	
	public double getLowFreqCutoff() {
		return lowFreqCutof;
	}
	
	/**
	 * Model Vs30 is from UCVM with any CyberShake Vs minimum threshold applied, calculated
	 * at model resolution (which will be higher resolution than the CyberShake mesh)
	 * @return model Vs30 (m/s), or null if not defined in the database
	 */
	public Double getModelVs30() {
		return modelVs30;
	}
	
	/**
	 * Model Vs30 is exactly what the CyberShake SGT code sees as Vs30. If grid spacig is larger
	 * than 30m, then the uppermost grid node is reported. Minimum Vs threshold is applied.
	 * @return mesh Vs30 (m/s), or null if not defined in the database
	 */
	public Double getMeshVs30() {
		return meshVs30;
	}

	public String getVs30Source() {
		return vs30Source;
	}

	/**
	 * Z1.0 for GMPE comparisons, calculated from the velocity model.
	 * @return Z1.0 (m), or null if not defined in the database
	 */
	public Double getZ10() {
		return z10;
	}
	
	/**
	 * Z2.5 for GMPE comparisons, calculated from the velocity model.
	 * @return Z2.5 (m), or null if not defined in the database
	 */
	public Double getZ25() {
		return z25;
	}

	@Override
	public String toString() {
		return "ID: " + getRunID() + ", Site_ID: " + getSiteID() + ", ERF_ID: " + getERFID() +
				", SGT Var ID: " + getSgtVarID() + ", Rup Var Scen ID: " + getRupVarScenID() +
				", Vel Model ID: " + getVelModelID() +
				", SGT Time: " + format.format(getSGTTimestamp()) + ", SGT Host: " + sgtHost +
				", PP Time: " + format.format(getPPTimestamp()) + ", PP Host: " + ppHost;
	}
	
	public static CybershakeRun fromResultSet(ResultSet rs) throws SQLException {
		int runID = rs.getInt("Run_ID");
		int siteID = rs.getInt("Site_ID");
		int erfID = rs.getInt("ERF_ID");
		int sgtVarID = rs.getInt("SGT_Variation_ID");
		int rupVarScenID = rs.getInt("Rup_Var_Scenario_ID");
		int velModelID = rs.getInt("Velocity_Model_ID");
		Timestamp sgtTime = rs.getTimestamp("SGT_Time");
		Timestamp ppTime = rs.getTimestamp("PP_Time");
		String sgtHost = rs.getString("SGT_Host");
		String ppHost = rs.getString("PP_Host");
		double maxFreq = rs.getDouble("Max_Frequency");
		double lowFreqCutoff = rs.getDouble("Low_Frequency_Cutoff");
		Double modelVs30 = rs.getDouble("Model_Vs30");
		if (rs.wasNull())
			modelVs30 = null;
		Double meshVs30 = rs.getDouble("Mesh_Vs30");
		if (rs.wasNull())
			meshVs30 = null;
		String vs30Source = rs.getString("Vs30_Source");
		Double z10 = rs.getDouble("Z1_0");
		if (rs.wasNull())
			z10 = null;
		Double z25 = rs.getDouble("Z2_5");
		if (rs.wasNull())
			z25 = null;
		
		return new CybershakeRun(runID, siteID, erfID, sgtVarID, rupVarScenID, velModelID,
				sgtTime, ppTime, sgtHost, ppHost, maxFreq, lowFreqCutoff,
				modelVs30, meshVs30, vs30Source, z10, z25);
	}

}
