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
	
	public enum Status {
		INITIAL("Initial"),
		SGT_STARTED("SGT Started"),
		SGT_ERROR("SGT Error"),
		SGT_GENERATED("SGT Generated"),
		PP_STARTED("PP Started"),
		PP_ERROR("PP Error"),
		CURVES_GENERATED("Curves Generated"),
		VERIFIED("Verified"),
		VERIFY_ERROR("Verify Error"),
		DELETE("Delete"),
		DELETED("Deleted");
		
		private String name;
		
		private Status(String name) {
			this.name = name;
		}
		
		public String getName() {
			return name;
		}
		
		public static Status forName(String name) {
			for (Status status : values())
				if (status.name.equals(name))
					return status;
			return null;
		}
		
		@Override
		public String toString() {
			return name;
		}
	}
	
	private int runID;
	private int siteID;
	private int erfID;
	private int sgtVarID;
	private int rupVarScenID;
	private int velModelID;
	private Timestamp sgtTime;
	private Timestamp ppTime;
	private Status status;
	private Timestamp statusTime;
	private String sgtHost;
	private String ppHost;
	private double maxFreq;
	private double lowFreqCutof;
	private Double modelVs30; // m/s, UCVM input to CyberShake, with floor applied
	private Double meshVsSurface; // m/s, as discretized in CyberShake mesh
	private Double minVs; // m/s
	private String vs30Source;
	private Double z10; // m
	private Double z25; // m
	
	public CybershakeRun(int runID, int siteID, int erfID, int sgtVarID, int rupVarScenID, int velModelID,
			Timestamp sgtTime, Timestamp ppTime, String sgtHost, String ppHost, Status status, Timestamp statusTime,
			double maxFreq, double lowFreqCutoff, Double modelVs30, Double meshVsSurface, Double minVs, String vs30Source, Double z10, Double z25) {
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
		this.meshVsSurface = meshVsSurface;
		this.minVs = minVs;
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
	
	public Status getStatus() {
		return status;
	}
	
	public Timestamp getStatusTimestamp() {
		return statusTime;
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
	public Double getMeshVsSurface() {
		return meshVsSurface;
	}
	
	public Double getMinimumVs() {
		return minVs;
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
				", SGT Time: " + format(getSGTTimestamp()) + ", SGT Host: " + sgtHost +
				", PP Time: " + format(getPPTimestamp()) + ", PP Host: " + ppHost +
				", Status: " + status + ", Status Time: " + format(getStatusTimestamp());
	}
	
	private String format(Timestamp timestamp) {
		if (timestamp == null)
			return "NULL";
		return format.format(timestamp);
	}
	
	public static CybershakeRun fromResultSet(ResultSet rs) throws SQLException {
		return fromResultSet(rs, "");
	}
	
	private static Timestamp parseTS(ResultSet rs, String field) {
		try {
			return rs.getTimestamp(field);
		} catch (SQLException e) {
			String message = e.getMessage();
			if (e.getCause() != null)
				message = e.getCause().getMessage();
			System.err.println("Could not parse timestamp for "+field+": "+message);
			return null;
		}
	}
	
	static CybershakeRun fromResultSet(ResultSet rs, String prefix) throws SQLException {
		int runID = rs.getInt(prefix+"Run_ID");
		int siteID = rs.getInt(prefix+"Site_ID");
		int erfID = rs.getInt(prefix+"ERF_ID");
		int sgtVarID = rs.getInt(prefix+"SGT_Variation_ID");
		int rupVarScenID = rs.getInt(prefix+"Rup_Var_Scenario_ID");
		int velModelID = rs.getInt(prefix+"Velocity_Model_ID");
		Timestamp sgtTime = parseTS(rs, prefix+"SGT_Time");
		Timestamp ppTime = parseTS(rs, prefix+"PP_Time");
		String sgtHost = rs.getString(prefix+"SGT_Host");
		String ppHost = rs.getString(prefix+"PP_Host");
		String statusStr = rs.getString(prefix+"Status");
		Status status = Status.forName(statusStr);
		Timestamp statusTime = parseTS(rs, prefix+"Status_Time");
		double maxFreq = rs.getDouble(prefix+"Max_Frequency");
		double lowFreqCutoff = rs.getDouble(prefix+"Low_Frequency_Cutoff");
		Double modelVs30 = rs.getDouble(prefix+"Model_Vs30");
		if (rs.wasNull())
			modelVs30 = null;
		Double meshVsSurface = rs.getDouble(prefix+"Mesh_Vs_Surface");
		if (rs.wasNull())
			meshVsSurface = null;
		Double minVs = rs.getDouble(prefix+"Minimum_Vs");
		if (rs.wasNull())
			minVs = null;
//		Double meshVsSurface = null;
//		Double minVs = null;
		String vs30Source = rs.getString(prefix+"Vs30_Source");
		Double z10 = rs.getDouble(prefix+"Z1_0");
		if (rs.wasNull())
			z10 = null;
		Double z25 = rs.getDouble(prefix+"Z2_5");
		if (rs.wasNull())
			z25 = null;
		
		return new CybershakeRun(runID, siteID, erfID, sgtVarID, rupVarScenID, velModelID,
				sgtTime, ppTime, sgtHost, ppHost, status, statusTime, maxFreq, lowFreqCutoff,
				modelVs30, meshVsSurface, minVs, vs30Source, z10, z25);
	}

}
