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
	private double vs30;
	private String vs30Source;
	
	public CybershakeRun(int runID, int siteID, int erfID, int sgtVarID, int rupVarScenID, int velModelID,
			Timestamp sgtTime, Timestamp ppTime, String sgtHost, String ppHost, double maxFreq, double lowFreqCutoff,
			double vs30, String vs30Source) {
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
		this.vs30 = vs30;
		this.vs30Source = vs30Source;
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
	
	public double getVs30() {
		return vs30;
	}

	public String getVs30Source() {
		return vs30Source;
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
		double vs30 = rs.getDouble("Vs30");
		String vs30Source;
		if (rs.wasNull()) {
			vs30 = Double.NaN;
			vs30Source = null;
		} else {
			Preconditions.checkState(vs30 > 0);
			vs30Source = rs.getString("Vs30_Source");
		}
		
		return new CybershakeRun(runID, siteID, erfID, sgtVarID, rupVarScenID, velModelID,
				sgtTime, ppTime, sgtHost, ppHost, maxFreq, lowFreqCutoff, vs30, vs30Source);
	}

}
