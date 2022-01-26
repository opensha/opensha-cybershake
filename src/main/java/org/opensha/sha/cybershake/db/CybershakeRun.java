package org.opensha.sha.cybershake.db;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.stream.Collectors;

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
	private Integer meshVsitopID; // ID of algorithm used to determine this value in Mesh_Vsitop_Metadata table
	private Double meshVsitop; // m/s, as discretized in CyberShake mesh
	private Double minVs; // m/s
	private String vs30Source;
	private Double z10; // m
	private Double z25; // m
	
	public CybershakeRun(int runID, int siteID, int erfID, int sgtVarID, int rupVarScenID, int velModelID,
			Timestamp sgtTime, Timestamp ppTime, String sgtHost, String ppHost, Status status, Timestamp statusTime,
			double maxFreq, double lowFreqCutoff, Double modelVs30, Double meshVsitop, Integer meshVsitopID, Double minVs, String vs30Source, Double z10, Double z25) {
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
		this.meshVsitop = meshVsitop;
		this.meshVsitopID = meshVsitopID;
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
	 * Mesh Vsitop is the representative value of Vs30 from the SGT code mesh. This value has been
	 * computed different ways in different CyberShake studies, and metadata for the exact algorithm
	 * used can be found in the Mesh_Vsitop_Metadata table (see getMeshVsitopID()).
	 * @return mesh Vs30 (m/s), or null if not defined in the database
	 */
	public Double getMeshVsitop() {
		return meshVsitop;
	}
	
	/**
	 * ID number in the Mesh_Vsitop_Metadata table, which describes the algorithm used to calculate
	 * the Mesh_Vsitop value.
	 * @return
	 */
	public Integer getMeshVsitopID() {
		return meshVsitopID;
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
		
		// we have changed the following fields a few times
		// 
		// use metadata to figure out which columns this database has and skip if missing. this will keep things working
		// with older sqlite files
		ResultSetMetaData rsmd = rs.getMetaData();
		HashSet<String> colLabels = new HashSet<>();
		for (int i=0; i<rsmd.getColumnCount(); i++) {
			String colLabel = rsmd.getColumnLabel(i+1);
			int dotIndex = colLabel.lastIndexOf('.');
			if (dotIndex >=0 )
				colLabel = colLabel.substring(dotIndex+1);
			colLabels.add(colLabel);
		}
		
		Double modelVs30 = getDoubleNullFallback(rs, colLabels, prefix+"Model_Vs30");
		Double meshVsitop = getDoubleNullFallback(rs, colLabels, prefix+"Mesh_Vsitop");
		Integer meshVsitopID = getIntNullFallback(rs, colLabels, prefix+"Mesh_Vsitop_ID");
		Double minVs = getDoubleNullFallback(rs, colLabels, prefix+"Minimum_Vs");
		Double z10 = getDoubleNullFallback(rs, colLabels, prefix+"Z1_0");
		Double z25 = getDoubleNullFallback(rs, colLabels, prefix+"Z2_5");
		String vs30Source = hasField(colLabels, prefix+"Vs30_Source") ? rs.getString(prefix+"Vs30_Source") : null;
		
		return new CybershakeRun(runID, siteID, erfID, sgtVarID, rupVarScenID, velModelID,
				sgtTime, ppTime, sgtHost, ppHost, status, statusTime, maxFreq, lowFreqCutoff,
				modelVs30, meshVsitop, meshVsitopID, minVs, vs30Source, z10, z25);
	}
	
	private static final HashSet<String> warned_labels = new HashSet<>();
	private static Double getDoubleNullFallback(ResultSet rs, HashSet<String> colLabels, String colLabel) throws SQLException {
		if (!hasField(colLabels, colLabel))
			return null;
		double ret = rs.getDouble(colLabel);
		if (rs.wasNull())
			return null;
		return ret;
	}
	private static Integer getIntNullFallback(ResultSet rs, HashSet<String> colLabels, String colLabel) throws SQLException {
		if (!hasField(colLabels, colLabel))
			return null;
		int ret = rs.getInt(colLabel);
		if (rs.wasNull())
			return null;
		return ret;
	}
	private static boolean hasField(HashSet<String> colLabels, String colLabel) {
		int dotIndex = colLabel.lastIndexOf('.');
		if (dotIndex >=0 )
			colLabel = colLabel.substring(dotIndex+1);
		if (!colLabels.contains(colLabel)) {
			synchronized (warned_labels) {
				if (!warned_labels.contains(colLabel)) {
					System.err.println("WARNING: Runs table is missing column '"+colLabel+"'. This is likely due to a "
							+ "schema change (using new code with an old DB, or old code with a new DB. Skipping field.");
					System.err.println("\tAvailable columns: "+colLabels.stream().collect(Collectors.joining(",")));
					warned_labels.add(colLabel);
				}
			}
			return false;
		}
		return true;
	}

}
