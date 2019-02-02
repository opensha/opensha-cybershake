package org.opensha.sha.cybershake.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeRun.Status;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

public class RunIDFetcher {
	
	// runs table fields
	private List<Integer> siteIDs;
	private Integer erfID;
	private Integer sgtVarID;
	private Integer rupVarScenID;
	private Integer velModelID;
	private Timestamp minTime;
	private Timestamp maxTime;
	private String sgtHost;
	private String ppHost;
	private Status status;
	private Integer studyID;
	
	// external fields
	private boolean hasCurves;
	private int[] hazardDatasetIDs;
	private Integer imTypeID;
	
	private DBAccess db;
	
	public RunIDFetcher(DBAccess db) {
		this.db = db;
	}
	
	public synchronized RunIDFetcher forSiteIDs(int... ids) {
		if (ids.length == 0)
			return this;
		if (siteIDs == null)
			siteIDs = new ArrayList<>();
		for (int id : ids)
			siteIDs.add(id);
		return this;
	}
	
	public synchronized RunIDFetcher forSiteIDs(List<Integer> siteIDs) {
		return forSiteIDs(Ints.toArray(siteIDs));
	}
	
	public RunIDFetcher forSiteNames(String... siteShortNames) {
		SiteInfo2DB sites2db = new SiteInfo2DB(db);
		for (String shortName : siteShortNames)
			forSiteIDs(sites2db.getSiteId(shortName));
		return this;
	}
	
	public RunIDFetcher forSiteNames(List<String> siteShortNames) {
		return forSiteNames(siteShortNames.toArray(new String[0]));
	}
	
	public RunIDFetcher forERF(int erfID) {
		this.erfID = erfID;
		return this;
	}
	
	public RunIDFetcher forSGTVarID(int sgtVarID) {
		this.sgtVarID = sgtVarID;
		return this;
	}
	
	public RunIDFetcher forRupVarScenID(int rupVarScenID) {
		this.rupVarScenID = rupVarScenID;
		return this;
	}
	
	public RunIDFetcher forVelModelID(int velModelID) {
		this.velModelID = velModelID;
		return this;
	}
	
	public RunIDFetcher forMinTime(Timestamp minTime) {
		this.minTime = minTime;
		return this;
	}
	
	public RunIDFetcher forMinTime(Date minTime) {
		return this.forMinTime(new Timestamp(minTime.getTime()));
	}
	
	public RunIDFetcher forMaxTime(Timestamp maxTime) {
		this.maxTime = maxTime;
		return this;
	}
	
	public RunIDFetcher forMaxTime(Date maxTime) {
		return this.forMaxTime(new Timestamp(maxTime.getTime()));
	}
	
	public RunIDFetcher forSGTHost(String sgtHost) {
		this.sgtHost = sgtHost;
		return this;
	}
	
	public RunIDFetcher forPPHost(String ppHost) {
		this.ppHost = ppHost;
		return this;
	}
	
	public RunIDFetcher forStatus(Status status) {
		this.status = status;
		return this;
	}
	
	public RunIDFetcher forStudyID(int studyID) {
		this.studyID = studyID;
		return this;
	}
	
	public RunIDFetcher hasHazardCurves() {
		return hasHazardCurves(new int[0], null);
	}
	
	public RunIDFetcher hasHazardCurves(int hazardDatasetID) {
		return hasHazardCurves(hazardDatasetID, null);
	}
	
	public RunIDFetcher hasHazardCurves(int[] hazardDatasetIDs) {
		return hasHazardCurves(hazardDatasetIDs, null);
	}
	
	public RunIDFetcher hasHazardCurves(Integer hazardDatasetID, Integer imTypeID) {
		int[] datasetArray = null;
		if (hazardDatasetID != null)
			datasetArray = new int[] { hazardDatasetID };
		return hasHazardCurves(datasetArray, imTypeID);
	}
	
	public RunIDFetcher hasHazardCurves(int[] hazardDatasetIDs, Integer imTypeID) {
		this.hasCurves = true;
		if (hazardDatasetIDs != null && hazardDatasetIDs.length > 0)
			this.hazardDatasetIDs = hazardDatasetIDs;
		this.imTypeID = imTypeID;
		return this;
	}
	
	public List<CybershakeRun> fetch() {
		String sql = buildSelectSQL();
		
		List<CybershakeRun> runs = new ArrayList<>();
		
		HashSet<Integer> runIDs = new HashSet<>();
		
		try {
			ResultSet rs = db.selectData(sql);
			boolean valid = rs.first();
			
			while (valid) {
				CybershakeRun run = CybershakeRun.fromResultSet(rs, "R.");
				Preconditions.checkState(!runIDs.contains(run.getRunID()), "Duplicate runID=%s. SQL:%s", run.getRunID(), sql);
				runs.add(run);
				runIDs.add(run.getRunID());
				
				valid = rs.next();
			}
			
			rs.close();
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
		
		return runs;
	}
	
	public String buildSelectSQL() {
		String sql = "SELECT DISTINCT R.* FROM CyberShake_Runs R";
		if (hasCurves)
			sql += " JOIN Hazard_Curves C ON R.Run_ID=C.Run_ID";
		List<String> wheres = new ArrayList<>();
		if (siteIDs != null && !siteIDs.isEmpty()) {
			if (siteIDs.size() == 1)
				wheres.add("R.Site_ID="+siteIDs.get(0));
			else
				wheres.add("R.Site_ID IN ('"+Joiner.on("','").join(siteIDs)+"')");
		}
		if (erfID != null)
			wheres.add("R.ERF_ID="+erfID);
		if (sgtVarID != null)
			wheres.add("R.SGT_Variation_ID="+sgtVarID);
		if (rupVarScenID != null)
			wheres.add("R.Rup_Var_Scenario_ID="+rupVarScenID);
		if (velModelID != null)
			wheres.add("R.Velocity_Model_ID="+velModelID);
		if (minTime != null)
			wheres.add("R.Status_Time>='"+CybershakeRun.format.format(minTime)+"'");
		if (maxTime != null)
			wheres.add("R.Status_Time<='"+CybershakeRun.format.format(maxTime)+"'");
		if (sgtHost != null)
			wheres.add("R.SGT_Host='"+sgtHost+"'");
		if (ppHost != null)
			wheres.add("R.PP_Host='"+ppHost+"'");
		if (status != null)
			wheres.add("R.Status='"+status.getName()+"'");
		if (studyID != null)
			wheres.add("R.Study_ID="+studyID);
		if (hasCurves) {
			if (hazardDatasetIDs != null && hazardDatasetIDs.length > 0)
				if (hazardDatasetIDs.length == 1)
					wheres.add("C.Hazard_Dataset_ID="+hazardDatasetIDs[0]);
				else
					wheres.add("C.Hazard_Dataset_ID IN ("+Joiner.on(",").join(Ints.asList(hazardDatasetIDs))+")");
			if (imTypeID != null)
				wheres.add("C.IM_Type_ID="+imTypeID);
		}
		
		if (!wheres.isEmpty())
			sql += "\nWHERE "+Joiner.on(" AND ").join(wheres);
		
		return sql;
	}
	
	public static void main(String[] args) {
//		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
//		
//		RunIDFetcher fetch = new RunIDFetcher(db);
//		
//		fetch.forStudyID(9);
//		fetch.forStatus(Status.VERIFIED);
//		fetch.hasHazardCurves(81);
//		
//		System.out.println(fetch.buildSelectSQL());
//		
//		System.out.println("");
//		List<CybershakeRun> runs = fetch.fetch();
//		System.out.println(runs.size()+" Runs");
//		for (CybershakeRun run : runs)
//			System.out.println("\t"+run);
//		System.out.println(runs.size()+" Runs");
//		
//		
//		
//		db.destroy();
		
		for (CyberShakeStudy study : CyberShakeStudy.values()) {
			System.out.println(study.getName());
			RunIDFetcher fetch = study.runFetcher();
			System.out.println(fetch.buildSelectSQL());
			System.out.println(fetch.fetch().size()+" runs");
			System.out.println();
		}
		
		for (CyberShakeStudy study : CyberShakeStudy.values())
			study.getDB().destroy();
	}

}
