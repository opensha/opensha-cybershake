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
	private boolean noTestSites = false;
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
	private List<String> nonNullRunFields;
	
	// external fields
	private boolean hasCurves;
	private boolean hasAmps;
	private int[] hazardDatasetIDs;
	private Integer imTypeID;

	private boolean unique;
	private boolean uniqueUseFirst;
	
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
	
	public RunIDFetcher noTestSites() {
		noTestSites = true;
		return this;
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
	
	public RunIDFetcher hasAmplitudes() {
		return hasAmplitudes(this.imTypeID);
	}
	
	public RunIDFetcher hasMeshVsitop() {
		if (nonNullRunFields == null)
			nonNullRunFields = new ArrayList<>();
		nonNullRunFields.add("Mesh_Vsitop");
		return this;
	}
	
	public RunIDFetcher hasModelVs30() {
		if (nonNullRunFields == null)
			nonNullRunFields = new ArrayList<>();
		nonNullRunFields.add("Model_Vs30");
		return this;
	}
	
	public RunIDFetcher hasAmplitudes(Integer imTypeID) {
		this.hasAmps = true;
		this.imTypeID = imTypeID;
		return this;
	}
	
	public RunIDFetcher unique(boolean useFirst) {
		this.unique = true;
		this.uniqueUseFirst = useFirst;
		return this;
	}
	
	public List<CybershakeRun> fetch() {
		String sql = buildSelectSQL();
		
		List<CybershakeRun> runs = new ArrayList<>();
		
		HashSet<Integer> runIDs = new HashSet<>();
		
		try {
			ResultSet rs = db.selectData(sql);
			boolean valid = rs.next();
			
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
		
		if (unique) {
			Map<Integer, CybershakeRun> siteRunMap = new HashMap<>();
			for (CybershakeRun run : runs) {
				Integer siteID = run.getSiteID();
				if (siteRunMap.containsKey(siteID)) {
					// duplicate
					int prevID = siteRunMap.get(siteID).getRunID();
					if (uniqueUseFirst && run.getRunID() < prevID
							|| !uniqueUseFirst && run.getRunID() > prevID) {
						// use this one
						siteRunMap.put(siteID, run);
					}
				} else {
					siteRunMap.put(siteID, run);
				}
			}
			if (siteRunMap.size() < runs.size()) {
				HashSet<CybershakeRun> keepers = new HashSet<>(siteRunMap.values());
				for (int i=runs.size(); --i>=0;)
					if (!keepers.contains(runs.get(i)))
						runs.remove(i);
			}
		}
		
		return runs;
	}
	
	public String buildSelectSQL() {
		String sql = "SELECT DISTINCT R.* FROM CyberShake_Runs R";
		if (hasCurves)
			sql += " JOIN Hazard_Curves C ON R.Run_ID=C.Run_ID";
		if (noTestSites)
			sql += " JOIN CyberShake_Sites S ON S.CS_Site_ID=R.Site_ID";
			
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
		if (nonNullRunFields != null)
			for (String field : nonNullRunFields)
				wheres.add("R."+field+" IS NOT NULL");
		if (hasCurves) {
			if (hazardDatasetIDs != null && hazardDatasetIDs.length > 0)
				if (hazardDatasetIDs.length == 1)
					wheres.add("C.Hazard_Dataset_ID="+hazardDatasetIDs[0]);
				else
					wheres.add("C.Hazard_Dataset_ID IN ("+Joiner.on(",").join(Ints.asList(hazardDatasetIDs))+")");
			if (imTypeID != null)
				wheres.add("C.IM_Type_ID="+imTypeID);
		}
		if (noTestSites)
			wheres.add("S.CS_Site_Type_ID != "+CybershakeSite.TYPE_TEST_SITE);
		
		if (!wheres.isEmpty())
			sql += "\nWHERE "+Joiner.on(" AND ").join(wheres);
		
		if (hasAmps) {
			sql = "SELECT R.* FROM (\n"+sql+"\n) R WHERE EXISTS(SELECT * FROM PeakAmplitudes WHERE Run_ID=R.Run_ID";
			if (imTypeID != null)
				sql += " AND IM_Type_ID="+imTypeID;
			sql += ")";
		}
		
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
		
////		CyberShakeStudy study = CyberShakeStudy.STUDY_21_12_RSQSIM_4983_SKIP65k_1Hz;
//		CyberShakeStudy study = CyberShakeStudy.STUDY_22_3_RSQSIM_5413;
		for (CyberShakeStudy study : CyberShakeStudy.values()) {
			System.out.println(study.getName());
			RunIDFetcher fetch = study.runFetcher();
			System.out.println(fetch.buildSelectSQL());
			System.out.println(fetch.fetch().size()+" runs");
			System.out.println();
		}
		
		for (CyberShakeStudy study : CyberShakeStudy.values())
			study.getDB().destroy();
		
//		CyberShakeStudy study1 = CyberShakeStudy.STUDY_21_12_RSQSIM_4983_SKIP65k_1Hz;
//		CyberShakeStudy study2 = CyberShakeStudy.STUDY_22_3_RSQSIM_5413;
//		
//		HashSet<Integer> ids1 = new HashSet<>();
//		for (CybershakeRun siteRun : study1.runFetcher().fetch())
//			ids1.add(siteRun.getSiteID());
//		HashSet<Integer> ids2 = new HashSet<>();
//		for (CybershakeRun siteRun : study2.runFetcher().fetch())
//			ids2.add(siteRun.getSiteID());
//		for (Integer id : ids1)
//			if (!ids2.contains(id))
//				System.out.println(study1.getName()+" has site "+id+" but "+study2.getName()+" doesn't");
//		for (Integer id : ids2)
//			if (!ids1.contains(id))
//				System.out.println(study2.getName()+" has site "+id+" but "+study1.getName()+" doesn't");
//		
//		study1.getDB().destroy();
//		study2.getDB().destroy();
	}

}
