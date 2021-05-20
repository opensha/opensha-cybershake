package org.opensha.sha.cybershake.bombay;

import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.commons.util.FileUtils;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.ERF2DB;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.faultSurface.AbstractEvenlyGriddedSurface;
import org.opensha.sha.faultSurface.RuptureSurface;

import com.google.common.collect.Lists;

public class RupHyposWithinCutoff {
	
	private Location hypoLocation;
	private double maxDistance;
	private String sourceNameConstr;
	private boolean useDepth;
	private double minMag;
	
	private DBAccess db;
	private ERF2DB erf2db;
	private SiteInfo2DB site2db;
	
	private AbstractERF ucerf;
	
	private ArrayList<Integer> sources = new ArrayList<Integer>();
	private ArrayList<ArrayList<Integer>> rups = new ArrayList<ArrayList<Integer>>();
	
	private HashMap<String, ArrayList<Integer>> rvMap = new HashMap<String, ArrayList<Integer>>();
	private HashMap<String, ArrayList<Integer>> excludedMap = new HashMap<String, ArrayList<Integer>>();
	
	private HashMap<String, ArrayList<Location>> rvLocMap = new HashMap<String, ArrayList<Location>>();
	
	private static final int ERFID = 35;
	private static final int RUP_VAR_SCEN_ID = 3;
	
	public RupHyposWithinCutoff(DBAccess db, Location hypoLocation,
			double maxDistance, String sourceNameConstr, boolean useDepth, double minMag) {
		this.db = db;
		if (useDepth)
			this.hypoLocation = hypoLocation;
		else
			this.hypoLocation = new Location(hypoLocation.getLatitude(), hypoLocation.getLongitude());
		this.maxDistance = maxDistance;
		if (sourceNameConstr != null && sourceNameConstr.length() == 0)
			sourceNameConstr = null;
		this.sourceNameConstr = sourceNameConstr;
		this.useDepth = useDepth;
		this.minMag = minMag;
		
		erf2db = new ERF2DB(db);
		site2db = new SiteInfo2DB(db);
		ucerf = MeanUCERF2_ToDB.createUCERF2ERF();
		
		loadRups();
		loadRVMap();
	}
	
	private void loadRups() {
		int numSources = ucerf.getNumSources();
		
		int sourceCount = 0;
		int rupCount = 0;
		
		for (int sourceID=0; sourceID<numSources; sourceID++) {
			ProbEqkSource source = ucerf.getSource(sourceID);
			String name = source.getName();
			if (sourceNameConstr != null && !name.toLowerCase().contains(sourceNameConstr))
				continue;
			ArrayList<Integer> rupIDs = new ArrayList<Integer>();
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				ProbEqkRupture rup = source.getRupture(rupID);
				if (shouldIncludeRup(rup)) {
					rupIDs.add(rupID);
				}
			}
			if (rupIDs.size() > 0) {
				rups.add(rupIDs);
				sources.add(sourceID);
				sourceCount++;
				rupCount += rupIDs.size();
			}
		}
		System.out.println("Identified " + sourceCount + " sources (" + rupCount + " rups)");
	}
	
	private boolean shouldIncludeRup(ProbEqkRupture rup) {
		if (rup.getMag() < minMag)
			return false;
		for(Location loc:rup.getRuptureSurface().getEvenlyDiscritizedListOfLocsOnSurface()) {
			double dist = LocationUtils.linearDistance(hypoLocation, loc);
			if (dist < maxDistance) {
				return true;
			}
		}
		return false;
	}
	
	private ArrayList<Integer> getRupVars(int sourceID, int rupID) {
		String sql = "SELECT Rup_Var_ID,Hypocenter_Lat,Hypocenter_Lon,Hypocenter_Depth FROM Rupture_Variations " +
					"WHERE ERF_ID=" + ERFID + " AND Rup_Var_Scenario_ID=" + RUP_VAR_SCEN_ID + " " +
					"AND Source_ID=" + sourceID + " AND Rupture_ID=" + rupID;
		ArrayList<Integer> rvs = new ArrayList<Integer>();
		int tot = 0;
		try {
			ArrayList<Integer> rvsExcluded = Lists.newArrayList();
			ResultSet rs = db.selectData(sql);
			boolean success = rs.next();
			while (success) {
				int rvID = rs.getInt("Rup_Var_ID");
				double lat = rs.getDouble("Hypocenter_Lat");
				double lon = rs.getDouble("Hypocenter_Lon");
				double depth = rs.getDouble("Hypocenter_Depth");
				Location loc;
				if (useDepth)
					loc = new Location(lat, lon, depth);
				else
					loc = new Location(lat, lon);
				tot++;
				
				double dist = LocationUtils.linearDistance(loc, hypoLocation);
				if (dist < maxDistance) {
					rvs.add(rvID);
					ArrayList<Location> locs = rvLocMap.get(getKey(sourceID, rupID));
					if (locs == null) {
						locs = new ArrayList<Location>();
						rvLocMap.put(getKey(sourceID, rupID), locs);
					}
					locs.add(loc);
				} else {
					rvsExcluded.add(rvID);
				}
				
				success = rs.next();
			}
			excludedMap.put(getKey(sourceID, rupID), rvsExcluded);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Source " + sourceID + ", Rup " + rupID + ", vars: " + rvs.size() + "/" + tot);
		
		return rvs;
	}
	
	public static String getKey(int sourceID, int rupID) {
		return sourceID + " " + rupID;
	}
	
	private void loadRVMap() {
		for (int i=0; i<sources.size(); i++) {
			int sourceID = sources.get(i);
			ArrayList<Integer> rupIDs = rups.get(i);
			for (int rupID : rupIDs) {
				ArrayList<Integer> rvs = getRupVars(sourceID, rupID);
				if (rvs.size() > 0)
					rvMap.put(getKey(sourceID, rupID), rvs);
			}
		}
	}
	
	public void writeSourceRupFile(String fileName) throws IOException {
		FileWriter fw = new FileWriter(fileName);
		for (int i=0; i<sources.size(); i++) {
			int sourceID = sources.get(i);
			ArrayList<Integer> rupIDs = rups.get(i);
			for (int rupID : rupIDs) {
				ArrayList<Integer> rvs = rvMap.get(getKey(sourceID, rupID));
				if (rvs == null)
					continue;
				for (int rv : rvs) {
					fw.write(sourceID + "\t" + rupID + "\t" + rv + "\n");
				}
			}
		}
		fw.close();
	}
	
	public void writeSourceRupInfoFile(String fileName) throws IOException {
		FileWriter fw = new FileWriter(fileName);
		for (int i=0; i<sources.size(); i++) {
			int sourceID = sources.get(i);
			ArrayList<Integer> rupIDs = rups.get(i);
			for (int rupID : rupIDs) {
				ArrayList<Integer> rvs = rvMap.get(getKey(sourceID, rupID));
				if (rvs == null)
					continue;
				double mag = ucerf.getSource(sourceID).getRupture(rupID).getMag();
				double prob = ucerf.getSource(sourceID).getRupture(rupID).getProbability();
				ArrayList<Location> locs = rvLocMap.get(getKey(sourceID, rupID));
				for (int j=0; j<rvs.size(); j++) {
					int rv = rvs.get(j);
					Location loc = locs.get(j);
					double dist = LocationUtils.linearDistance(loc, hypoLocation);
					fw.write(sourceID + "\t" + rupID + "\t" + rv + "\t" + mag + "\t" + prob + "\t" + dist + "\n");
				}
			}
		}
		fw.close();
	}
	
	public static HashMap<String, ArrayList<Integer>> loadMapFromFile(String fileName)
				throws FileNotFoundException, IOException {
		ArrayList<String> lines = FileUtils.loadFile(fileName);
		HashMap<String, ArrayList<Integer>> map = new HashMap<String, ArrayList<Integer>>();
		for (String line : lines) {
			StringTokenizer tok = new StringTokenizer(line);
			int sourceID = Integer.parseInt(tok.nextToken());
			int rupID = Integer.parseInt(tok.nextToken());
			int rvID = Integer.parseInt(tok.nextToken());
			String key = getKey(sourceID, rupID);
			ArrayList<Integer> rvs = map.get(key);
			if (rvs == null) {
				rvs = new ArrayList<Integer>();
				map.put(key, rvs);
			}
			rvs.add(rvID);
		}
		return map;
	}
	
	public AbstractERF getERF() {
		return ucerf;
	}
	
	public int getNumExcluded(int sourceID, int rupID) {
		String key = getKey(sourceID, rupID);
		if (excludedMap.containsKey(key))
			return excludedMap.get(key).size();
		else
			return -1;
	}
	
	public ArrayList<Integer> getExcludedRVs(int sourceID, int rupID) {
		String key = getKey(sourceID, rupID);
		if (excludedMap.containsKey(key))
			return excludedMap.get(key);
		else
			return null;
	}
	
	public ArrayList<Integer> getVariationsWithinCutoff(int sourceID, int rupID) {
		return rvMap.get(getKey(sourceID, rupID));
	}

}
