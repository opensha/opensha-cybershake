package org.opensha.sha.cybershake.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.Region;
import org.opensha.sha.calc.hazardMap.HazardDataSetLoader;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

public class AttenRelCurves2DB {
	
	private static final String ATTEN_REL_CURVES_TABLE_NAME = "AR_Hazard_Curves";
	private static final String ATTEN_REL_CURVE_POINTS_TABLE_NAME = "AR_Hazard_Curve_Points";
	
	private DBAccess db;
	
	private static final int CURVE_BUNDLE_SIZE = 1000;
	
	public AttenRelCurves2DB(DBAccess db) {
		this.db = db;
	}
	
	public void insertARCurves(Date date, int datasetID, int imTypeID,
			Map<Location, ? extends DiscretizedFunc> map) throws SQLException {
		
		ArrayList<DiscretizedFunc> curvesToInsert = new ArrayList<DiscretizedFunc>();
		ArrayList<Location> locsToInsert = new ArrayList<Location>();
		
		while (map.size() > 0) {
			if (curvesToInsert.size() == CURVE_BUNDLE_SIZE) {
				doInsertBundle(date, datasetID, imTypeID, curvesToInsert, locsToInsert);
				System.out.println(map.size()+" left");
				curvesToInsert = new ArrayList<DiscretizedFunc>();
				locsToInsert = new ArrayList<Location>();
			}
			
			Location loc = map.keySet().iterator().next();
			DiscretizedFunc curve = map.remove(loc);
			
			curvesToInsert.add(curve);
			locsToInsert.add(loc);
		}
		
		if (curvesToInsert.size() > 0) {
			doInsertBundle(date, datasetID, imTypeID, curvesToInsert, locsToInsert);
		}
	}
	
	private void doInsertBundle(Date date, int datasetID, int imTypeID,
			ArrayList<DiscretizedFunc> curves, ArrayList<Location> locs) throws SQLException {
		System.out.println("Inserting IDs for "+curves.size()+" curves");
		HashMap<Location, Integer> ids = insertCurveIDsForLocs(date, datasetID, imTypeID, locs);
		System.out.println("Done inserting IDs");
		
		StringBuilder sql = new StringBuilder("INSERT INTO "+ATTEN_REL_CURVE_POINTS_TABLE_NAME
					+" (AR_Hazard_Curve_ID, X_Value, Y_Value) VALUES");
		
		for (int i=0; i<curves.size(); i++) {
			DiscretizedFunc curve = curves.get(i);
			Location loc = locs.get(i);
			
			Integer arCurveID = ids.get(loc);
			if (arCurveID == null)
				throw new IllegalStateException("Curve IDs inserted, but no curve found for loc: "+loc);
			
			for (int j=0; j<curve.size(); j++) {
				if (j > 0 || i > 0)
					sql.append(",");
				sql.append("\n");
				
				sql.append("("+arCurveID+", "+curve.getX(j)+", "+curve.getY(j)+")");
			}
		}
		
//		System.out.println(sql);
		System.out.println("Inserting points for "+curves.size()+" curves");
		
		db.insertUpdateOrDeleteData(sql.toString()); 
	}
	
	private HashMap<Location, Integer> insertCurveIDsForLocs(Date date, int datasetID,
			int imTypeID, ArrayList<Location> locs)
	throws SQLException {
		String insertSQL = "INSERT INTO "+ATTEN_REL_CURVES_TABLE_NAME
					+" (Curve_Date, AR_Hazard_Dataset_ID, IM_Type_ID, Lat, Lon) VALUES";
		
		String dateStr;
		if (date == null)
			dateStr = "null";
		else
			dateStr = "'"+DBAccess.SQL_DATE_FORMAT.format(date)+"'";
		boolean first = true;
		for (Location loc : locs) {
			if (first)
				first = false;
			else
				insertSQL += ",";
			insertSQL += "\n(";
			insertSQL += dateStr+", "+datasetID+", "+imTypeID+", "+loc.getLatitude()+", "+loc.getLongitude();
			insertSQL += ")";
		}
		
		db.insertUpdateOrDeleteData(insertSQL);
		
		String selectSQL = "SELECT AR_Hazard_Curve_ID, Lat, Lon FROM "+ATTEN_REL_CURVES_TABLE_NAME
				+" ORDER BY AR_Hazard_Curve_ID DESC LIMIT "+locs.size();
		
		ResultSet rs = db.selectData(selectSQL);
		
		boolean success = rs.first();
		if (!success)
			throw new SQLException("Emty result set!");
		
		HashMap<Location, Integer> ret = new HashMap<Location, Integer>();
		
		while (!rs.isAfterLast()) {
			int arCurveID = rs.getInt(1);
			double lat = rs.getDouble(2);
			double lon = rs.getDouble(3);
			
			ret.put(new Location(lat, lon), arCurveID);
			
			rs.next();
		}
		
		return ret;
	}
	
	public HashMap<Location, Integer> getCurveIDs(int datasetID, int imTypeID, Region reg) throws SQLException {
		HashMap<Location, Integer> ids = new HashMap<Location, Integer>();
		String sql = "SELECT AR_Hazard_Curve_ID,Lat,Lon FROM "+ATTEN_REL_CURVES_TABLE_NAME
					+" WHERE AR_Hazard_Dataset_ID="+datasetID+" AND IM_Type_ID="+imTypeID;
		
		if (reg != null) {
			sql += " AND Lat>="+reg.getMinLat()+" AND Lat<="+reg.getMaxLat()
					+" AND Lon>="+reg.getMinLon()+" AND Lon<="+reg.getMaxLon();
		}
		
		ResultSet rs = db.selectData(sql);
		
		boolean success = rs.first();
		if (!success)
			return ids;
		
		while (!rs.isAfterLast()) {
			int curveID = rs.getInt(1);
			double lat = rs.getDouble(2);
			double lon = rs.getDouble(3);
			
			Location loc = new Location(lat, lon);
			
			ids.put(loc, curveID);
			
			rs.next();
		}
		
		return ids;
	}
	
	public ArbitrarilyDiscretizedFunc getCurve(int curveID) throws SQLException {
		ArrayList<Integer> ids = new ArrayList<Integer>();
		ids.add(curveID);
		return getCurves(ids).values().iterator().next();
	}
	
	public HashMap<Integer, ArbitrarilyDiscretizedFunc> getCurves(Iterable<Integer> curveIDs)
	throws SQLException {
		return getCurves(curveIDs, -1, -1, false);
	}
	
	public HashMap<Integer, ArbitrarilyDiscretizedFunc> getCurves(
			Iterable<Integer> curveIDs,
			double minValue,
			double maxValue,
			boolean isProbAt_IML)
	throws SQLException {
		HashMap<Integer, ArbitrarilyDiscretizedFunc> curves =
			new HashMap<Integer, ArbitrarilyDiscretizedFunc>();
		
		StringBuilder sql = new StringBuilder();
		sql.append("SELECT AR_Hazard_Curve_ID,X_Value,Y_Value FROM "
				+ATTEN_REL_CURVE_POINTS_TABLE_NAME+" WHERE AR_Hazard_Curve_ID IN\n(");
		boolean first = true;
		for (int curveID : curveIDs) {
			if (first)
				first = false;
			else
				sql.append(",");
			sql.append(curveID);
		}
		sql.append(")");
		String fixedVal;
		if (isProbAt_IML)
			fixedVal = "X_Value";
		else
			fixedVal = "Y_Value";
		if (minValue > 0) {
			sql.append("\nAND "+fixedVal+">="+minValue);
		}
		if (maxValue > 0) {
			sql.append("\nAND "+fixedVal+"<="+maxValue);
		}
		
		ResultSet rs = db.selectData(sql.toString());
		
		boolean success = rs.first();
		if (!success)
			return curves;
		
		while (!rs.isAfterLast()) {
			int id = rs.getInt(1);
			if (!curves.containsKey(id))
				curves.put(id, new ArbitrarilyDiscretizedFunc());
			ArbitrarilyDiscretizedFunc curve = curves.get(id);
			
			double x = rs.getDouble(2);
			double y = rs.getDouble(3);
			
			curve.set(x, y);
			
			rs.next();
		}
		
		return curves;
	}
	
	public void deleteAllCurvesFromDataset(int datasetID, int imTypeID) throws SQLException {
		List<Integer> curveIDs = Lists.newArrayList(getCurveIDs(datasetID, imTypeID, null).values());
		System.out.println("Deleting "+curveIDs.size()+" curves");
		deleteCurves(curveIDs);
	}
	
	private static final int max_delete = 500;
	public void deleteCurves(List<Integer> curveIDs) throws SQLException {
		curveIDs = Lists.newArrayList(curveIDs);
		while (curveIDs.size() > max_delete) {
			List<Integer> subCurves = Lists.newArrayList();
			for (int i=0; i<max_delete; i++)
				subCurves.add(curveIDs.remove(curveIDs.size()-1));
			deleteCurves(subCurves);
		}
		if (curveIDs.isEmpty())
			return;
		System.out.println("Deleting "+curveIDs.size()+" curves");
		// first delete points
		String sql = "DELETE FROM "+ATTEN_REL_CURVE_POINTS_TABLE_NAME+" WHERE AR_Hazard_Curve_ID IN ("
				+Joiner.on(",").join(curveIDs)+");";
		db.insertUpdateOrDeleteData(sql);
		
		// now delete curve references
		sql = "DELETE FROM "+ATTEN_REL_CURVES_TABLE_NAME+" WHERE AR_Hazard_Curve_ID IN ("
				+Joiner.on(",").join(curveIDs)+");";
		db.insertUpdateOrDeleteData(sql);
	}
	
	private HashMap<Location, ArbitrarilyDiscretizedFunc> doFetchSubset(
			HashMap<Location, Integer> ids) throws SQLException {
		return doFetchSubset(ids, -1, -1, false);
	}
	
	private HashMap<Location, ArbitrarilyDiscretizedFunc> doFetchSubset(
			HashMap<Location, Integer> ids,
			double minValue,
			double maxValue,
			boolean isProbAt_IML)
			throws SQLException {
		System.out.print("Fetching "+ids.size()+" curves...");
		HashMap<Integer, ArbitrarilyDiscretizedFunc> curves = getCurves(ids.values(),
				minValue, maxValue, isProbAt_IML);
		System.out.println("DONE!");
		
		HashMap<Location, ArbitrarilyDiscretizedFunc> map =
			new HashMap<Location, ArbitrarilyDiscretizedFunc>();
		
		for (Location loc : ids.keySet()) {
			map.put(loc, curves.get(ids.get(loc)));
		}
		
		return map;
	}
	
	public HashMap<Location, ArbitrarilyDiscretizedFunc> fetchCurves(int datasetID, int imTypeID, Region reg)
	throws SQLException {
		HashMap<Location, Integer> ids = getCurveIDs(datasetID, imTypeID, reg);
		
		HashMap<Location, ArbitrarilyDiscretizedFunc> map =
			new HashMap<Location, ArbitrarilyDiscretizedFunc>();
		
		HashMap<Location, Integer> idSubset = new HashMap<Location, Integer>();
		for (Location loc : ids.keySet()) {
			if (idSubset.size() >= CURVE_BUNDLE_SIZE) {
				map.putAll(doFetchSubset(idSubset));
				idSubset = new HashMap<Location, Integer>();
				System.gc();
			}
			idSubset.put(loc, ids.get(loc));
		}
		if (idSubset.size() > 0)
			map.putAll(doFetchSubset(idSubset));
		
		return map;
	}
	
	private GeoDataSet doMapFetch(
			boolean isProbAt_IML,
			double level,
			HashMap<Location, Integer> ids,
			boolean latitudeX,
			double minValue,
			double maxValue) throws SQLException {
		GeoDataSet xyz = new ArbDiscrGeoDataSet(latitudeX);
		
		HashMap<Location, ArbitrarilyDiscretizedFunc> curves =
			doFetchSubset(ids, minValue, maxValue, isProbAt_IML);
		
		for (Location loc : curves.keySet()) {
			ArbitrarilyDiscretizedFunc curve = curves.get(loc);
			double value = HazardDataSetLoader.getCurveVal(curve, isProbAt_IML, level);
			xyz.set(loc, value);
		}
		
		return xyz;
	}
	
	private double[] detectMinMax(ArbitrarilyDiscretizedFunc curve, boolean isProbAt_IML, double level) {
		double minValue = -1;
		double maxValue = -1;
		
		if (isProbAt_IML) {
			// we do it based on fixed x values
			for (int i=0; i<curve.size(); i++) {
				double val = curve.getX(i);
				if (val < level && val > minValue)
					minValue = val;
				if (val > level && (maxValue < 0 || val < maxValue)) {
					maxValue = val;
				}
			}
			minValue -= 0.01;
			maxValue += 0.01;
		} else {
			// we do it based on level
			double logLevel = Math.log10(level);
			minValue = Math.pow(10, logLevel - 2);
			maxValue = Math.pow(10, logLevel + 2);
		}
		
		double[] ret = {minValue, maxValue};
		return ret;
	}
	
	public GeoDataSet fetchMap(
			int datasetID,
			int imTypeID,
			boolean isProbAt_IML,
			double level,
			boolean latitudeX,
			Region reg) throws SQLException {
		GeoDataSet xyz = new ArbDiscrGeoDataSet(latitudeX);
		
		System.out.println("Fetching AR curves for dataset "+datasetID);
		HashMap<Location, Integer> ids = getCurveIDs(datasetID, imTypeID, reg);
		
		// get the first curve to determine the min/max we should select for
		ArbitrarilyDiscretizedFunc firstCurve = getCurve(ids.values().iterator().next());
		double[] minMax = detectMinMax(firstCurve, isProbAt_IML, level);
		double minValue = minMax[0];
		double maxValue = minMax[1];
		System.out.println("isProbAt_IML="+isProbAt_IML+", level="+level);
		System.out.println("Detected min/max: min="+minValue+", max="+maxValue);
//		System.exit(0);
		
		HashMap<Location, Integer> idSubset = new HashMap<Location, Integer>();
		for (Location loc : ids.keySet()) {
			if (idSubset.size() >= CURVE_BUNDLE_SIZE) {
				xyz.setAll(doMapFetch(isProbAt_IML, level, idSubset, latitudeX,
						minValue, maxValue));
				idSubset = new HashMap<Location, Integer>();
				System.gc();
			}
			idSubset.put(loc, ids.get(loc));
		}
		if (idSubset.size() > 0)
			xyz.setAll(doMapFetch(isProbAt_IML, level, idSubset, latitudeX,
					minValue, maxValue));
		
		return xyz;
	}

}
