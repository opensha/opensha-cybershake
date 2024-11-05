package org.opensha.sha.cybershake.maps;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JOptionPane;

import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.geo.Location;
import org.opensha.commons.util.DataUtils.MinMaxAveTracker;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.sha.calc.hazardMap.BinaryHazardCurveReader;
import org.opensha.sha.calc.hazardMap.HazardDataSetLoader;
import org.opensha.sha.cybershake.db.AttenRelCurves2DB;
import org.opensha.sha.cybershake.db.AttenRelDataSets2DB;
import org.opensha.sha.cybershake.db.AttenRels2DB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeVelocityModel;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.param.OtherParams.SigmaTruncLevelParam;
import org.opensha.sha.imr.param.OtherParams.SigmaTruncTypeParam;

import com.google.common.base.Preconditions;

public class ARCurveInserter {
	
	private static int MAX_CURVES_TO_INSERT = -1;
	
	private static Map<Location, ArbitrarilyDiscretizedFunc> loadCurves(File dir) {
		System.out.println("Loading curves form: "+dir.getAbsolutePath());
		HashMap<Location, ArbitrarilyDiscretizedFunc> map =
			new HashMap<Location, ArbitrarilyDiscretizedFunc>();
		
		if (dir.isFile() && dir.getName().endsWith(".bin")) {
			// binary format
			
			try {
				BinaryHazardCurveReader reader = new BinaryHazardCurveReader(dir.getAbsolutePath());
				return reader.getCurveMap();
			} catch (Exception e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
		}
		
		for (File file : dir.listFiles()) {
			if (MAX_CURVES_TO_INSERT >= 0 && map.size() >= MAX_CURVES_TO_INSERT)
				return map;
			
			if (file.isDirectory()) {
				map.putAll(loadCurves(file));
				continue;
			}
			if (!file.getName().endsWith(".txt"))
				continue;
			
			try {
				Location loc = HazardDataSetLoader.decodeFileName(file.getName());
				if (loc == null)
					continue;
				
				ArbitrarilyDiscretizedFunc curve =
					ArbitrarilyDiscretizedFunc.loadFuncFromSimpleFile(file.getAbsolutePath());
				map.put(loc, curve);
			} catch (Exception e) {
				continue;
			}
		}
		
		return map;
	}
	
	private static void setTruncation(ScalarIMR imr, double trunc) {
		imr.getParameter(SigmaTruncLevelParam.NAME).setValue(trunc);
		if (trunc < 0)
			imr.getParameter(SigmaTruncTypeParam.NAME).setValue(SigmaTruncTypeParam.SIGMA_TRUNC_TYPE_NONE);
		else
			imr.getParameter(SigmaTruncTypeParam.NAME).setValue(SigmaTruncTypeParam.SIGMA_TRUNC_TYPE_1SIDED);
		
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
//		String dir = "/home/kevin/CyberShake/baseMaps/ave2008/curves_3sec";
//		String dir = "/home/kevin/CyberShake/baseMaps/2012_05_22-cvmh/AVG2008";
//		String dir = "/home/kevin/CyberShake/baseMaps/2013_11_07-cvm4-cs-nga2/CY2013/curves/imrs1/";
//		String dir = "/home/kevin/CyberShake/baseMaps/2014_03_03-cvm4i26-cs-nga-3sec/CB2008/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2014_03_05-cvmhnogtl-cs-nga-3sec/AVE2008/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2015_05_27-cvm4i26-cs-nga-2sec/NGA_2008/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2014_03_18-cvm4i26-cs-nga-5sec/AVE2008/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2015_06_12-cvm4i26-cs-nga-10sec/NGA_2008/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2016_05_09-cvm4i26-cs-nga-0.2sec/NGA_2008/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2016_05_09-cvm4i26-cs-nga-0.5sec/NGA_2008/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2016_05_09-cvm4i26-cs-nga-1sec/NGA_2008/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_04_06-ccai6-cs-nga2-2sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_04_06-ccai6-cs-nga2-3sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_04_06-ccai6-cs-nga2-5sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_04_06-ccai6-cs-nga2-10sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_04_06-cca1d-cs-nga2-2sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_04_06-cca1d-cs-nga2-3sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_04_06-cca1d-cs-nga2-5sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_04_06-cca1d-cs-nga2-10sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_04_10-ccai6-cs-nga2-2sec/ASK2014/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_04_10-ccai6-cs-nga2-2sec/BSSA2014/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_04_10-ccai6-cs-nga2-2sec/CB2014/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_04_10-ccai6-cs-nga2-2sec/CY2014/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_04_10-cvm4i26-cs-nga2-2sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_04_12-statewide-nobasin-cs-nga2-2sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_04_12-statewide-nobasin-cs-nga2-3sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_04_12-statewide-nobasin-cs-nga2-5sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_04_12-statewide-nobasin-cs-nga2-10sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_08_24-ccai6-cs-nga2-2sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_08_24-ccai6-cs-nga2-3sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_08_24-ccai6-cs-nga2-5sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_08_24-ccai6-cs-nga2-10sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_09_05-cvm4i26-ca-nga2-3sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2017_09_10-cvm4i26-cs-nga2-individual-5sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2018_04_05-cca1d-cs-nga2-2sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2018_04_05-cca1d-cs-nga2-3sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2018_04_05-cca1d-cs-nga2-5sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2018_04_05-cca1d-cs-nga2-10sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2018_10_09-cs18_8-cs-nga2-10sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2022_01_13-rs4983-cvm4i26-thompson2020-cs-nga2-2sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2022_01_13-rs4983-cvm4i26-thompson2020-cs-nga2-3sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2022_01_13-rs4983-cvm4i26-thompson2020-cs-nga2-5sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2022_01_13-rs4983-cvm4i26-thompson2020-cs-nga2-10sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2022_03_30-rs5413-cvm4i26-thompson2020-cs-nga2-2sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2022_03_30-rs5413-cvm4i26-thompson2020-cs-nga2-3sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2022_03_30-rs5413-cvm4i26-thompson2020-cs-nga2-5sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2022_03_30-rs5413-cvm4i26-thompson2020-cs-nga2-10sec/NGAWest_2014_NoIdr/curves/imrs1.bin";
//		String dir = "/home/kevin/CyberShake/baseMaps/2023_03_08-cvm4i26-m01-taper-cs-nga2-2sec/NGAWest_2014_NoIdr/curves/imrs1.bin"; double period = 2d;
//		String dir = "/home/kevin/CyberShake/baseMaps/2023_03_08-cvm4i26-m01-taper-cs-nga2-3sec/NGAWest_2014_NoIdr/curves/imrs1.bin"; double period = 3d;
//		String dir = "/home/kevin/CyberShake/baseMaps/2023_03_08-cvm4i26-m01-taper-cs-nga2-5sec/NGAWest_2014_NoIdr/curves/imrs1.bin"; double period = 5d;
//		String dir = "/home/kevin/CyberShake/baseMaps/2023_03_08-cvm4i26-m01-taper-cs-nga2-10sec/NGAWest_2014_NoIdr/curves/imrs1.bin"; double period = 10d;
//		String dir = "/home/kevin/CyberShake/baseMaps/2023_03_08-cvm4i26-m01-taper-cs-nga2-0p01sec/NGAWest_2014_NoIdr/curves/imrs1.bin"; double period = 0.01d;
		File baseDir = new File("/home/kevin/CyberShake/baseMaps/");
		
//		String dirName = "2023_03_28-cvm4i26_m01_taper-cs-nga2-0p01sec"; double period = 0.01;
//		String dirName = "2023_03_28-cvm4i26_m01_taper-cs-nga2-0p02sec"; double period = 0.02;
//		String dirName = "2023_03_28-cvm4i26_m01_taper-cs-nga2-0p05sec"; double period = 0.05;
//		String dirName = "2023_03_28-cvm4i26_m01_taper-cs-nga2-0p1sec"; double period = 0.1;
//		String dirName = "2023_03_28-cvm4i26_m01_taper-cs-nga2-0p2sec"; double period = 0.2;
//		String dirName = "2023_03_28-cvm4i26_m01_taper-cs-nga2-0p5sec"; double period = 0.5;
//		String dirName = "2023_03_28-cvm4i26_m01_taper-cs-nga2-1sec"; double period = 1d;
//		String dirName = "2023_03_28-cvm4i26_m01_taper-cs-nga2-2sec"; double period = 2d;
//		String dirName = "2023_03_28-cvm4i26_m01_taper-cs-nga2-3sec"; double period = 3d;
//		String dirName = "2023_03_28-cvm4i26_m01_taper-cs-nga2-5sec"; double period = 5d;
//		String dirName = "2023_03_28-cvm4i26_m01_taper-cs-nga2-10sec"; double period = 10d;
//		String dirName = "2024_10_30-study_24_8-cs-nga2-2sec"; double period = 2d;
//		String dirName = "2024_10_30-study_24_8-cs-nga2-3sec"; double period = 3d;
//		String dirName = "2024_10_30-study_24_8-cs-nga2-5sec"; double period = 5d;
		String dirName = "2024_10_30-study_24_8-cs-nga2-10sec"; double period = 10d;

		Calendar cal = GregorianCalendar.getInstance();
		cal.set(2024, 9, 30); // month is 0-based, 3=April
		
		// UPDATE ERF ID and VM ID !!!!!!!
		boolean deleteOld = true;
		
		ScalarIMR imr = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
		String curveFileName = "NGAWest_2014_NoIdr/curves/imrs1.bin";
		
		File dir = new File(baseDir, dirName);
		System.out.println("Directory: "+dir.getAbsolutePath());
		Preconditions.checkState(dir.exists(), "Directory doesn't exist: %s", dir.getAbsolutePath());
		
		File curveFile = new File(dir, curveFileName);
		System.out.println("Curve file: "+curveFile.getAbsolutePath());
		Preconditions.checkState(curveFile.exists(), "Curve file doesn't exist: %s", curveFile.getAbsolutePath());
		
//		ScalarIMR imr = AttenRelRef.ASK_2014.instance(null);
//		ScalarIMR imr = AttenRelRef.BSSA_2014.instance(null);
//		ScalarIMR imr = AttenRelRef.CB_2014.instance(null);
//		ScalarIMR imr = AttenRelRef.CY_2014.instance(null);
		imr.setParamDefaults();
		setTruncation(imr, 3d);
		/*		UPDATE THESE		*/
//		int erfID = 36;
//		int erfID = 63;
		int erfID = 64;
		int velModelID = CybershakeVelocityModel.Models.STUDY_24_8.instance().getID();
		int imTypeID = CybershakeIM.getSA(CyberShakeComponent.RotD50, period).getID();
//		int velModelID = -1; // Vs30 only
		/*		END UPDATE THESE	*/
		int probModelID = 1;
		int timeSpanID = 1;
		String dbHostName = Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME;
		Date calcDate = cal.getTime();
		Date timeSpanDate = null;
		// for small insert tests
//		MAX_CURVES_TO_INSERT = 0;
		
		// load the curves
		Map<Location, ArbitrarilyDiscretizedFunc> curves = loadCurves(curveFile);
		System.out.println("Loaded "+curves.size()+" curves");
		
		DBAccess db = Cybershake_OpenSHA_DBApplication.getAuthenticatedDBAccess(true, true, dbHostName);
		
		AttenRelDataSets2DB arDataSets2DB = new AttenRelDataSets2DB(db);
		AttenRelCurves2DB arCurves2DB = new AttenRelCurves2DB(db);
		AttenRels2DB ar2db = new AttenRels2DB(db);
		
//		// for bulk deletion
//		for (int typeID : new int[] {152,158,162,167})
//			try {
//				arCurves2DB.deleteAllCurvesFromDataset(34, typeID);
//			} catch (SQLException e1) {
//				// TODO Auto-generated catch block
//				e1.printStackTrace();
//			}
//		System.exit(0);
		
		try {
			int arID = ar2db.getAttenRelID(imr);
			if (arID < 0)
				throw new RuntimeException("AR not found!");
			
			int datasetID = arDataSets2DB.getDataSetID(arID, erfID, velModelID, probModelID, timeSpanID, timeSpanDate);
			if (datasetID < 0) {
				int ret = JOptionPane.showConfirmDialog(null, "Add new Dataset ID?", "Dataset ID not found", JOptionPane.YES_NO_OPTION);
				if (ret == JOptionPane.YES_OPTION) {
					double gridSpacing = Double.POSITIVE_INFINITY;
					MinMaxAveTracker latTrack = new MinMaxAveTracker();
					MinMaxAveTracker lonTrack = new MinMaxAveTracker();
					Location prevLoc = null;
					for (Location loc : curves.keySet()) {
						double lat = loc.getLatitude();
						double lon = loc.getLongitude();
						latTrack.addValue(lat);
						lonTrack.addValue(lon);
						if (prevLoc != null) {
							double diff = Math.abs(lon - prevLoc.getLongitude());
							if (diff < gridSpacing && (float)diff > 0f)
								gridSpacing = diff;
						}
						
						prevLoc = loc;
					}
					datasetID = arDataSets2DB.addDataSetID(arID, erfID, velModelID, probModelID, timeSpanID, timeSpanDate,
							latTrack.getMin(), latTrack.getMax(), lonTrack.getMin(), lonTrack.getMax(), gridSpacing);
				} else {
					System.exit(1);
				}
			} else if (deleteOld) {
				arCurves2DB.deleteAllCurvesFromDataset(datasetID, imTypeID);
			}
			
			arCurves2DB.insertARCurves(calcDate, datasetID, imTypeID, curves);
		} catch (Exception e) {
			e.printStackTrace();
		}
		db.destroy();
		System.exit(0);
	}

}
