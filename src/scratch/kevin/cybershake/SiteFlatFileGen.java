package scratch.kevin.cybershake;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.siteData.SiteData;
import org.opensha.commons.data.siteData.impl.CVM_CCAi6BasinDepth;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.sha.cybershake.HazardCurveFetcher;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.ERF2DB;
import org.opensha.sha.cybershake.db.HazardCurve2DB;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.faultSurface.EvenlyGriddedSurfFromSimpleFaultData;
import org.opensha.sha.faultSurface.EvenlyGriddedSurface;
import org.opensha.sha.faultSurface.FaultTrace;
import org.opensha.sha.faultSurface.RuptureSurface;
import org.opensha.sha.faultSurface.StirlingGriddedSurface;
import org.opensha.sha.faultSurface.utils.GriddedSurfaceUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;

public class SiteFlatFileGen {

	public static void main(String[] args) throws IOException {
		File baseDir = new File("/home/kevin/CyberShake/pge_flat_files");
		
		int datasetID = 81; // CCA 3D
		String datasetLabel = "17_3_cca_3d";
		SiteData<Double> z10fetch = new CVM_CCAi6BasinDepth(SiteData.TYPE_DEPTH_TO_1_0);
		SiteData<Double> z25fetch = new CVM_CCAi6BasinDepth(SiteData.TYPE_DEPTH_TO_2_5);
		
//		int datasetID = 80; // CCA 1D
//		String datasetLabel = "17_3_cca_1d";
		
		File outputDir = new File(baseDir, datasetLabel);
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		List<Integer> imTypeIDs = new ArrayList<>();
		List<String> imLabels = new ArrayList<>();
		
		imTypeIDs.add(167);
		imLabels.add("2s SA RotD50 (cm/s/s)");
		
		imTypeIDs.add(162);
		imLabels.add("3s SA RotD50 (cm/s/s)");
		
		imTypeIDs.add(158);
		imLabels.add("5s SA RotD50 (cm/s/s)");
		
		imTypeIDs.add(154);
		imLabels.add("7.5s SA RotD50 (cm/s/s)");
		
		imTypeIDs.add(152);
		imLabels.add("10s SA RotD50 (cm/s/s)");
		
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
		try {
			File inputCSVFile = new File(baseDir, "Vs30s.csv");
			CSVFile<String> inputCSV = CSVFile.readFile(inputCSVFile, true);
			
			HazardCurveFetcher fetch = new HazardCurveFetcher(db, datasetID, imTypeIDs.get(0));
			List<CybershakeSite> curveSites = fetch.getCurveSites();
			List<Integer> runIDs = fetch.getRunIDs();
			Preconditions.checkState(runIDs.size() == curveSites.size());
			
			AbstractERF erf = MeanUCERF2_ToDB.createUCERF2ERF();
			
			File erfGeomFile = new File(outputDir, "erf_geometries.csv");
			writeERFGeometryFile(erfGeomFile, erf);
			
			CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(db, new File("/data/kevin/cybershake/amps_cache/"), erf);
			List<CybershakeIM> ims = new ArrayList<>();
			HazardCurve2DB curve2db = new HazardCurve2DB(db);
			for (int imTypeID : imTypeIDs)
				ims.add(curve2db.getIMFromID(imTypeID));
			ERF2DB erf2db = new ERF2DB(db);
			Runs2DB run2db = new Runs2DB(db);
			Table<Integer, Integer, List<Location>> hyposTable = HashBasedTable.create();
			
			for (int row=1; row<inputCSV.getNumRows(); row++) {
				String siteName = inputCSV.get(row, 0);
				int siteID = Integer.parseInt(inputCSV.get(row, 1));
				double vs30 = Double.parseDouble(inputCSV.get(row, 4));
				
				System.out.println("Site: "+siteName+" ("+siteID+"), Vs30="+vs30);
				
				int runID = -1;
				Location siteLoc = null;
				for (int i=0; i<curveSites.size(); i++) {
					if (siteID == curveSites.get(i).id) {
						runID = runIDs.get(i);
						siteLoc = curveSites.get(i).createLocation();
						System.out.println("Found matching runID: "+runID);
					}
				}
				Preconditions.checkState(runID >= 0);
				
				CybershakeRun run = run2db.getRun(runID);
				
				double z10 = z10fetch.getValue(siteLoc);
				double z25 = z25fetch.getValue(siteLoc);
				
				CSVFile<String> csv = new CSVFile<>(true);
				List<String> header = Lists.newArrayList("Site Name", "Site Lat", "Site Lon", "Site Vs30 (m/s)",
						"Site Z1.0 (km)", "Site Z2.5 (km)", "Source ID", "Rupture ID", "Rupture Variation ID",
						"Magnitude", "Dip", "Rake", "ZTOR (km)", "DDW (km)", "Rrup (km)", "Rjb (km)", "Rx (km)", "Ry0 (km)",
						"Hypocenter Lat", "Hypocenter Lon", "Hypocenter Depth (km)",
						"Closest Point Lat", "Closest Point Lon", "Closest Point Depth (km)");
				header.addAll(imLabels);
				csv.addLine(header);
				
				List<double[][][]> valsList = new ArrayList<>();
				for (CybershakeIM im : ims)
					valsList.add(amps2db.getAllIM_Values(runID, im));
				double[][][] vals0 = valsList.get(0);
				for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
					if (vals0[sourceID] == null)
						continue;
					System.out.println("Source "+sourceID);
					ProbEqkSource source = erf.getSource(sourceID);
					Preconditions.checkState(vals0[sourceID].length == source.getNumRuptures());
					for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
						if (vals0[sourceID][rupID] == null)
							continue;
						ProbEqkRupture rup = source.getRupture(rupID);
						RuptureSurface surf = rup.getRuptureSurface();
						
						List<Location> hypos = hyposTable.get(sourceID, rupID);
						if (hypos == null) {
							hypos = new ArrayList<>();
							HashMap<Integer, Location> hyposMap = erf2db.getHypocenters(run.getERFID(), sourceID, rupID, run.getRupVarScenID());
							for (int i=0; i<hyposMap.size(); i++)
								hypos.add(hyposMap.get(i));
							hyposTable.put(sourceID, rupID, hypos);
						}
						Preconditions.checkState(vals0[sourceID][rupID].length == hypos.size());
						
						double rJB = surf.getDistanceJB(siteLoc);
						double rRup = surf.getDistanceRup(siteLoc);
						double rX = surf.getDistanceX(siteLoc);
						double rY0 = GriddedSurfaceUtils.getDistanceY0(surf.getEvenlyDiscritizedUpperEdge(), siteLoc);
						double ddw = surf.getAveWidth();
						double ztor = surf.getAveRupTopDepth();
						
						Location closestPt = null;
						double minDist = Double.POSITIVE_INFINITY;
						for (Location pt : surf.getEvenlyDiscritizedListOfLocsOnSurface()) {
							double dist = LocationUtils.linearDistanceFast(siteLoc, pt);
							if (dist < minDist) {
								closestPt = pt;
								minDist = dist;
							}
						}
						Preconditions.checkState((float)minDist == (float)rRup);
						
						for (int rv=0; rv<vals0[sourceID][rupID].length; rv++) {
							List<String> line = new ArrayList<>();
							line.add(siteName);
							line.add((float)siteLoc.getLatitude()+"");
							line.add((float)siteLoc.getLongitude()+"");
							line.add((float)vs30+"");
							line.add((float)z10+"");
							line.add((float)z25+"");
							line.add(sourceID+"");
							line.add(rupID+"");
							line.add(rv+"");
							line.add((float)rup.getMag()+"");
							line.add((float)surf.getAveDip()+"");
							line.add((float)rup.getAveRake()+"");
							line.add((float)ztor+"");
							line.add((float)ddw+"");
							line.add((float)rRup+"");
							line.add((float)rJB+"");
							line.add((float)rX+"");
							line.add((float)rY0+"");
							Location hypo = hypos.get(rv);
							line.add((float)hypo.getLatitude()+"");
							line.add((float)hypo.getLongitude()+"");
							line.add((float)hypo.getDepth()+"");
							line.add((float)closestPt.getLatitude()+"");
							line.add((float)closestPt.getLongitude()+"");
							line.add((float)closestPt.getDepth()+"");
							for (double[][][] vals : valsList)
								line.add(vals[sourceID][rupID][rv]+"");
							csv.addLine(line);
						}
					}
				}
				File outputFile = new File(outputDir, siteName+"_run"+runID+".csv");
				System.out.println("Writing CSV with "+csv.getNumRows()+" rows to: "+outputFile.getAbsolutePath());
				csv.writeToFile(outputFile);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			db.destroy();
		}
	}
	
	private static void writeERFGeometryFile(File file, AbstractERF erf) throws IOException {
		CSVFile<String> csv = new CSVFile<>(false);
		
		csv.addLine("Source ID", "Rup ID", "Mag", "Annual Probability", "Rake", "Dip", "Dip Direction", "Length (km)",
				"Top Depth (km)", "Width (km)", "Num Trace Points",
				"Trace Lat 1", "Trace Lon 1", "Trace Depth 1", "Trace Lat 2", "Trace Lon 2", "Trace Depth 2", "...");
		
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			ProbEqkSource source = erf.getSource(sourceID);
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				ProbEqkRupture rup = source.getRupture(rupID);
				
				List<String> line = new ArrayList<>();
				line.add(sourceID+"");
				line.add(rupID+"");
				line.add((float)rup.getMag()+"");
				line.add(rup.getProbability()+"");
				line.add((float)rup.getAveRake()+"");
				
				EvenlyGriddedSurface surf = (EvenlyGriddedSurface)rup.getRuptureSurface();
				FaultTrace trace;
				if (surf instanceof EvenlyGriddedSurfFromSimpleFaultData)
					trace = ((EvenlyGriddedSurfFromSimpleFaultData)surf).getFaultTrace();
				else
					trace = surf.getUpperEdge();
				
				line.add((float)surf.getAveDip()+"");
				double dipDir = surf.getAveDipDirection();
				if (Double.isNaN(dipDir))
					dipDir = trace.getDipDirection();
				line.add((float)dipDir+"");
				line.add((float)surf.getAveLength()+"");
				line.add((float)surf.getAveRupTopDepth()+"");
				line.add((float)surf.getAveWidth()+"");
				
				line.add(trace.size()+"");
				for (Location loc : trace) {
					line.add((float)loc.getLatitude()+"");
					line.add((float)loc.getLongitude()+"");
					line.add((float)loc.getDepth()+"");
				}
				csv.addLine(line);
			}
		}
		System.out.println("Writing ERF geometries to: "+file.getAbsolutePath());
		csv.writeToFile(file);
	}

}
