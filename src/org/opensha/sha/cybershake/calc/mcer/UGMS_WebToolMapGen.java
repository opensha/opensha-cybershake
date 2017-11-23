package org.opensha.sha.cybershake.calc.mcer;

import java.awt.Color;
import java.awt.geom.Point2D;
import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.data.xyz.GriddedGeoDataSet;
import org.opensha.commons.geo.GriddedRegion;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.Region;
import org.opensha.commons.mapping.gmt.GMT_Map;
import org.opensha.commons.mapping.gmt.elements.PSXYSymbol;
import org.opensha.commons.mapping.gmt.elements.TopographicSlopeFile;
import org.opensha.commons.mapping.gmt.elements.PSXYSymbol.Symbol;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.calc.hazardMap.BinaryHazardCurveReader;
import org.opensha.sha.cybershake.HazardCurveFetcher;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.maps.CyberShake_GMT_MapGenerator;

import com.google.common.base.Stopwatch;

import scratch.UCERF3.analysis.FaultBasedMapGen;

public class UGMS_WebToolMapGen {

	public static void main(String[] args) throws Exception {
		File gmpeFile = new File("/home/kevin/CyberShake/MCER/gmpe_cache_gen/mcer_binary_results_2017_07_27/UCERF3/Wills.bin");
//		File gmpeFile = new File("/home/kevin/CyberShake/MCER/gmpe_cache_gen/mcer_binary_results_2017_07_27/UCERF3/classD_default.bin");
		double gmpeSpacing = 0.02;
		File csFile = new File("/home/kevin/CyberShake/MCER/maps/study_15_4_rotd100/interp_tests/mcer_spectrum_0.002.bin");
		double csSpacing = 0.002;
		int datasetID = 57;
		double period = 5d;
		File outputDir = new File("/tmp");
		
		System.out.println("Loading CS");
		GriddedSpectrumInterpolator csInterp = getInterpolator(csFile, csSpacing);
		System.out.println("Loading GMPE");
		GriddedSpectrumInterpolator gmpeInterp = getInterpolator(gmpeFile, gmpeSpacing);
		
		double mapSpacing = 0.01;
		Region region = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
		GriddedRegion gridReg = new GriddedRegion(region, mapSpacing, null);
		GriddedGeoDataSet combXYZ = new GriddedGeoDataSet(gridReg, false);
		GriddedGeoDataSet gmpeXYZ = new GriddedGeoDataSet(gridReg, false);
		GriddedGeoDataSet csXYZ = new GriddedGeoDataSet(gridReg, false);
		
		GriddedGeoDataSet[] xyzs = { combXYZ, gmpeXYZ, csXYZ };
		String[] labels = { "CyberShake/GMPE MCER, "+(int)period+"s SA",
				"GMPE MCER, "+(int)period+"s SA", "CyberShake MCER, "+(int)period+"s SA" };
		String[] prefixes = { "cs_gmpe_mcer_"+(float)period,
				"gmpe_mcer_"+(float)period, "cs_mcer_"+(float)period };
		
		System.out.println("Building XYZ");
		for (int i=0; i<combXYZ.size(); i++) {
			Location loc = combXYZ.getLocation(i);
			if (!region.contains(loc)) {
				combXYZ.set(i, Double.NaN);
				csXYZ.set(i, Double.NaN);
				gmpeXYZ.set(i, Double.NaN);
				continue;
			}
			
			try {
				DiscretizedFunc csMCER;
				DiscretizedFunc gmpeMCER;
				try {
					csMCER = csInterp.getInterpolated(loc);
					gmpeMCER = gmpeInterp.getInterpolated(loc);
				} catch (Exception e) {
					csMCER = csInterp.getClosest(loc);
					gmpeMCER = gmpeInterp.getClosest(loc);
				}
//				DiscretizedFunc csMCER = csInterp.getClosest(loc);
//				DiscretizedFunc gmpeMCER = gmpeInterp.getClosest(loc);
				
				DiscretizedFunc finalMCER = MCERDataProductsCalc.calcFinalMCER(csMCER, gmpeMCER);
				combXYZ.set(i, finalMCER.getY(period));
				csXYZ.set(i, csMCER.getY(period));
				gmpeXYZ.set(i, gmpeMCER.getY(period));
			} catch (Exception e) {
				combXYZ.set(i, Double.NaN);
				csXYZ.set(i, Double.NaN);
				gmpeXYZ.set(i, Double.NaN);
			}
		}
		
		System.out.println("Building map");
		CPT cpt = CPT.loadFromStream(UGMS_WebToolMapGen.class.getResourceAsStream(
				"/org/opensha/sha/cybershake/conf/cpt/cptFile_hazard_input.cpt"));
		cpt = cpt.rescale(0d, 0.6);
		cpt.setNanColor(CyberShake_GMT_MapGenerator.OUTSIDE_REGION_COLOR);
		
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.ARCHIVE_HOST_NAME);
		HazardCurveFetcher fetch = new HazardCurveFetcher(db, datasetID, 151);
		db.destroy();
		double width = 0.0342;
		double penWidth = 0.06;
		Color penColor = Color.WHITE;
		Color fillColor = Color.WHITE;
		Symbol symbol = Symbol.INVERTED_TRIANGLE;
		
		for (int i=0; i<xyzs.length; i++) {
			GriddedGeoDataSet xyz = xyzs[i];
			String label = labels[i];
			String prefix = prefixes[i];
			
			GMT_Map map = new GMT_Map(region, xyz, mapSpacing, cpt);
			
			// now add sites
			for (CybershakeSite site : fetch.getCurveSites()) {
				Location loc = site.createLocation();
				Point2D pt = new Point2D.Double(loc.getLongitude(), loc.getLatitude());
				PSXYSymbol xy = new PSXYSymbol(pt, symbol, width, penWidth, penColor, fillColor);
				map.addSymbol(xy);
			}
			
			map.setLogPlot(false);
			map.setMaskIfNotRectangular(true);
			map.setTopoResolution(TopographicSlopeFile.CA_THREE);
//			map.setTopoResolution(null);
//			map.setUseGMTSmoothing(false);
			map.setBlackBackground(false);
			map.setCustomScaleMin((double)cpt.getMinValue());
			map.setCustomScaleMax((double)cpt.getMaxValue());
			map.setCustomLabel(label);
			map.setRescaleCPT(false);
			map.setImageWidth(12);
			FaultBasedMapGen.LOCAL_MAPGEN = false;
			System.out.println("Plotting map: "+label);
			FaultBasedMapGen.plotMap(outputDir, prefix, false, map);
		}
	}
	
	private static GriddedSpectrumInterpolator getInterpolator(File dataFile, double spacing) throws Exception {
		System.out.println("Loading spectrum from "+dataFile.getAbsolutePath());
		Stopwatch watch = Stopwatch.createStarted();
		BinaryHazardCurveReader reader = new BinaryHazardCurveReader(dataFile.getAbsolutePath());
		Map<Location, ArbitrarilyDiscretizedFunc> map = reader.getCurveMap();
		watch.stop();
		System.out.println("Took "+watch.elapsed(TimeUnit.SECONDS)+" s to load spectrum");
		
		GriddedSpectrumInterpolator interp = new GriddedSpectrumInterpolator(map, spacing);
		return interp;
	}

}
