package org.opensha.sha.cybershake.calc.mcer;

import java.io.DataOutput;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.data.siteData.SiteData;
import org.opensha.commons.data.siteData.impl.WillsMap2015;
import org.opensha.commons.data.xyz.GriddedGeoDataSet;
import org.opensha.commons.exceptions.GMT_MapException;
import org.opensha.commons.geo.GriddedRegion;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.geo.Region;
import org.opensha.commons.mapping.gmt.GMT_Map;
import org.opensha.commons.mapping.gmt.elements.GMT_CPT_Files;
import org.opensha.commons.util.binFile.GeolocatedRectangularBinaryMesh2DCalculator;
import org.opensha.commons.util.binFile.BinaryMesh2DCalculator.DataType;
import org.opensha.commons.util.cpt.CPT;

import com.google.common.base.Preconditions;
import com.google.common.io.LittleEndianDataOutputStream;

import scratch.UCERF3.analysis.FaultBasedMapGen;

public class CyberShakeRegionVs30Writer {

	public static void main(String[] args) throws IOException, GMT_MapException {
		File outputDir = new File("/home/kevin/CyberShake/MCER/maps/study_15_4_rotd100/");
		SiteData<Double> vs30Prov = new WillsMap2015(new File("/data/kevin/opensha/wills2015.flt").getAbsolutePath());
		Region region = new CaliforniaRegions.CYBERSHAKE_MAP_REGION();
		double spacing = 0.002;
		
		Location minLoc = new Location(region.getMinLat(), region.getMinLon());
		Location maxLoc = new Location(region.getMaxLat(), region.getMaxLon());
		GriddedRegion gridRegion = new GriddedRegion(new Region(minLoc, maxLoc), spacing, minLoc);
		int ny = gridRegion.getNumLatNodes();
		int nx = gridRegion.getNumLonNodes();
		Preconditions.checkState(nx*ny == gridRegion.getNodeCount());
		boolean startBottom = false;
		boolean startLeft = true;
		double startLat = maxLoc.getLatitude();
		double startLon = minLoc.getLongitude();
		GeolocatedRectangularBinaryMesh2DCalculator rectCalc = new GeolocatedRectangularBinaryMesh2DCalculator(
				DataType.FLOAT, nx, ny, startLat, startLon, startBottom, startLeft, spacing);
		int totNum = nx*ny;
		LocationList locs = new LocationList();
		for (int i=0; i<totNum; i++) {
			long x = rectCalc.calcMeshX(i);
			long y = rectCalc.calcMeshY(i);
			Preconditions.checkState(x >= 0 && x < nx);
			Preconditions.checkState(y >= 0 && y < ny);
			Location loc = rectCalc.getLocationForPoint(x, y);
			locs.add(loc);
			if (i % 100000 == 0)
				System.out.println(i+": "+loc);
		}
		System.out.println("Writing "+locs.size()+" values");
		List<Double> vals = vs30Prov.getValues(locs);
		File vs30File = new File(outputDir, "wills_2015_vs30.flt");
		DataOutput dout = new LittleEndianDataOutputStream(new FileOutputStream(vs30File));
		for (double val : vals)
			if (!vs30Prov.isValueValid(val) || val <= 0d)
				dout.writeFloat(Float.NaN);
			else
				dout.writeFloat((float)val);
		((LittleEndianDataOutputStream)dout).close();
		File hdrFile = new File(outputDir, vs30File.getName().replaceAll(".flt", ".hdr"));
		FileWriter fw = new FileWriter(hdrFile);
		fw.write("BYTEORDER      I\n");
		fw.write("LAYOUT         BIL\n");
		fw.write("NROWS          "+ny+"\n");
		fw.write("NCOLS          "+nx+"\n");
		fw.write("NBANDS         1\n");
		fw.write("NBITS          32\n");
		fw.write("BANDROWBYTES   "+(nx*4)+"\n");
		fw.write("TOTALROWBYTES  "+(nx*4)+"\n");
		fw.write("PIXELTYPE      FLOAT\n");
		fw.write("ULXMAP         "+startLon+"\n");
		fw.write("ULYMAP         "+startLat+"\n");
		fw.write("XDIM           "+(float)spacing+"\n");
		fw.write("YDIM           "+(float)spacing+"\n");
		fw.close();
		rectCalc = GeolocatedRectangularBinaryMesh2DCalculator.readHDR(hdrFile);
		WillsMap2015 newVs30 = new WillsMap2015(vs30File.getAbsolutePath(), rectCalc);
		plotSiteData(vs30Prov, gridRegion, spacing, outputDir, "wills_2015_vs30_orig");
		plotSiteData(newVs30, gridRegion, spacing, outputDir, "wills_2015_vs30_new");
	}
	
	private static void plotSiteData(SiteData<Double> prov, Region region, double spacing, File outputDir, String prefix)
			throws IOException, GMT_MapException {
		GriddedRegion gridReg = new GriddedRegion(region, spacing, null);
		GriddedGeoDataSet data = new GriddedGeoDataSet(gridReg, false);
		List<Double> vals = prov.getValues(gridReg.getNodeList());
		Preconditions.checkState(vals.size() == data.size());
		for (int i=0; i<data.size(); i++) {
			double val = vals.get(i);
			Preconditions.checkState(val != 0, "We got a zero?? %s", i);
//			Preconditions.checkState(Double.isFinite(val), "Bad val at index "+i+": ");
			data.set(i, val);
			if (i % 100000 == 0)
				System.out.println(i+": "+val);
		}
		double minZ = Math.floor(data.getMinZ()/10d)*10d;
		double maxZ = Math.ceil(data.getMaxZ()/10d)*10d;
		System.out.println("Z range: "+data.getMinZ()+" "+data.getMaxZ()+" ("+data.size()+" pts)");
		CPT cpt = GMT_CPT_Files.MAX_SPECTRUM.instance().rescale(minZ, maxZ);
		GMT_Map map = new GMT_Map(region, data, spacing, cpt);
		map.setUseGMTSmoothing(false);
		FaultBasedMapGen.plotMap(outputDir, prefix, false, map);
	}

}
