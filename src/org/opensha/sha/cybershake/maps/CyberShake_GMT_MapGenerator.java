package org.opensha.sha.cybershake.maps;

import java.awt.Color;
import java.awt.geom.Point2D;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.exceptions.GMT_MapException;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.Region;
import org.opensha.commons.mapping.gmt.GMT_Map;
import org.opensha.commons.mapping.gmt.GMT_MapGenerator;
import org.opensha.commons.mapping.gmt.SecureMapGenerator;
import org.opensha.commons.mapping.gmt.elements.GMT_CPT_Files;
import org.opensha.commons.mapping.gmt.elements.PSXYSymbol;
import org.opensha.commons.mapping.gmt.elements.TopographicSlopeFile;
import org.opensha.commons.util.RunScript;
import org.opensha.commons.util.XYZClosestPointFinder;
import org.opensha.commons.util.DataUtils.MinMaxAveTracker;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.commons.util.cpt.CPTVal;
import org.opensha.sha.cybershake.maps.InterpDiffMap.InterpDiffMapType;
import org.opensha.sha.cybershake.maps.servlet.CS_InterpDiffMapServletAccessor;
import org.opensha.sha.cybershake.plot.ScatterSymbol;

import com.google.common.io.Files;

public class CyberShake_GMT_MapGenerator implements SecureMapGenerator {
	
	public static int[] dpis = {72, 150, 300};
	
	public static GeoDataSet getDiffs(GeoDataSet baseMap, GeoDataSet scatterData, boolean ratio) {
		System.out.println("Generating diffs for interpolation...");
		GeoDataSet diffs = new ArbDiscrGeoDataSet(baseMap.isLatitudeX());
		
		XYZClosestPointFinder xyz = new XYZClosestPointFinder(baseMap);
		
		for (int i=0; i<scatterData.size(); i++) {
			Location loc = scatterData.getLocation(i);
			
			double scatterVal = scatterData.get(i);
			double closestVal = xyz.getClosestVal(loc);
//			System.out.println("scatterVal: " + scatterVal);
//			System.out.println("closestVal: " + closestVal);
			
			if (ratio)
				diffs.set(loc, scatterVal / closestVal);
			else
				diffs.set(loc, scatterVal - closestVal);
		}
		System.out.println("DONE");
		
		return diffs;
	}
	
	public ArrayList<String> getGMT_ScriptLines(GMT_Map map, String dir) throws GMT_MapException {
		if (map instanceof InterpDiffMap)
			return getGMT_ScriptLines((InterpDiffMap)map, dir);
		else
			throw new IllegalArgumentException("map must be of type InterpDiffMap!");
		
	}
	
	private static MinMaxAveTracker calcExtentsWithinRegion(GeoDataSet xyz, Region region) {
		MinMaxAveTracker tracker = new MinMaxAveTracker();
		
		for (int i=0; i<xyz.size(); i++) {
			double val = xyz.get(i);
			Location loc = xyz.getLocation(i);
			if (region.contains(loc)) {
				tracker.addValue(val);
			}
		}
		
		return tracker;
	}
	
	public static final Color OUTSIDE_REGION_COLOR = Color.GRAY;
//	public static final Color OUTSIDE_REGION_COLOR = Color.WHITE;
	
	public static CPT getHazardCPT() throws IOException {
//		CPT cpt = CPT.loadFromStream(HardCodedInterpDiffMapCreator.class.getResourceAsStream(
//				"/resources/cpt/MaxSpectrum2.cpt"));
		CPT cpt = CPT.loadFromStream(HardCodedInterpDiffMapCreator.class.getResourceAsStream(
				"/org/opensha/sha/cybershake/conf/cpt/cptFile_hazard_input.cpt"));
		cpt.setNanColor(OUTSIDE_REGION_COLOR);
		return cpt;
	}
	
	public static CPT getRatioCPT() throws IOException {
//		CPT ratioCPT = GMT_CPT_Files.MAX_SPECTRUM.instance();
//		ratioCPT = ratioCPT.rescale(0, 2);
//		return ratioCPT;
//		CPT cpt = CPT.loadFromStream(CyberShake_GMT_MapGenerator.class.getResourceAsStream(
//				"/org/opensha/sha/cybershake/conf/cpt/cptFile_ratio.cpt"));
		CPT cpt = CPT.loadFromStream(CyberShake_GMT_MapGenerator.class.getResourceAsStream(
				"/org/opensha/sha/cybershake/conf/cpt/cptFile_ratio_tighter.cpt"));
		cpt.setNanColor(OUTSIDE_REGION_COLOR);
		return cpt;
	}
	
	public static CPT getDiffCPT() throws IOException {
//		CPT diffCPT = GMT_CPT_Files.MAX_SPECTRUM.instance();
//		diffCPT = diffCPT.rescale(-0.8, 0.8);
//		return diffCPT;
		CPT cpt = CPT.loadFromStream(CyberShake_GMT_MapGenerator.class.getResourceAsStream(
				"/org/opensha/sha/cybershake/conf/cpt/cptFile_diff.cpt"));
		cpt.setNanColor(OUTSIDE_REGION_COLOR);
		cpt = cpt.rescale(-0.15, 0.15);
		// this yeilds ugly vals, round
		for (CPTVal val : cpt) {
			double start = val.start;
			start *= 1000d;
			start = Math.round(start)/1000d;
			double end = val.end;
			end *= 1000d;
			end = Math.round(end)/1000d;
			val.start = (float)start;
			val.end = (float)end;
		}
		return cpt;
	}
	
	public ArrayList<String> getGMT_ScriptLines(InterpDiffMap map, String dir) throws GMT_MapException {
		System.out.println("Generating map script for dir: " + dir);
		
		if (!dir.endsWith(File.separator))
			dir += File.separator;

		ArrayList<String> rmFiles = new ArrayList<String>();
		
		String commandLine;

		ArrayList<String> gmtCommandLines = new ArrayList<String>();
		
		InterpDiffMapType[] mapTypes = map.getMapTypes();
		if (mapTypes == null)
			mapTypes = InterpDiffMapType.values();
		
		boolean shouldInterp = false;
		boolean shouldMakeTopo = true;
		boolean shouldMakeRatio = false;
		for (InterpDiffMapType type : mapTypes) {
			if (type != InterpDiffMapType.BASEMAP) {
				// if it's anything but a basemap, we need to do the interpolation
				shouldInterp = true;
			}
//			if (type != InterpDiffMapType.DIFF && type != InterpDiffMapType.RATIO) {
//				// if it's anything but a diff map, we need to prepare the topo
//				shouldMakeTopo = map.getTopoResolution() != null;
//			}
			if (type == InterpDiffMapType.RATIO && map.getGriddedData() != null) {
				shouldMakeRatio = true;
			}
		}
		
		double minLon = map.getRegion().getMinLon();
		double maxLon = map.getRegion().getMaxLon();
		double minLat = map.getRegion().getMinLat();
		double maxLat = map.getRegion().getMaxLat();
		
		gmtCommandLines.add("#!/bin/bash");
		gmtCommandLines.add("");
		gmtCommandLines.add("cd " + dir);
		gmtCommandLines.add("");
		gmtCommandLines.addAll(GMT_MapGenerator.getGMTPathEnvLines());
		String regionVals = "-R" + minLon + "/" + maxLon + "/" + minLat + "/" + maxLat;
		gmtCommandLines.add("REGION=\""+regionVals+"\"");
		String region = " $REGION ";
		double plotWdth = 6.5;
		String proj = " $PROJ ";
		gmtCommandLines.add("PROJ=\"-JM"+plotWdth+"i\"");
		gmtCommandLines.add("## Plot Script ##");
		gmtCommandLines.add("");
		
		double mapGridSpacing = map.getGriddedDataInc();
		GMT_InterpolationSettings interpSettings = map.getInterpSettings();
		double interpGridSpacing = interpSettings.getInterpSpacing();
		
		GeoDataSet griddedData;
		GeoDataSet scatterData;
		griddedData = map.getGriddedData();
		scatterData = map.getScatter();
		
		// write the basemap
		String basemapXYZName = map.getXyzFileName();
		// this is done by the servlet
//		try {
//			ArbDiscretizedXYZ_DataSet.writeXYZFile(griddedData, dir + basemapXYZName);
//		} catch (IOException e) {
//			throw new GMT_MapException("Could not write XYZ data to a file", e);
//		}
		String baseGRD = "base_map.grd";
		if (griddedData != null) {
			rmFiles.add(baseGRD);
			gmtCommandLines.add("# convert xyz file to grd file");
			commandLine = "${GMT_PATH}xyz2grd "+ basemapXYZName +" -G"+ baseGRD+ " -I"+mapGridSpacing+
							region +" -D/degree/degree/amp/=/=/=  -:";
			gmtCommandLines.add(commandLine+"\n");
		}
		
		String interpUnsampledGRD = "interpolated.grd";
		String interpSampledGRD = "interp_resampled.grd";
		String interpRatioUnsampledGRD = "interpolated_ratio.grd";
		String interpRatioSampledGRD = "interp_ratio_resampled.grd";
		if (shouldInterp) {
			// do the interpolation
			String interpXYZName;
			GeoDataSet toBeWritten;
			if (griddedData == null) {
				interpXYZName = "scatter.xyz";
				toBeWritten = scatterData;
			} else {
				interpXYZName = "scatter_diffs.xyz";
				toBeWritten = getDiffs(griddedData, scatterData, false);
			}
			try {
				ArbDiscrGeoDataSet.writeXYZFile(toBeWritten, dir + interpXYZName);
			} catch (IOException e) {
				throw new GMT_MapException("Could not write XYZ data to a file", e);
			}
			
			rmFiles.add(interpUnsampledGRD);
			gmtCommandLines.add("# do GMT interpolation on the scatter data");
			commandLine = "${GMT_PATH}surface "+ interpXYZName +" -G"+ interpUnsampledGRD+ " -I"+interpGridSpacing
							+region+interpSettings.getConvergenceArg()+" "+interpSettings.getSearchArg()
							+" "+interpSettings.getTensionArg()+" -: -h0 2>/dev/null";
			gmtCommandLines.add(commandLine);
			if (interpSettings.isSaveInterpSurface()) {
				gmtCommandLines.add("# write interpolated XYZ file");
				commandLine = "${GMT_PATH}grd2xyz "+ interpUnsampledGRD+ " > "+GMT_InterpolationSettings.INTERP_XYZ_FILE_NAME;
				gmtCommandLines.add(commandLine);
			}
			// resample the interpolation
			
			rmFiles.add(interpSampledGRD);
			if (interpGridSpacing == mapGridSpacing) {
				gmtCommandLines.add("# the grid spacings are equal, we can just copy");
				gmtCommandLines.add("cp " + interpUnsampledGRD + " " + interpSampledGRD);
			} else {
				gmtCommandLines.add("# resample the interpolated file");
				boolean bicubic = false;
				commandLine = "${GMT_PATH}grdsample "+interpUnsampledGRD+" -G"+interpSampledGRD
								+" -I"+mapGridSpacing+region;
				if (!bicubic)
					commandLine += "-nl";
				gmtCommandLines.add(commandLine+"\n");
			}
			
			String interpRatioXYZName = "ratios.xyz";
			if (shouldMakeRatio) {
				try {
					ArbDiscrGeoDataSet.writeXYZFile(getDiffs(griddedData, scatterData, true), dir + interpRatioXYZName);
				} catch (IOException e) {
					throw new GMT_MapException("Could not write XYZ data to a file", e);
				}
				
				rmFiles.add(interpRatioUnsampledGRD);
				gmtCommandLines.add("# do GMT interpolation on the scatter data");
				commandLine = "${GMT_PATH}surface "+ interpRatioXYZName +" -G"+ interpRatioUnsampledGRD+ " -I"+interpGridSpacing
								+region+interpSettings.getConvergenceArg()+" "+interpSettings.getSearchArg()
								+" "+interpSettings.getTensionArg()+" -: -H0 2>/dev/null";
				gmtCommandLines.add(commandLine);
				// resample the interpolation
				
				rmFiles.add(interpRatioSampledGRD);
				if (interpGridSpacing == mapGridSpacing) {
					gmtCommandLines.add("# the grid spacings are equal, we can just copy");
					gmtCommandLines.add("cp " + interpRatioUnsampledGRD + " " + interpRatioSampledGRD);
				} else {
					gmtCommandLines.add("# resample the interpolated file");
					boolean bicubic = false;
					commandLine = "${GMT_PATH}grdsample "+interpRatioUnsampledGRD+" -G"+interpRatioSampledGRD
									+" -I"+mapGridSpacing+region;
					if (!bicubic)
						commandLine += "-nl";
					gmtCommandLines.add(commandLine+"\n");
				}
			}
		}
		
		// get color scale limits
		double colorScaleMin, colorScaleMax;
		if (map.isCustomScale()) {
			colorScaleMin = map.getCustomScaleMin();
			colorScaleMax = map.getCustomScaleMax();
//			if (map.isLogPlot()) {
//				colorScaleMin = Math.log10(colorScaleMin);
//				colorScaleMax = Math.log10(colorScaleMax);
//			}
			if (colorScaleMin >= colorScaleMax)
				throw new RuntimeException("Error: Color-Scale Min must be less than the Max");
		}
		else {
			double minGrid;
			double maxGrid;
			if (griddedData != null) {
				MinMaxAveTracker baseTracker = calcExtentsWithinRegion(griddedData, map.getRegion());
				if (baseTracker.getNum() == 0)
					throw new GMT_MapException("Base map has no points within mask region!");
				minGrid = baseTracker.getMin();
				maxGrid = baseTracker.getMax();
			} else {
				minGrid = Double.POSITIVE_INFINITY;
				maxGrid = Double.NEGATIVE_INFINITY;
			}
			colorScaleMin = minGrid;
			colorScaleMax = maxGrid;
			if (scatterData != null) {
				// if this is just a basemap, scatter might be null!
				double minScatter = scatterData.getMinZ();
				double maxScatter = scatterData.getMaxZ();
				if (minScatter < minGrid)
					colorScaleMin = minScatter;
				if (maxScatter > maxGrid)
					colorScaleMax = maxScatter;
			}
			System.out.println(colorScaleMin+","+colorScaleMax);
			if (colorScaleMin == colorScaleMax)
				throw new RuntimeException("Can't make the image plot because all Z values in the XYZ dataset have the same value ");
		}
		
		// write the CPT
		String inputCPT;
		String diffCPTfile = null;
		CPT diffCPT = null;
		String ratioCPTfile = null;
		CPT ratioCPT = null;
		CPT cpt = null;
		for (InterpDiffMapType mapType : mapTypes) {
			if (mapType == InterpDiffMapType.DIFF) {
				try {
//					diffCPT = GMT_CPT_Files.GMT_POLAR.instance();
					diffCPT = getDiffCPT();
					diffCPTfile = "cptFile_diff.cpt";
					diffCPT.writeCPTFile(dir+diffCPTfile);
				} catch (IOException e) {
					throw new GMT_MapException("Could not write diff CPT file", e);
				}
			} else if (mapType == InterpDiffMapType.RATIO) {
				try {
//					ratioCPT = GMT_CPT_Files.GMT_POLAR.instance();
					ratioCPT = getRatioCPT();
					ratioCPTfile = "cptFile_ratio.cpt";
					ratioCPT.writeCPTFile(dir+ratioCPTfile);
				} catch (IOException e) {
					throw new GMT_MapException("Could not write ratio CPT file", e);
				}
			}
		}
		if (map.getCptFile() != null) {
			inputCPT = GMT_MapGenerator.GMT_DATA_PATH + map.getCptFile();
		} else {
			inputCPT = "cptFile_input.cpt";
			cpt = map.getCpt();
			try {
				cpt.writeCPTFile(dir + inputCPT);
			} catch (IOException e) {
				throw new GMT_MapException("Could not write custom CPT file", e);
			}
		}
		
		String cptFile;
		if (map.isRescaleCPT()) {
			cptFile = "cptFile.cpt";
			// make the cpt file
			float inc = (float) ((colorScaleMax-colorScaleMin)/20);
			gmtCommandLines.add("# Rescale the CPT file");
			commandLine="${GMT_PATH}makecpt -C" + inputCPT + " -T" + colorScaleMin +"/"+ colorScaleMax +"/" + inc + " -Z > "+cptFile;
			gmtCommandLines.add(commandLine+"\n");
		} else {
			cptFile = inputCPT;
		}
		
		gmtCommandLines.add("# Set GMT paper/font defaults");
		// set some defaults
		String pageColor, frameColor, mapPenColor;
		if (map.isBlackBackground()) {
			pageColor = "black";
			frameColor = "white";
			mapPenColor = "+"+frameColor;
		} else {
			pageColor = "white";
			frameColor = "black";
			mapPenColor = frameColor;
		}
		boolean laRegion = (float)map.getRegion().getMaxLat() == 35.08f && (float)map.getRegion().getMinLat() == 33.25f;
		String mediaName;
		if (laRegion)
			// LA region
			mediaName = "csmap";
		else
			mediaName = "letter";
		commandLine = "${GMT_PATH}gmtset FONT_ANNOT_PRIMARY=14p,"+frameColor+" FONT_LABEL=18p,"+frameColor+" PS_PAGE_COLOR" +
				"="+pageColor+" PS_PAGE_ORIENTATION=portrait PS_MEDIA="+mediaName+" MAP_DEFAULT_PEN="+mapPenColor +
				" FORMAT_GEO_MAP=-D MAP_FRAME_WIDTH=0.1i COLOR_FOREGROUND="+frameColor+" MAP_FRAME_PEN=1p";
		gmtCommandLines.add(commandLine+"\n");
		
		String interpPlotGRD;
		if (shouldInterp && griddedData != null) {
			interpPlotGRD = "interp_diff.grd";
			// combine the basemap and interpolated map
			gmtCommandLines.add("# add the interpolated vals to the basemap");
			gmtCommandLines.add("${GMT_PATH}grdmath "+baseGRD+" "+interpSampledGRD+" ADD = "+interpPlotGRD+"\n");
		} else {
			interpPlotGRD = interpSampledGRD;
		}
		
		String intenGRD = null;
		TopographicSlopeFile topoFile = map.getTopoResolution();
		if (shouldMakeTopo) {
			// redefine the region so that maxLat, minLat, and delta fall exactly on the topoIntenFile
			// i don't think we need this, we'll add it back in if needed
//			double topoGridSpacing = GeoTools.secondsToDeg(map.getTopoResolution().resolution());
//			double tempNum = Math.ceil((minLat-topoFile.region().getMinLat())/topoGridSpacing);
//			minLat = tempNum*topoGridSpacing+topoFile.region().getMinLat();
//			tempNum = Math.ceil((minLon-(topoFile.region().getMinLon()))/topoGridSpacing);
//			minLon = tempNum*topoGridSpacing+(topoFile.region().getMinLon());
//			maxLat = Math.floor(((maxLat-minLat)/topoGridSpacing))*topoGridSpacing +minLat;
//			maxLon = Math.floor(((maxLon-minLon)/topoGridSpacing))*topoGridSpacing +minLon;
//			String tregion = " -R" + minLon + "/" + maxLon + "/" + minLat + "/" + maxLat + " ";
			String topoIntenFile = GMT_MapGenerator.GMT_DATA_PATH + topoFile.fileName();

			intenGRD = "topo_inten.grd";
			gmtCommandLines.add("# Cut the topo file to match the data region");
			commandLine="${GMT_PATH}grdcut " + topoIntenFile + " -G"+intenGRD+ " " +region;
			rmFiles.add(intenGRD);
			gmtCommandLines.add(commandLine);
		}
		
		String maskGRD = null;
		if (!map.getRegion().isRectangular()) {
			String maskName = "mask.xy";
			maskGRD = "mask.grd";
			rmFiles.add(maskGRD);
			try {
				GMT_MapGenerator.writeMaskFile(map.getRegion(), dir+maskName);
			} catch (IOException e) {
				throw new GMT_MapException("Couldn't write mask file!", e);
			}
			String spacing;
			if (shouldMakeTopo)
				spacing = topoFile.resolution() + "c";
			else
				spacing = mapGridSpacing + "";
			gmtCommandLines.add("# creat mask");
			commandLine = "${GMT_PATH}grdmask "+maskName+region+" -I"+spacing+" -NNaN/1/1 -G"+maskGRD;
			gmtCommandLines.add(commandLine+"\n");
		}
		
		String xOff = " -X1.5i";
		String yOff = " -Y2.0i";
		
		double plotHght = ((maxLat-minLat)/(maxLon-minLon))*plotWdth/Math.cos(Math.PI*(maxLat+minLat)/(2*180));
		double yOffset = 11 - plotHght - 0.5;
		if (!laRegion) {
			yOff = " -Y" + yOffset + "i ";

			// set x-axis offset to 1 inch
			xOff = " -X1.0i ";
		}
		
		for (InterpDiffMapType mapType : mapTypes) {
			gmtCommandLines.add("# PLOTTING: "+mapType+"\n");
			String grdFile;
			String psFile = mapType.getPrefix()+".ps";
			Color markerColor = mapType.getMarkerColor();
			boolean markers = markerColor != null;
			String myCPTFileName = cptFile;
			double myCPTMin = colorScaleMin;
			double myCPTMax = colorScaleMax;
			CPT myCPT = cpt;
			String scaleLabel = map.getCustomLabel();
			boolean cptEqualSpacing = map.isCPTEqualSpacing();
			if (mapType == InterpDiffMapType.BASEMAP) {
				grdFile = baseGRD;
				if (map.isAutoLabel())
					scaleLabel = "GMPE Basemap, "+scaleLabel;
			} else if (mapType == InterpDiffMapType.INTERP_MARKS) {
				grdFile = interpPlotGRD;
				if (map.isAutoLabel())
					scaleLabel = "CyberShake Hazard Map, "+scaleLabel;
			} else if (mapType == InterpDiffMapType.INTERP_NOMARKS) {
				grdFile = interpPlotGRD;
				if (map.isAutoLabel())
					scaleLabel = "CyberShake Hazard Map, "+scaleLabel;
			} else if (mapType == InterpDiffMapType.DIFF) {
				if (griddedData == null)
					continue;
				myCPTFileName = diffCPTfile;
				myCPTMin = diffCPT.getMinValue();
				myCPTMax = diffCPT.getMaxValue();
				myCPT = diffCPT;
				grdFile = interpSampledGRD;
				if (map.isAutoLabel())
					scaleLabel = "Difference Map, "+scaleLabel;
			} else if (mapType == InterpDiffMapType.RATIO) {
				// ratios!
				if (griddedData == null)
					continue;
				myCPTFileName = ratioCPTfile;
				myCPTMin = ratioCPT.getMinValue();
				myCPTMax = ratioCPT.getMaxValue();
				myCPT = ratioCPT;
				grdFile = interpRatioSampledGRD;
				if (map.isAutoLabel())
					scaleLabel = "Ratio Map, "+scaleLabel;
				cptEqualSpacing = true;
			} else {
				throw new IllegalStateException("Unknown map type: "+mapType);
			}
			
			if (markers && myCPT != null && myCPT.get(0).minColor.equals(Color.BLUE)
					&& myCPT.get(myCPT.size()-1).maxColor.equals(Color.RED))
				// little hack for polar images
				markerColor = Color.BLACK;
			
			int dpi = map.getDpi();
			String gmtSmoothOption="";
			if (!map.isUseGMTSmoothing()) gmtSmoothOption=" -T ";
			// generate the image depending on whether topo relief is desired
			String topoOption = "";
			if (shouldMakeTopo) {
				String topoResGRD = "topores_"+grdFile;
				rmFiles.add(topoResGRD);
				gmtCommandLines.add("# Resample the map to the topo resolution");
				commandLine="${GMT_PATH}grdsample "+grdFile+" -G"+topoResGRD+" -I" +
				topoFile.resolution() + "c -nl "+region;
				gmtCommandLines.add(commandLine);
				grdFile = topoResGRD;
//				if (mapType != InterpDiffMapType.DIFF && mapType != InterpDiffMapType.RATIO)
					topoOption = " -I"+intenGRD;
			}
			if (maskGRD != null) {
				String unmaskedGRD = "unmasked_"+grdFile;
				rmFiles.add(unmaskedGRD);
				gmtCommandLines.add("mv "+grdFile+" "+unmaskedGRD);
				gmtCommandLines.add("${GMT_PATH}grdmath "+unmaskedGRD+" "+maskGRD+" MUL = "+grdFile+"\n");
			}
			gmtCommandLines.add("# Plot the gridded data");
			commandLine="${GMT_PATH}grdimage "+ grdFile + xOff + yOff + proj + topoOption + " -C"+myCPTFileName+
							" "+gmtSmoothOption+" -K -E"+dpi+ region + " > " + psFile;
			gmtCommandLines.add(commandLine+"\n");
			
			if (markers) {
				gmtCommandLines.add("# scatter markers");
				// TODO fix
				
				String pen1 = "0.03i";
				String pen2 = "0.0162i";
				Region r = map.getRegion();
				double maxSpan = Math.max(r.getMaxLat()-r.getMinLat(), r.getMaxLon()-r.getMinLon());
				if (maxSpan < 3d) {
					pen1 = "0.06i";
					pen2 = "0.0342i";
				}
				
				// write out file
				String symbolFile = "symbol_set.xy";
				gmtCommandLines.add("${COMMAND_PATH}cat  << END > " + symbolFile);
				boolean symbolCPT = map.isUseCPTForScatterColor() && myCPT != null;
				for (int i=0; i<scatterData.size(); i++) {
					Point2D point = scatterData.getPoint(i);
					String line = point.getX() + "\t" + point.getY();
					if (symbolCPT)
						line += "\t" + (float)scatterData.get(i);
					gmtCommandLines.add(line);
				}
				gmtCommandLines.add("END");
				
				if (symbolCPT) {
					commandLine = "${GMT_PATH}psxy "+symbolFile+" "+region+proj+"-S"+ScatterSymbol.SYMBOL_INVERTED_TRIANGLE
							+pen1+" -C"+myCPTFileName+" -W"+pen2+" -: -K -O >> "+psFile;
					// TODO -G?
				} else {
					String colorStr = GMT_MapGenerator.getGMTColorString(markerColor);
					commandLine = "${GMT_PATH}psxy "+symbolFile+" "+region+proj+"-S"+ScatterSymbol.SYMBOL_INVERTED_TRIANGLE
							+pen1+" -G"+colorStr + " -W"+pen2+","+colorStr + " -: -K -O >> "+psFile;
				}
				gmtCommandLines.add(commandLine);
				
				// old way
//				for (int i=0; i<scatterData.size(); i++) {
//					Point2D pt = scatterData.getPoint(i);
//					double x = pt.getX();
//					double y = pt.getY();
//					
//					if (map.isUseCPTForScatterColor() && myCPT != null)
//						colorStr = GMT_MapGenerator.getGMTColorString(myCPT.getColor((float)scatterData.get(i)));
//					
//					commandLine = "echo " + x + " " + y + " | ";
//					commandLine += "${GMT_PATH}psxy"+region+proj+"-S"+ScatterSymbol.SYMBOL_INVERTED_TRIANGLE
//									+"0.03i -G"+colorStr + " -W0.0162i,"+colorStr + " -: -K -O >> "+psFile;
//					gmtCommandLines.add(commandLine);
//				}
			}
			
			GMT_MapGenerator.addSpecialElements(gmtCommandLines, map, region, proj, psFile);
			
			GMT_MapGenerator.addColorbarCommand(gmtCommandLines, scaleLabel, map.isLogPlot(),
					myCPTMin, myCPTMax, myCPTFileName, psFile, cptEqualSpacing, map.getCPTCustomInterval());
			
			gmtCommandLines.add("# basemap");
			commandLine = "${GMT_PATH}psbasemap -B0.5/0.5eWNs"+region+proj+"-O >> "+psFile;
			gmtCommandLines.add(commandLine+"\n");
			
			gmtCommandLines.add("# conversions");
			for (int odpi : dpis) {
				String cropArgs = "";
				if (!laRegion) {
					int widthInPixels = (int)(8.263888889*odpi + 0.5); // 595 for 72dpi
					int heightInPixels = (int) ((11.0 - yOffset + 2.0) * (double) odpi);
					cropArgs = " -crop "+widthInPixels+"x"+heightInPixels+"+0+0";
				}
				String convertArgs = "-density " + odpi+cropArgs;
				String fName = mapType.getPrefix() + "." + odpi + ".png";

				// add a command line to convert the ps file to a jpg file - using convert
				gmtCommandLines.add("${CONVERT_PATH} " + convertArgs + " " + psFile + " " + fName);
			}
		}
		
		GMT_MapGenerator.addCleanup(gmtCommandLines, rmFiles);
		
		System.out.println("DONE generating map script for dir: " + dir);
		
		return gmtCommandLines;
	}
	
	public File plotLocally(InterpDiffMap map, File gmtDataDir) throws GMT_MapException, IOException {
		if (gmtDataDir != null)
			GMT_MapGenerator.GMT_DATA_PATH = gmtDataDir.getAbsolutePath()+File.separator;
		GMT_MapGenerator.clearEnv();
		
		CS_InterpDiffMapServletAccessor.checkLog(map);
		
		File tempDir = Files.createTempDir();
		
		List<String> script = getGMT_ScriptLines(map, tempDir.getAbsolutePath());
		
		File scriptFile = new File(tempDir, "script.sh");
		
		if (map.getGriddedData() != null) {
			GeoDataSet griddedData = map.getGriddedData();
			griddedData.setLatitudeX(true);
			ArbDiscrGeoDataSet.writeXYZFile(griddedData,
					tempDir.getAbsolutePath()+"/"+new File(map.getXyzFileName()).getName());
		}
		
		FileWriter fw = new FileWriter(scriptFile);
		BufferedWriter bw = new BufferedWriter(fw);
		for (String line : script)
			bw.write(line + "\n");
		bw.close();
		
		String[] command = {
				"sh", "-c", "/bin/bash "+scriptFile.getAbsolutePath()+" > /dev/null 2> /dev/null"};
		RunScript.runScript(command);
		
		return tempDir;
	}

}
