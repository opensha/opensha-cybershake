package org.opensha.sha.calc.mcer;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.data.xyz.GriddedGeoDataSet;
import org.opensha.commons.exceptions.GMT_MapException;
import org.opensha.commons.geo.BorderType;
import org.opensha.commons.geo.GriddedRegion;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.geo.Region;
import org.opensha.commons.mapping.gmt.GMT_Map;
import org.opensha.commons.mapping.gmt.elements.GMT_CPT_Files;
import org.opensha.commons.util.cpt.CPT;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import scratch.UCERF3.analysis.FaultBasedMapGen;

public class TLDataLoader {
	
	private List<Region> regions;
	private List<Double> values;
	
	private static final double distance_buffer = 10; // buffer in km for really close but just outside
	
	public TLDataLoader(CSVFile<String> polygons, CSVFile<String> attributes) {
		regions = Lists.newArrayList();
		values = Lists.newArrayList();
		
		Map<String, Double> shapeIDtoVals = Maps.newHashMap();
		
		// load in values
		for (int row=1; row<attributes.getNumRows(); row++) {
			String shapeID = attributes.get(row, 0);
			double value = Double.parseDouble(attributes.get(row, 2));
			shapeIDtoVals.put(shapeID, value);
		}
		
		// load in polygons
		Map<String, LocationList> shapeIDtoBoundaries = Maps.newHashMap();
		
		for (int row=1; row<polygons.getNumRows(); row++) {
			String shapeID = polygons.get(row, 0);
			double lon = Double.parseDouble(polygons.get(row, 1));
			double lat = Double.parseDouble(polygons.get(row, 2));
			Location loc = new Location(lat, lon);
			LocationList boundary = shapeIDtoBoundaries.get(shapeID);
			if (boundary == null) {
				boundary = new LocationList();
				shapeIDtoBoundaries.put(shapeID, boundary);
			}
			boundary.add(loc);
		}
		
		for (String shapeID : shapeIDtoVals.keySet()) {
			double val = shapeIDtoVals.get(shapeID);
			LocationList boundary = shapeIDtoBoundaries.get(shapeID);
			Region region = new Region(boundary, BorderType.GREAT_CIRCLE);
			regions.add(region);
			values.add(val);
		}
	}
	
	public double getValue(Location loc) {
		Preconditions.checkNotNull(loc);
		for (int i=0; i<regions.size(); i++)
			if (regions.get(i).contains(loc))
				return values.get(i);
		// ok see if it's a really near miss that we should still include
		double minDistance = Double.POSITIVE_INFINITY;
		int minIndex = -1;
		for (int i=0; i<regions.size(); i++) {
			double dist = regions.get(i).distanceToLocation(loc);
			if (dist < minDistance) {
				minDistance= dist;
				minIndex = i;
			}
		}
		if (minDistance < distance_buffer)
			return values.get(minIndex);
		return Double.NaN;
	}

	public static void main(String[] args) throws IOException, GMT_MapException {
//		TLDataLoader tl = new TLDataLoader(
//				CSVFile.readFile(new File("/tmp/temp-nodes.csv"), true),
//				CSVFile.readFile(new File("/tmp/temp-attributes.csv"), true));
		TLDataLoader tl = new TLDataLoader(
				CSVFile.readStream(TLDataLoader.class.getResourceAsStream("/resources/data/site/USGS_TL/tl-nodes.csv"), true),
				CSVFile.readStream(TLDataLoader.class.getResourceAsStream("/resources/data/site/USGS_TL/tl-attributes.csv"), true));
		
		double spacing = 0.01;
		GriddedRegion gridReg = new GriddedRegion(new CaliforniaRegions.CYBERSHAKE_MAP_REGION(), spacing, null);
		GriddedGeoDataSet data = new GriddedGeoDataSet(gridReg, false);
		int numWith = 0;
		int total = data.size();
		for (int i=0; i<data.size(); i++) {
			double z = tl.getValue(data.getLocation(i));
			if (!Double.isNaN(z))
				numWith++;
			data.set(i, z);
		}
		
		System.out.println(numWith+"/"+total+" have TsubL data!");
		
		CPT cpt = GMT_CPT_Files.MAX_SPECTRUM.instance().rescale(8, 12d);
		cpt.setNanColor(Color.GRAY);
		GMT_Map map = FaultBasedMapGen.buildMap(cpt, null, null, data, spacing, gridReg, false, "TL Data");
//		FaultBasedMapGen.plotMap(new File("/tmp"), "tl_data", false, map);
		FaultBasedMapGen.plotMap(new File("/tmp"), "tl_data_no_buf", false, map);
		
		System.exit(0);
	}

}
