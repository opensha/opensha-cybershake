package scratch.kevin.cybershake;

import java.awt.Color;
import java.io.File;
import java.io.IOException;

import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.data.xyz.GriddedGeoDataSet;
import org.opensha.commons.exceptions.GMT_MapException;
import org.opensha.commons.geo.BorderType;
import org.opensha.commons.geo.GriddedRegion;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.commons.geo.Region;
import org.opensha.commons.mapping.gmt.GMT_Map;
import org.opensha.commons.mapping.gmt.elements.GMT_CPT_Files;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.SiteInfo2DB;

import scratch.UCERF3.analysis.FaultBasedMapGen;

public class SmallerPolygonGen {

	public static void main(String[] args) throws IOException, GMT_MapException {
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB();
		
		SiteInfo2DB sites2db = new SiteInfo2DB(db);
		Location upperLeft = sites2db.getLocationForSite("s040");
		Location upperRight = sites2db.getLocationForSite("s778");
		Location lowerRight = sites2db.getLocationForSite("s758");
		// move down 10 km
		lowerRight = LocationUtils.location(lowerRight, Math.toRadians(LocationUtils.azimuth(upperRight, lowerRight)), 10d);
		Location lowerLeft = sites2db.getLocationForSite("s022");
		// move down 20 km
		lowerLeft = LocationUtils.location(lowerLeft, Math.toRadians(LocationUtils.azimuth(upperLeft, lowerLeft)), 20d);
		
		System.out.println(upperLeft);
		System.out.println(upperRight);
		System.out.println(lowerRight);
		System.out.println(lowerLeft);
		
		LocationList border = new LocationList();
		border.add(upperLeft);
		border.add(upperRight);
		border.add(lowerRight);
		border.add(lowerLeft);
		
		Region smaller = new Region(border, BorderType.GREAT_CIRCLE);
		
		double spacing = 0.01;
		GriddedRegion gridReg = new GriddedRegion(new CaliforniaRegions.CYBERSHAKE_MAP_REGION(), spacing, null);
		GriddedGeoDataSet data = new GriddedGeoDataSet(gridReg, false);
		for (int i=0; i<data.size(); i++) {
			if (smaller.contains(data.getLocation(i)))
				data.set(i, 1d);
			else
				data.set(i, 0d);
		}
		System.out.println((int)data.getSumZ()+"/"+data.size()+" are in smaller region!");
		
		CPT cpt = GMT_CPT_Files.MAX_SPECTRUM.instance().rescale(0, 1d);
		cpt.setNanColor(Color.GRAY);
		GMT_Map map = FaultBasedMapGen.buildMap(cpt, null, null, data, spacing, gridReg, false, "Smaller Region");
		FaultBasedMapGen.plotMap(new File("/tmp"), "cs_region", false, map);
		
		db.destroy();
	}

}
