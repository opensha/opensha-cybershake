package org.opensha.sha.cybershake.maps;

import org.opensha.commons.geo.GriddedRegion;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.geo.Region;
import org.opensha.commons.util.DataUtils.MinMaxAveTracker;

public class CustomGriddedRegion extends GriddedRegion {
	
	private LocationList nodeList;
	
	private static Region getRegion(LocationList nodeList) {
		MinMaxAveTracker latTrack = new MinMaxAveTracker();
		MinMaxAveTracker lonTrack = new MinMaxAveTracker();
		for (Location loc : nodeList) {
			latTrack.addValue(loc.getLatitude());
			lonTrack.addValue(loc.getLongitude());
		}
		Location northEast = new Location(latTrack.getMax(), lonTrack.getMin());
		Location southWest = new Location(latTrack.getMin(), lonTrack.getMax());
		return new Region(northEast, southWest);
	}
	
	public CustomGriddedRegion(LocationList nodeList) {
		super(getRegion(nodeList), 0.5, null);
		this.nodeList = nodeList;
	}

	@Override
	public int getNodeCount() {
		return nodeList.size();
	}

	@Override
	public LocationList getNodeList() {
		return nodeList;
	}

	@Override
	public int indexForLocation(Location loc) {
		return nodeList.indexOf(loc);
	}

	@Override
	public Location locationForIndex(int index) {
		return nodeList.get(index);
	}

}
