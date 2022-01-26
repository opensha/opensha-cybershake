package org.opensha.sha.cybershake.openshaAPIs;

import java.util.ArrayList;

import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationList;
import org.opensha.sha.faultSurface.AbstractEvenlyGriddedSurface;
import org.opensha.sha.faultSurface.AbstractEvenlyGriddedSurfaceWithSubsets;
import org.opensha.sha.faultSurface.FaultTrace;

public class CyberShakeEvenlyGriddedSurface extends AbstractEvenlyGriddedSurfaceWithSubsets {

	public CyberShakeEvenlyGriddedSurface( int numRows, int numCols, double gridSpacing) {
		super(numRows, numCols, gridSpacing);
	}
	
	public void setAllLocations(ArrayList<Location> locs) {
		int num = numRows * numCols;
		if (num != locs.size())
			throw new RuntimeException("ERROR: Not the right amount of locations! (expected " + num + ", got " + locs.size() + ")");
		
		int count = 0;
		
		for (int i=0; i<numRows; i++) {
			for (int j=0; j<numCols; j++) {
				this.set(i, j, locs.get(count));
				
				count++;
			}
		}
	}

	public void set( int row, int column, Location loc ) throws ArrayIndexOutOfBoundsException {

		String S = C + ": set(): ";
		checkBounds( row, column, S );
		data[row * numCols + column] = loc;
	}

	@Override
	public double getAveDip() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getAveDipDirection() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getAveRupTopDepth() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getAveStrike() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public LocationList getPerimeter() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FaultTrace getUpperEdge() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected AbstractEvenlyGriddedSurface getNewInstance() {
		return new CyberShakeEvenlyGriddedSurface(numRows, numCols, gridSpacingAlong);
	}

}
