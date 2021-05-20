package org.opensha.sha.cybershake.maps;

import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.geo.Location;

public class ProbGainCalc {

	public static ArbDiscrGeoDataSet calcProbGain(GeoDataSet refXYZ, GeoDataSet modXYZ) {
		ArbDiscrGeoDataSet gainXYZ = new ArbDiscrGeoDataSet(true);

		for (int refInd=0; refInd<refXYZ.size(); refInd++) {
			Location refLoc = refXYZ.getLocation(refInd);
			double refZ = refXYZ.get(refInd);
			for (int modInd=0; modInd<modXYZ.size(); modInd++) {
				Location modLoc = modXYZ.getLocation(modInd);
				double modZ = modXYZ.get(modInd);

				if (!refLoc.equals(modLoc))
					continue;

				double gain = modZ / refZ;

				gainXYZ.set(modLoc, gain);
			}
		}

		return gainXYZ;
	}

	public static ArbDiscrGeoDataSet calcProbDiff(GeoDataSet refXYZ, GeoDataSet modXYZ) {
		ArbDiscrGeoDataSet gainXYZ = new ArbDiscrGeoDataSet(true);

		for (int refInd=0; refInd<refXYZ.size(); refInd++) {
			Location refLoc = refXYZ.getLocation(refInd);
			double refZ = refXYZ.get(refInd);
			for (int modInd=0; modInd<modXYZ.size(); modInd++) {
				Location modLoc = modXYZ.getLocation(modInd);
				double modZ = modXYZ.get(modInd);

				if (!refLoc.equals(modLoc))
					continue;

				double gain = modZ - refZ;

				gainXYZ.set(modLoc, gain);
			}
		}

		return gainXYZ;
	}

}
