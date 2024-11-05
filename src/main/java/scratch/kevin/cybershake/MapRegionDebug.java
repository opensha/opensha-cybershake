package scratch.kevin.cybershake;

import java.io.IOException;
import java.util.Random;

import org.opensha.commons.data.xyz.ArbDiscrGeoDataSet;
import org.opensha.commons.data.xyz.GriddedGeoDataSet;
import org.opensha.commons.exceptions.GMT_MapException;
import org.opensha.commons.geo.GriddedRegion;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.Region;
import org.opensha.commons.mapping.gmt.elements.GMT_CPT_Files;
import org.opensha.commons.mapping.gmt.elements.TopographicSlopeFile;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.maps.GMT_InterpolationSettings;
import org.opensha.sha.cybershake.maps.HardCodedInterpDiffMapCreator;
import org.opensha.sha.cybershake.maps.InterpDiffMap;
import org.opensha.sha.cybershake.maps.InterpDiffMap.InterpDiffMapType;
import org.opensha.sha.cybershake.maps.servlet.CS_InterpDiffMapServletAccessor;

public class MapRegionDebug {

	public static void main(String[] args) throws GMT_MapException, IOException, ClassNotFoundException {
		Region reg = CyberShakeStudy.STUDY_24_8_LF.getRegion();
		
		double basemapInc = 0.005;
		boolean logPlot = false;
		boolean LOCAL_MAPGEN = false;
		
		GriddedRegion baseMapGridded = new GriddedRegion(reg, basemapInc, GriddedRegion.ANCHOR_0_0);
		GriddedGeoDataSet xyz = new GriddedGeoDataSet(baseMapGridded, true);
		Random r = new Random(baseMapGridded.getNodeCount());
		for (int i=0; i<xyz.size(); i++)
			xyz.set(i, r.nextDouble());
		
		int numScatters = 100;
		ArbDiscrGeoDataSet scatter = new ArbDiscrGeoDataSet(true);
		for (int i=0; i<numScatters; i++) {
			Location loc = baseMapGridded.getLocation(r.nextInt(baseMapGridded.getNodeCount()));
			loc = new Location(loc.lat + 0.0001*(r.nextDouble()-0.5), loc.lon + 0.0001*(r.nextDouble()-0.5));
			scatter.set(loc, r.nextDouble()-0.5);
		}
		
		CPT cpt = GMT_CPT_Files.RAINBOW_UNIFORM.instance();
		
		InterpDiffMapType[] types = {
				InterpDiffMapType.BASEMAP,
				InterpDiffMapType.INTERP_NOMARKS
		};
		GMT_InterpolationSettings interpSettings = GMT_InterpolationSettings.getDefaultSettings();
		interpSettings.setInterpSpacing(basemapInc);
		InterpDiffMap map = new InterpDiffMap(reg, xyz, basemapInc, cpt, scatter, interpSettings, types);
		map.setCustomLabel("Title");
		map.setTopoResolution(TopographicSlopeFile.CA_THREE);
		map.setLogPlot(logPlot);
		map.setDpi(300);
		map.setXyzFileName("base_map.xyz");
		map.setCustomScaleMin(0d);
		map.setCustomScaleMax((double)cpt.getMaxValue());
//		map.getInterpSettings().setSaveInterpSurface(saveInterp);
		
		String metadata = "meta";
		
		System.out.println("Making map...");
		String addr;
		if (LOCAL_MAPGEN)
			addr = HardCodedInterpDiffMapCreator.plotLocally(map);
		else
			addr = CS_InterpDiffMapServletAccessor.makeMap(null, map, metadata);
		System.out.println(addr);
	}

}
