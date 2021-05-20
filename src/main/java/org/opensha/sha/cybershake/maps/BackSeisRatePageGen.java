package org.opensha.sha.cybershake.maps;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.data.xyz.GriddedGeoDataSet;
import org.opensha.commons.exceptions.GMT_MapException;
import org.opensha.commons.geo.GriddedRegion;
import org.opensha.commons.geo.Location;
import org.opensha.commons.mapping.gmt.GMT_Map;
import org.opensha.commons.mapping.gmt.elements.GMT_CPT_Files;
import org.opensha.commons.util.MarkdownUtils;
import org.opensha.commons.util.MarkdownUtils.TableBuilder;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.earthquake.rupForecastImpl.Frankel02.Point2Vert_SS_FaultPoisSource;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.UCERF2;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.griddedSeis.Point2Vert_FaultPoisSource;
import org.opensha.sha.faultSurface.RuptureSurface;

import com.google.common.base.Preconditions;

import scratch.UCERF3.analysis.FaultBasedMapGen;

public class BackSeisRatePageGen {

	public static void main(String[] args) throws IOException, GMT_MapException {
		File mainOutputDir = new File("/home/kevin/markdown/cybershake-analysis/");
		CyberShakeStudy study = CyberShakeStudy.STUDY_15_4;
		
		double[] mags = { 5d, 6d, 6.5, 7d, 7.5, 8d };
		
		File studyDir = new File(mainOutputDir, study.getDirName());
		Preconditions.checkState(studyDir.exists() || studyDir.mkdir());
		
		File mapsDir = new File(studyDir, "erf_background_seismicity_maps");
		Preconditions.checkState(mapsDir.exists() || mapsDir.mkdir());
		
		File resourcesDir = new File(mapsDir, "resources");
		Preconditions.checkState(resourcesDir.exists() || resourcesDir.mkdir());
		
		AbstractERF erf = study.getERF();
		
		List<String> lines = new ArrayList<>();
		lines.add("# "+erf.getName()+" Background Seismicity rate maps");
		lines.add("");
		
		TableBuilder table = MarkdownUtils.tableBuilder();
		table.addLine("Min Mag", "Background Sources", "Fault Sources");
		
		erf.getParameter(UCERF2.BACK_SEIS_NAME).setValue(UCERF2.BACK_SEIS_EXCLUDE);
		erf.updateForecast();
		
		GriddedGeoDataSet[] backXYZs = new GriddedGeoDataSet[mags.length];
		GriddedGeoDataSet[] faultXYZs = new GriddedGeoDataSet[mags.length];
		
		for (int m=0; m<mags.length; m++) {
			System.out.println("Calculating fault, M"+(float)mags[m]);
			faultXYZs[m] = calcRateXYZ(erf, mags[m]);
		}
		
		erf.getParameter(UCERF2.BACK_SEIS_NAME).setValue(UCERF2.BACK_SEIS_ONLY);
		erf.getParameter(UCERF2.BACK_SEIS_RUP_NAME).setValue(UCERF2.BACK_SEIS_RUP_POINT);
		erf.updateForecast();
		
		for (int m=0; m<mags.length; m++) {
			System.out.println("Calculating back, M"+(float)mags[m]);
			backXYZs[m] = calcRateXYZ(erf, mags[m]);
		}
		
		for (int m=0; m<mags.length; m++) {
			double maxZ = Double.MIN_VALUE;
			double minZ = Double.MAX_VALUE;
			
			for (int i=0; i<faultXYZs[m].size(); i++) {
				double z = faultXYZs[m].get(i);
				if (Double.isFinite(z)) {
					maxZ = Math.max(maxZ, z);
					minZ = Math.min(minZ, z);
				}
			}
			
			for (int i=0; i<backXYZs[m].size(); i++) {
				double z = backXYZs[m].get(i);
				if (Double.isFinite(z)) {
					maxZ = Math.max(maxZ, z);
					minZ = Math.min(minZ, z);
				}
			}
			
			String faultPrefix = "fault_rates_m"+(float)mags[m];
			String backPrefix = "back_rates_m"+(float)mags[m];
			
			System.out.println("Plotting M"+(float)mags[m]);
			
			writeRateMap(resourcesDir, backPrefix, backXYZs[m],
					"Log10 M>="+(float)mags[m]+" Background Seismicity Rate", minZ, maxZ);
			
			writeRateMap(resourcesDir, faultPrefix, faultXYZs[m],
					"Log10 M>="+(float)mags[m]+" Fault Rate", minZ, maxZ);
			
			table.initNewLine();
			table.addColumn("**M&ge;"+(float)mags[m]+"**");
			table.addColumn("![Background Map](resources/"+backPrefix+".png)");
			table.addColumn("![Fault Map](resources/"+faultPrefix+".png)");
			table.finalizeLine();
		}
		lines.addAll(table.build());

		// write markdown
		MarkdownUtils.writeReadmeAndHTML(lines, mapsDir);
	}
	
//	private static Map<RuptureSurface, HashSet<Integer>> gridNodeCache = new HashMap<>();
	
	private static GriddedGeoDataSet calcRateXYZ(AbstractERF erf, double minMag) {
		GriddedRegion gridReg = new CaliforniaRegions.RELM_TESTING_GRIDDED(0.1d);
		GriddedGeoDataSet xyz = new GriddedGeoDataSet(gridReg, false);
		
		double duration = erf.getTimeSpan().getDuration();
		
		for (ProbEqkSource source : erf) {
			for (ProbEqkRupture rup : source) {
				if (rup.getMag() < minMag)
					continue;

				RuptureSurface surf = rup.getRuptureSurface();
				HashSet<Integer> gridNodes = new HashSet<>();
				
				List<Location> locs = new ArrayList<>();
				if (surf instanceof Point2Vert_FaultPoisSource) {
					locs.add(((Point2Vert_FaultPoisSource)source).getLocation());
				} else if (surf instanceof Point2Vert_SS_FaultPoisSource) {
					locs.add(((Point2Vert_SS_FaultPoisSource)source).getLocation());
				} else {
//					locs.addAll(surf.getPerimeter());
					locs.addAll(surf.getEvenlyDiscritizedListOfLocsOnSurface());
				}

				for (Location loc : locs) {
					int node = gridReg.indexForLocation(loc);
					if (node >= 0)
						gridNodes.add(node);
				}
				
				double rate = rup.getMeanAnnualRate(duration);
				for (int node : gridNodes)
					xyz.set(node, xyz.get(node)+rate);
			}
		}
		
		xyz.log10();
		
		return xyz;
	}
	
	private static void writeRateMap(File resourcesDir, String prefix, GriddedGeoDataSet xyz,
			String label, double minZ, double maxZ) throws IOException, GMT_MapException {
		CPT cpt = GMT_CPT_Files.RAINBOW_UNIFORM.instance();
		cpt = cpt.rescale(minZ, maxZ);
		cpt.setNanColor(Color.WHITE);
		cpt.setBelowMinColor(Color.WHITE);
		
		for (int i=0; i<xyz.size(); i++)
			if (!Double.isFinite(xyz.get(i)))
				xyz.set(i, minZ - 1d);
		
		GMT_Map map = new GMT_Map(xyz.getRegion(), xyz, xyz.getRegion().getLatSpacing(), cpt);
		map.setCustomLabel(label);
		map.setCustomScaleMin(minZ);
		map.setCustomScaleMax(maxZ);
		map.setTopoResolution(null);
		map.setBlackBackground(false);
		map.setUseGMTSmoothing(false);
		
		FaultBasedMapGen.LOCAL_MAPGEN = true;
		FaultBasedMapGen.plotMap(resourcesDir, prefix, false, map);
	}

}
