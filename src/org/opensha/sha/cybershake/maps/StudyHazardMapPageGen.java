package org.opensha.sha.cybershake.maps;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.data.xyz.AbstractGeoDataSet;
import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.Region;
import org.opensha.commons.mapping.gmt.elements.PSXYSymbol;
import org.opensha.commons.mapping.gmt.elements.TopographicSlopeFile;
import org.opensha.commons.util.FileUtils;
import org.opensha.commons.util.MarkdownUtils;
import org.opensha.commons.util.MarkdownUtils.TableBuilder;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.cybershake.ModProbConfig;
import org.opensha.sha.cybershake.bombay.ModProbConfigFactory;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.maps.InterpDiffMap.InterpDiffMapType;
import org.opensha.sha.cybershake.maps.servlet.CS_InterpDiffMapServletAccessor;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;

public class StudyHazardMapPageGen {
	
	private static final boolean LOCAL_MAPGEN = false;

	public static void main(String[] args) throws IOException {
		File mainOutputDir = new File("/home/kevin/git/cybershake-analysis/");
		
		CyberShakeStudy study = CyberShakeStudy.STUDY_15_4;
		double[] periods = { 2d, 3d, 5d, 10d };
		CyberShakeComponent[] components = { CyberShakeComponent.GEOM_MEAN };
		ScalarIMR baseMapGMPE = AttenRelRef.NGA_2008_4AVG.instance(null);
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_18_8;
//		double[] periods = { 2d, 3d, 5d, 10d };
//		CyberShakeComponent[] components = { CyberShakeComponent.RotD50 };
//		ScalarIMR baseMapGMPE = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
		
		boolean replot = false;
		
		List<Double> probLevels = new ArrayList<>();
		List<String> probLabels = new ArrayList<>();
		List<String> probFileLables = new ArrayList<>();
		
		probLevels.add(4e-4);
		probLabels.add("2% in 50 yr");
		probFileLables.add("2in50");
		
//		probLevels.add(0.002);
//		probLabels.add("10% in 50 yr");
//		probFileLables.add("10in50");
		
		boolean isProbAt_IML = false;
		
		CPT hazardCPT = CPT.loadFromStream(HardCodedInterpDiffMapCreator.class.getResourceAsStream(
				"/org/opensha/sha/cybershake/conf/cpt/cptFile_hazard_input.cpt"));
//		CPT hazardCPT = CPT.loadFromStream(HardCodedInterpDiffMapCreator.class.getResourceAsStream(
//				"/resources/cpt/MaxSpectrum2.cpt"));
		hazardCPT.setNanColor(CyberShake_GMT_MapGenerator.OUTSIDE_REGION_COLOR);
		boolean logPlot = false;
		
//		CPT ratioCPT = CPT.loadFromStream(HardCodedInterpDiffMapCreator.class.getResourceAsStream(
//				"/org/opensha/sha/cybershake/conf/cpt/cptFile_ratio_tighter.cpt"));
////		CPT ratioCPT = CPT.loadFromStream(HardCodedInterpDiffMapCreator.class.getResourceAsStream(
////				"/org/opensha/sha/cybershake/conf/cpt/cptFile_ratio.cpt"));
//		ratioCPT.setNanColor(CyberShake_GMT_MapGenerator.OUTSIDE_REGION_COLOR);
		
		// GMPE params
		if (baseMapGMPE != null) {
			baseMapGMPE.setParamDefaults();
			HardCodedInterpDiffMapCreator.setTruncation(baseMapGMPE, 3.0);
		}
		
		File studyDir = new File(mainOutputDir, study.getDirName());
		Preconditions.checkState(studyDir.exists() || studyDir.mkdir());
		
		File mapDir = new File(studyDir, "hazard_maps");
		Preconditions.checkState(mapDir.exists() || mapDir.mkdir());
		
		File resourcesDir = new File(mapDir, "resources");
		Preconditions.checkState(resourcesDir.exists() || resourcesDir.mkdir());
		
		List<String> lines = new ArrayList<>();
		lines.add("# "+study.getName()+" Hazard Maps");
		lines.add("");
		if (baseMapGMPE != null)
			lines.add("**Basemap GMPE: "+baseMapGMPE.getName());
		lines.add("");
		lines.add("**Study Details**");
		lines.add("");
		lines.addAll(study.getMarkdownMetadataTable());
		lines.add("");
		
		int tocIndex = lines.size();
		String topLink = "*[(top)](#table-of-contents)*";
		
		Region region = study.getRegion();
		double baseMapRes = 0.005;
		InterpDiffMapType[] mapTypes;
		if (baseMapGMPE == null)
			mapTypes = new InterpDiffMapType [] { InterpDiffMapType.INTERP_NOMARKS, InterpDiffMapType.INTERP_MARKS};
		else
			mapTypes = new InterpDiffMapType [] { InterpDiffMapType.INTERP_NOMARKS, InterpDiffMapType.INTERP_MARKS,
					InterpDiffMapType.BASEMAP, InterpDiffMapType.DIFF, InterpDiffMapType.RATIO};
		
		HardCodedInterpDiffMapCreator.cs_db = study.getDB();
		HardCodedInterpDiffMapCreator.gmpe_db = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
		
		List<Integer> datasetIDs = Ints.asList(study.getDatasetIDs());
		
		try {
			for (double period : periods) {
				for (CyberShakeComponent component : components) {
					CybershakeIM im = CybershakeIM.getSA(component, period);
					
					String periodLabel = optionalDigitDF.format(period)+"sec SA, "+component.getShortName();
					String periodFileLabel = optionalDigitDF.format(period)+"s_"+component.getShortName();
					
					if (probLevels.size() > 1)
						lines.add("## "+periodLabel);
					
					System.out.println("Doing "+periodLabel);
					
					for (int i=0; i<probLevels.size(); i++) {
						double probLevel = probLevels.get(i);
						String probLabel = probLabels.get(i);
						String probFileLabel = probFileLables.get(i);
						
						String title = periodLabel+", "+probLabel;
						
						String prefix = "map_"+periodFileLabel+"_"+probFileLabel;
						
						List<InterpDiffMapType> typesToPlot = new ArrayList<>();
						for (InterpDiffMapType type : mapTypes) {
							File pngFile = new File(resourcesDir, prefix+"_"+type.getPrefix()+".png");
							if (replot || !pngFile.exists())
								typesToPlot.add(type);
						}
						
						if (!typesToPlot.isEmpty()) {
							System.out.println("Plotting "+typesToPlot.size()+" maps");
							
							GeoDataSet baseMap = null;
							if (baseMapGMPE != null) {
								// load the basemap
								System.out.println("Loading basemap");
								baseMap = HardCodedInterpDiffMapCreator.loadBaseMap(
										baseMapGMPE, isProbAt_IML, probLevel, study.getVelocityModelID(),im.getID(), region);
							}
							GeoDataSet scatterData = HardCodedInterpDiffMapCreator.getMainScatter(
									isProbAt_IML, probLevel, datasetIDs, im.getID(), null);
							
							System.out.println("Creating map instance...");
							GMT_InterpolationSettings interpSettings = GMT_InterpolationSettings.getDefaultSettings();
							
							double cptMax;
							if (period >= 10d)
								cptMax = 0.4d;
							else if (period >= 5d)
								cptMax = 0.6d;
							else
								cptMax = 1d;
							
							CPT cpt = hazardCPT.rescale(0d, cptMax);
							
							InterpDiffMap map = new InterpDiffMap(region, baseMap, baseMapRes, cpt, scatterData,
									interpSettings, typesToPlot.toArray(new InterpDiffMapType[0]));
							map.setCustomLabel(title);
							map.setTopoResolution(TopographicSlopeFile.CA_THREE);
							map.setLogPlot(logPlot);
							map.setDpi(300);
							map.setXyzFileName("base_map.xyz");
							map.setCustomScaleMin(0d);
							map.setCustomScaleMax(cptMax);
							
							String metadata = "isProbAt_IML: " + isProbAt_IML + "\n" +
											"val: " + probLevel + "\n" +
											"imTypeID: " + im.getID() + "\n";
							
							System.out.println("Making map...");
							String addr;
							if (LOCAL_MAPGEN)
								addr = HardCodedInterpDiffMapCreator.plotLocally(map);
							else
								addr = CS_InterpDiffMapServletAccessor.makeMap(null, map, metadata);
							
							System.out.println("Done, downloading");
							
							for (InterpDiffMapType type : typesToPlot) {
								File pngFile = new File(resourcesDir, prefix+"_"+type.getPrefix()+".png");
								File psFile = new File(resourcesDir, prefix+"_"+type.getPrefix()+".ps");
								if (LOCAL_MAPGEN) {
									File inFile = new File(addr, pngFile.getName());
									Preconditions.checkState(inFile.exists(), "In file doesn't exist: %s", inFile.getAbsolutePath());
									Files.copy(inFile, pngFile);
									inFile = new File(addr, psFile.getName());
									Preconditions.checkState(inFile.exists(), "In file doesn't exist: %s", inFile.getAbsolutePath());
									Files.copy(inFile, psFile);
								} else {
									if (!addr.endsWith("/"))
										addr += "/";
									FileUtils.downloadURL(addr+type.getPrefix()+".150.png", pngFile);
									FileUtils.downloadURL(addr+type.getPrefix()+".ps", psFile);
								}
							}
						}
						
						String headings = "##";
						if (probLevels.size() > 1)
							headings += "#";
						
						lines.add(headings+" "+title);
						lines.add(topLink); lines.add("");
						
						TableBuilder table = MarkdownUtils.tableBuilder();
						table.addLine("**Map Type**", "**Map**");
						for (InterpDiffMapType type : mapTypes) {
							File pngFile = new File(resourcesDir, prefix+"_"+type.getPrefix()+".png");
							Preconditions.checkState(pngFile.exists(), "Map doesn't exist: %s", pngFile.getAbsolutePath());
							table.addLine("**"+type.getName()+"**", "!["+type.getName()+"](resources/"+pngFile.getName()+")");
						}
						lines.addAll(table.build());
						lines.add("");
					}
				}
			}
			
			// add TOC
			lines.addAll(tocIndex, MarkdownUtils.buildTOC(lines, 2, 4));
			lines.add(tocIndex, "## Table Of Contents");

			// write markdown
			MarkdownUtils.writeReadmeAndHTML(lines, studyDir);
			
			System.out.println("Done, writing summary");
			study.writeMarkdownSummary(studyDir);
			System.out.println("Writing studies index");
			CyberShakeStudy.writeStudiesIndex(mainOutputDir);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			study.getDB().destroy();
			if (study.getDB() != HardCodedInterpDiffMapCreator.gmpe_db)
				HardCodedInterpDiffMapCreator.gmpe_db.destroy();
			System.exit(0);
		}
	}
	
	static final DecimalFormat optionalDigitDF = new DecimalFormat("0.##");

}
