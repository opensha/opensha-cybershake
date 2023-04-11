package scratch.kevin.cybershake;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.opensha.commons.data.xyz.GeoDataSet;
import org.opensha.commons.exceptions.GMT_MapException;
import org.opensha.commons.geo.Region;
import org.opensha.commons.mapping.gmt.elements.TopographicSlopeFile;
import org.opensha.commons.util.FileUtils;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.cybershake.HazardCurveFetcher;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.AttenRelCurves2DB;
import org.opensha.sha.cybershake.db.AttenRelDataSets2DB;
import org.opensha.sha.cybershake.db.AttenRels2DB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.maps.GMT_InterpolationSettings;
import org.opensha.sha.cybershake.maps.HardCodedInterpDiffMapCreator;
import org.opensha.sha.cybershake.maps.InterpDiffMap;
import org.opensha.sha.cybershake.maps.InterpDiffMap.InterpDiffMapType;
import org.opensha.sha.cybershake.maps.servlet.CS_InterpDiffMapServletAccessor;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.param.OtherParams.Component;
import org.opensha.sha.util.component.ComponentConverter;
import org.opensha.sha.util.component.ComponentTranslation;

import com.google.common.base.Preconditions;
import com.google.common.io.Files;

public class TurkeyCompareMapGen {
	
	private static boolean LOCAL_MAPGEN = true;
	private static DecimalFormat oDF = new DecimalFormat("0.##");
	private static DecimalFormat pDF = new DecimalFormat("0.##%");

	public static void main(String[] args) throws SQLException, IOException, GMT_MapException, ClassNotFoundException {
		CyberShakeStudy study = CyberShakeStudy.STUDY_22_12_HF;
		
		File outputDir = new File("/home/kevin/CyberShake/2023_turkey_comps/maps");
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		CybershakeIM ims[] = {
//				CybershakeIM.getSA(CyberShakeComponent.RotD100, 0.01),
				CybershakeIM.getSA(CyberShakeComponent.RotD100, 3d)
		};
		
		CybershakeIM gmIMs[] = {
//				CybershakeIM.getSA(CyberShakeComponent.RotD50, 0.01),
				CybershakeIM.getSA(CyberShakeComponent.RotD50, 3d)
		};
		ComponentTranslation trans = ComponentConverter.getConverter(Component.RotD50, Component.RotD100);
		
		double[] imRefGMs = {
//				1.616981,
//				0.820891
				0.4
		};
		
		double duration = 10d;
		
		Preconditions.checkState(imRefGMs.length == ims.length);
		
		ScalarIMR baseMapGMPE = AttenRelRef.NGAWest_2014_AVG_NOIDRISS.instance(null);
		
		baseMapGMPE.setParamDefaults();
		HardCodedInterpDiffMapCreator.setTruncation(baseMapGMPE, 3.0);
		
		InterpDiffMapType[] mapTypes = { InterpDiffMapType.INTERP_NOMARKS, InterpDiffMapType.INTERP_MARKS,
				InterpDiffMapType.BASEMAP, InterpDiffMapType.DIFF, InterpDiffMapType.RATIO};
		
		HardCodedInterpDiffMapCreator.cs_db = study.getDB();
		HardCodedInterpDiffMapCreator.gmpe_db = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
		
		int[] dsIDs = study.getDatasetIDs();
		List<CybershakeRun> runs = study.runFetcher().fetch();
		
		Region region = study.getRegion();
		double baseMapRes = 0.005;
		
		CPT hazardCPT = CPT.loadFromStream(HardCodedInterpDiffMapCreator.class.getResourceAsStream(
				"/org/opensha/sha/cybershake/conf/cpt/cptFile_hazard_input.cpt"));
		
		for (int i=0; i<ims.length; i++) {
			CybershakeIM im = ims[i];
			double level = imRefGMs[i];
			boolean isProbAt_IML = true;
			
			CybershakeIM gmIM = gmIMs[i];
			GeoDataSet baseMap = loadConvertBaseMap(HardCodedInterpDiffMapCreator.gmpe_db, gmIM, trans, baseMapGMPE,
					isProbAt_IML, level, study.getERF_ID(), study.getVelocityModelID(), region, duration);
			
			HazardCurveFetcher fetch = new HazardCurveFetcher(study.getDB(), runs, dsIDs, im.getID());
			if (duration != 1d)
				fetch.scaleForDuration(1d, duration);
			GeoDataSet scatterData = HardCodedInterpDiffMapCreator.getMainScatter(
					isProbAt_IML, level, fetch, im.getID(), null);
			
			System.out.println("Creating map instance...");
			GMT_InterpolationSettings interpSettings = GMT_InterpolationSettings.getDefaultSettings();
			
//			double cptMin = 0d;
//			double cptMax = 0.01d;
//			boolean logPlot = false;
			double cptMin = -5;
			double cptMax = -2;
			boolean logPlot = true;
			
			if (logPlot) {
				double min = Math.pow(10, cptMin);
				for (int j=0; j<baseMap.size(); j++)
					if (!(baseMap.get(j) > min))
						baseMap.set(j, min);
				for (int j=0; j<scatterData.size(); j++)
					if (!(scatterData.get(j) > min))
						scatterData.set(j, min);
			}
			
			String title = oDF.format(im.getVal())+"s SA, ";
			String prefix = "map_"+oDF.format(im.getVal())+"s";
			if (isProbAt_IML) {
				title += oDF.format(duration)+" year POE "+oDF.format(level)+" (g)";
				prefix += "_poe_"+oDF.format(level)+"g";
			} else {
				title += pDF.format(level)+" POE in "+oDF.format(duration)+" years";
				prefix += "_"+oDF.format(duration*100d)+"_in_"+oDF.format(duration);
			}
			
			CPT cpt = hazardCPT.rescale(cptMin, cptMax);
			
			InterpDiffMap map = new InterpDiffMap(region, baseMap, baseMapRes, cpt, scatterData,
					interpSettings, mapTypes);
			map.setCustomLabel(title);
			map.setTopoResolution(TopographicSlopeFile.CA_THREE);
			map.setLogPlot(logPlot);
			map.setDpi(300);
			map.setXyzFileName("base_map.xyz");
			map.setCustomScaleMin(cptMin);
			map.setCustomScaleMax(cptMax);
			if (logPlot)
				map.setCPTCustomInterval(1d);
			
			String metadata = "isProbAt_IML: " + isProbAt_IML + "\n" +
							"val: " + level + "\n" +
							"imTypeID: " + im.getID() + "\n";
			
			System.out.println("Making map...");
			String addr;
			if (LOCAL_MAPGEN)
				addr = HardCodedInterpDiffMapCreator.plotLocally(map);
			else
				addr = CS_InterpDiffMapServletAccessor.makeMap(null, map, metadata);
			
			System.out.println("Done, downloading");
			
			for (InterpDiffMapType type : mapTypes) {
				File pngFile = new File(outputDir, prefix+"_"+type.getPrefix()+".png");
				File psFile = new File(outputDir, prefix+"_"+type.getPrefix()+".ps");
				if (LOCAL_MAPGEN) {
					File inFile = new File(addr, type.getPrefix()+".150.png");
					Preconditions.checkState(inFile.exists(), "In file doesn't exist: %s", inFile.getAbsolutePath());
					Files.copy(inFile, pngFile);
					inFile = new File(addr, type.getPrefix()+".ps");
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
		
		study.getDB().destroy();
	}
	
	private static GeoDataSet loadConvertBaseMap(DBAccess db, CybershakeIM gmIM, ComponentTranslation trans,
			ScalarIMR imr, boolean isProbAt_IML, double level, int erfID, int velModelID, Region reg, double duration)
					throws SQLException {
		AttenRels2DB ar2db = new AttenRels2DB(db);
		int attenRelID = ar2db.getAttenRelID(imr);
		
		AttenRelDataSets2DB ds2db = new AttenRelDataSets2DB(db);
		int datasetID = ds2db.getDataSetID(attenRelID, erfID, velModelID, 1, 1, null);
		
		AttenRelCurves2DB curves2db = new AttenRelCurves2DB(db);
		GeoDataSet xyz = curves2db.fetchMap(datasetID, gmIM.getID(), isProbAt_IML, level, true, reg, trans, duration);
		System.out.println("Got "+xyz.size()+" basemap values!");
		return xyz;
	}

}
