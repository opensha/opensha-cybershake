package scratch.kevin.cybershake.simCompare;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.siteData.impl.WillsMap2015;
import org.opensha.commons.geo.Location;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.calc.mcer.CyberShakeSiteRun;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.imr.param.SiteParams.DepthTo1pt0kmPerSecParam;
import org.opensha.sha.imr.param.SiteParams.DepthTo2pt5kmPerSecParam;
import org.opensha.sha.imr.param.SiteParams.Vs30_Param;

import com.google.common.base.Joiner;
import com.google.common.collect.Range;

public class SiteSearchByVs30 {
	
	public static void main(String[] args) throws IOException {
//		Range<Double> vs30Range = Range.closed(500d, 500d);
//		Range<Double> willsRange = Range.closed(180d, 500d);
//		String outPrefix = "cs_vs30_sites_500";
		Range<Double> vs30Range = Range.closed(800d, 1000d);
		Range<Double> willsRange = Range.closed(600d, 1200d);
		String outPrefix = "cs_vs30_sites_800";
		
		File outputDir = new File("/home/kevin/CyberShake/sites/site_search");
		
		// study from which we are selecting sites
		CyberShakeStudy siteStudy = CyberShakeStudy.STUDY_15_4;
		
		List<CyberShakeSiteRun> sites = CyberShakeSiteBuilder.buildSites(siteStudy, Vs30_Source.Simulation, siteStudy.runFetcher().fetch());
		List<CyberShakeSiteRun> matches = new ArrayList<>();
		List<String> bbStations = new ArrayList<>();
		
		WillsMap2015 wills = new WillsMap2015();
		
		for (CyberShakeSiteRun site : sites) {
			double vs30 = site.getParameter(Double.class, Vs30_Param.NAME).getValue();
			double willsVal = wills.getValue(site.getLocation());
			if (vs30Range.contains(vs30) && (willsRange == null || willsRange.contains(willsVal))) {
				System.out.println("Site: "+site.getName()+"\tVs30: "+vs30+"\tWills: "+willsVal);
				matches.add(site);
				if (site.getCS_Site().type_id == CybershakeSite.TYPE_BROADBAND_STATION)
					bbStations.add(site.getName());
			}
		}
		
		System.out.println("Found "+matches.size()+" matching sites");
		System.out.println("BroadBand stations: "+Joiner.on(",").join(bbStations));
		
		siteStudy.getDB().destroy();
		
		File outputFile = new File(outputDir, outPrefix+".kml");
		System.out.println("Writing KML to "+outputFile.getAbsolutePath());
		
		File csvOutFile = new File(outputDir, outPrefix+"csv");
		CSVFile<String> csv = new CSVFile<>(true);
		csv.addLine("Name", "CS Vs30 (m/s)", "Wills Vs30 (m/s)", "Z1.0 (m)", "Z2.5 (km)", "Latitude", "Longitude");
		
		FileWriter fw = new FileWriter(outputFile);
		
		fw.write("<?xml version=\"1.0\" encoding=\"UTF-8\"?>" + "\n");
		fw.write("<kml xmlns=\"http://earth.google.com/kml/2.2\">" + "\n");
		fw.write("  <Folder>" + "\n");
		fw.write("    <name>CyberShake sites with Vs30=["+vs30Range.lowerEndpoint().floatValue()+" "
				+vs30Range.upperEndpoint().floatValue()+"]</name>" + "\n");
//		fw.write("    <description>Open Seismic Hazard Analysis Evenly Gridded Region</description>" + "\n");
		
		for (Site site : matches) {
			Location loc = site.getLocation();
			
			fw.write("    <Placemark>" + "\n");
			fw.write("      <name>"+site.getName()+"</name>" + "\n");
			fw.write("      <Point id=\""+site.getName()+"\">" + "\n");
			fw.write("        <coordinates>" + loc.getLongitude() + "," + loc.getLatitude() + "," + (-loc.getDepth()) + "</coordinates>" + "\n");
//			fw.write("        <latitude>" + loc.getLatitude() + "</latitude>" + "\n");
//			fw.write("        <altitude>" + (-loc.getDepth()) + "</altitude>" + "\n");
			fw.write("      </Point>" + "\n");
			
			fw.write("    </Placemark>" + "\n");

			double vs30 = site.getParameter(Double.class, Vs30_Param.NAME).getValue();
			double willsVs30 = wills.getValue(loc);
			double z10 = site.getParameter(Double.class, DepthTo1pt0kmPerSecParam.NAME).getValue();
			double z25 = site.getParameter(Double.class, DepthTo2pt5kmPerSecParam.NAME).getValue();
			
			csv.addLine(site.getName(), (float)vs30+"", (float)willsVs30+"", (float)z10+"", (float)z25+"",
					(float)loc.getLatitude()+"", (float)loc.getLongitude()+"");
		}
		
		csv.writeToFile(csvOutFile);
		
		fw.write("  </Folder>" + "\n");
		fw.write("</kml>" + "\n");
		fw.flush();
		fw.close();
	}

}
