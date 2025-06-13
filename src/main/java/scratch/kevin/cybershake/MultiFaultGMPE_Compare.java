package scratch.kevin.cybershake;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.sha.earthquake.EqkRupture;
import org.opensha.sha.faultSurface.CompoundSurface;
import org.opensha.sha.faultSurface.RuptureSurface;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;

import scratch.kevin.bbp.BBP_Site;
import scratch.kevin.bbp.BBP_SourceFile;

public class MultiFaultGMPE_Compare {

	public static void main(String[] args) throws IOException {
		ScalarIMR[] gmpes = {
				AttenRelRef.ASK_2014.get(),
				AttenRelRef.BSSA_2014.get(),
				AttenRelRef.CB_2014.get(),
				AttenRelRef.CY_2014.get()
		};
//		File dir = new File("/home/kevin/CyberShake/multifault_gmpe_calcs/landers");
//		List<BBP_SourceFile> segments = List.of(
//				BBP_SourceFile.readFile(new File(dir, "landers_v16_08_1_seg1.src")),
//				BBP_SourceFile.readFile(new File(dir, "landers_v16_08_1_seg2.src")),
//				BBP_SourceFile.readFile(new File(dir, "landers_v16_08_1_seg3.src")));
//		List<BBP_Site> bbpSites = BBP_Site.readFile(new File(dir, "landers_v19_06_2.stl"));
//		double mag = 7.22;
		
		File dir = new File("/home/kevin/CyberShake/multifault_gmpe_calcs/ridgecrest");
		List<BBP_SourceFile> segments = List.of(
				BBP_SourceFile.readFile(new File(dir, "ridgecrest-c_v20_09_1_seg1.src")),
				BBP_SourceFile.readFile(new File(dir, "ridgecrest-c_v20_09_1_seg2.src")),
				BBP_SourceFile.readFile(new File(dir, "ridgecrest-c_v20_09_1_seg3.src")));
		List<BBP_Site> bbpSites = BBP_Site.readFile(new File(dir, "ridgecrest_c_v20_08_1.stl"));
		double mag = 7.08;
		
		
		double rake = segments.get(0).getFocalMechanism().getRake();
		
		List<RuptureSurface> segSurfaces = new ArrayList<>();
		for (BBP_SourceFile segment : segments)
			segSurfaces.add(segment.getSurface().getQuadSurface());
		
		CompoundSurface fullSurf = new CompoundSurface(segSurfaces);
		EqkRupture rup = new EqkRupture(mag, rake, fullSurf, null);
		
		for (ScalarIMR gmpe : gmpes) {
			gmpe.setIntensityMeasure(SA_Param.NAME);
			gmpe.setEqkRupture(rup);
		}
		double[] periods = {0.01, 0.02, 0.05, 0.075, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0, 4.0, 5.0, 7.5, 10.0};
		
		List<Site> sites = new ArrayList<>();
		for (BBP_Site site : bbpSites)
			sites.add(site.buildGMPE_Site(null));
		
		for (Site site : sites)
			System.out.println(site.getName()+"\t"+site.getLocation()+"\n"+site.getParameterListMetadataString());
		
		for (double period : periods) {
			for (ScalarIMR gmpe : gmpes)
				SA_Param.setPeriodInSA_Param(gmpe.getIntensityMeasure(), period);
			File outputFile = new File(dir, "gmpe_results_"+(float)period+"s.csv");
			CSVFile<String> csv = new CSVFile<>(true);
			List<String> header = new ArrayList<>();
			header.add("Site Name");
			header.add("Latitude");
			header.add("Longitude");
			for (ScalarIMR gmpe : gmpes) {
				header.add(gmpe.getShortName()+" ln mean");
				header.add(gmpe.getShortName()+" sigma");
			}
			csv.addLine(header);
			for (Site site : sites) {
				List<String> line = new ArrayList<>(csv.getNumCols());
				line.add(site.getName());
				line.add((float)site.getLocation().lat+"");
				line.add((float)site.getLocation().lon+"");
				for (ScalarIMR gmpe : gmpes) {
					gmpe.setSite(site);
					line.add((float)gmpe.getMean()+"");
					line.add((float)gmpe.getStdDev()+"");
				}
				csv.addLine(line);
			}
			
			csv.writeToFile(outputFile);
		}
	}

}
