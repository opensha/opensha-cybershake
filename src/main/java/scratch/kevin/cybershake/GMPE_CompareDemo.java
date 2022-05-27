package scratch.kevin.cybershake;

import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder;
import org.opensha.sha.cybershake.CyberShakeSiteBuilder.Vs30_Source;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.imr.AttenRelRef;
import org.opensha.sha.imr.ScalarIMR;
import org.opensha.sha.imr.param.IntensityMeasureParams.SA_Param;

import com.google.common.base.Preconditions;

public class GMPE_CompareDemo {
	
	public static void main(String[] args) throws IOException {
		CyberShakeStudy study = CyberShakeStudy.STUDY_15_4;
		
		File outputDir = new File("/tmp/cs_gmpe_compare");
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		// setup ERF from CS study
		AbstractERF erf = study.getERF();
		erf.updateForecast();
		
		// init GMPE
		ScalarIMR gmpe = AttenRelRef.ASK_2014.instance(null);
		gmpe.setParamDefaults();
		
		// set to SA
		gmpe.setIntensityMeasure(SA_Param.NAME);
		double[] periods = {2d, 3d, 5d, 10d};
		
		// get cybershake run IDs for study
		List<CybershakeRun> runs = study.runFetcher().fetch();
		
		// get site list, this also sets Vs30 and basin depth values in each site for use with the GMPEs
		List<Site> sites = CyberShakeSiteBuilder.buildSites(study, Vs30_Source.Simulation, runs);
		
		List<String> header = new ArrayList<>();
		header.add("Source ID");
		header.add("Rupture ID");
		DecimalFormat df = new DecimalFormat("0.#");
		for (double period : periods) {
			header.add(df.format(period)+"s Log Mean");
			header.add(df.format(period)+"s Std Dev");
		}
		
		for (Site site : sites) {
			gmpe.setSite(site);
			
			CSVFile<String> csv = new CSVFile<>(true);
			csv.addLine(header);
			
			for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
				ProbEqkSource source = erf.getSource(sourceID);
				if (source.getMinDistance(site) > 200d)
					// skip sources more than 200km away
					continue;
				
				for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
					ProbEqkRupture rup = source.getRupture(rupID);
					gmpe.setEqkRupture(rup);
					
					List<String> line = new ArrayList<>();
					line.add(sourceID+"");
					line.add(rupID+"");
					
					for (double period : periods) {
						// set period in GMPE
						SA_Param.setPeriodInSA_Param(gmpe.getIntensityMeasure(), period);
						
						line.add(gmpe.getMean()+"");
						line.add(gmpe.getStdDev()+"");
					}
					csv.addLine(line);
				}
			}
			csv.writeToFile(new File(outputDir, site.getName()+".csv"));
		}
	}

}
