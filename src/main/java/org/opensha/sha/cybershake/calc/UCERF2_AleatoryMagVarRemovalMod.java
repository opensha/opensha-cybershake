package org.opensha.sha.cybershake.calc;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;

import org.opensha.commons.calc.magScalingRelations.MagAreaRelationship;
import org.opensha.commons.calc.magScalingRelations.magScalingRelImpl.HanksBakun2002_MagAreaRel;
import org.opensha.commons.calc.magScalingRelations.magScalingRelImpl.Somerville_2006_MagAreaRel;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.TimeSpan;
import org.opensha.commons.eq.MagUtils;
import org.opensha.commons.util.ClassUtils;
import org.opensha.sha.cybershake.db.MeanUCERF2_ToDB;
import org.opensha.sha.earthquake.ERF;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.earthquake.rupForecastImpl.Frankel02.Frankel02_TypeB_EqkSource;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.UnsegmentedSource;
import org.opensha.sha.earthquake.rupForecastImpl.WGCEP_UCERF_2_Final.MeanUCERF2.MeanUCERF2;
import org.opensha.sha.faultSurface.RuptureSurface;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import scratch.UCERF3.enumTreeBranches.ScalingRelationships;

public class UCERF2_AleatoryMagVarRemovalMod implements RuptureProbabilityModifier {
	
	private ERF erf;
	// for counting/debugging
	private HashSet<Integer> aleatorySources = new HashSet<Integer>();
	
	public UCERF2_AleatoryMagVarRemovalMod(ERF erf) {
		this.erf = erf;
	}

	@Override
	public double getModifiedProb(int sourceID, int rupID, double origProb) {
		ProbEqkSource source = erf.getSource(sourceID);
//		if (rupID == 0)
//			System.out.println(source.getName()+"\t"+isAleatory(source)+"\t"+source.getClass());
//		if (sourceID == 247) {
//			System.out.println("Raymond type: "+source.getClass());
//			System.exit(0);
//		}
		if (!aleatorySources.contains(sourceID) && !isAleatory(source))
			return origProb;
		aleatorySources.add(sourceID);
		int startRupID;
		if (source instanceof UnsegmentedSource) {
			startRupID = ((UnsegmentedSource)source).getTotNumGR_Rups();
			// sanity check
			double mag = source.getRupture(startRupID).getMag();
			for (int i=startRupID+1; i<source.getNumRuptures(); i++) {
				double newMag = source.getRupture(i).getMag();
				Preconditions.checkState(newMag > mag);
				mag = newMag;
			}
		} else {
			startRupID = 0;
		}
		if (rupID < startRupID)
			return origProb;
		// find the mode of the distribution
		// dist is moment rate balance, so look for max mo rate
		double totMoRate = 0d;
		double maxMoRate = 0d;
		int maxMoRateID = -1;
		
		double erfDuration = erf.getTimeSpan().getDuration(TimeSpan.YEARS);
		
		for (int testRupID=startRupID; testRupID<source.getNumRuptures(); testRupID++) {
			ProbEqkRupture rup = source.getRupture(testRupID);
			double rate = rup.getMeanAnnualRate(erfDuration);
			double moment = MagUtils.magToMoment(rup.getMag());
			double moRate = moment*rate;
			totMoRate += moRate;
			if (moRate >= maxMoRate) {
				maxMoRate = moRate;
				maxMoRateID = testRupID;
			}
		}
		// make probability zero if not the most probable rupture
		if (maxMoRateID != rupID)
			return 0;
		// return the total source probability if it is
		double rupMoment = MagUtils.magToMoment(source.getRupture(maxMoRateID).getMag());
		double newRate = totMoRate / rupMoment;
		double newProb = 1d - Math.exp(-newRate*erfDuration);
		Preconditions.checkState(newProb > origProb, "Didn't increase...? %s <= %s. ID=%s, name=%s, class=%s",
				newProb, origProb, sourceID, source.getName(),
				ClassUtils.getClassNameWithoutPackage(source.getClass()));
		
		return newProb;
	}
	
	private boolean isAleatory(ProbEqkSource source) {
		// special case for B Faults
		if (source instanceof UnsegmentedSource)
			return ((UnsegmentedSource)source).getTotNumChar_Rups() > 1;
		// special case for non-CA B Faults
		if (source instanceof Frankel02_TypeB_EqkSource)
			return false;
		// check to make sure the area of each rupture is the same
		// otherwise it's a floating source and we don't want to modify it
		double area = -1d;
		for (ProbEqkRupture rup : source) {
			double newArea = rup.getRuptureSurface().getArea();
			if (area < 0)
				area = newArea;
			else if ((float)area != (float)newArea)
				return false;
		}
		
		return true;
	}
	
	private static void writeDiagnostics(File plotDir, ERF erf, UCERF2_AleatoryMagVarRemovalMod mod) throws IOException {
		CSVFile<String> csv = new CSVFile<String>(false);
		CSVFile<String> summaryCSV = new CSVFile<String>(true);
		summaryCSV.addLine("Source ID", "Source Name", "Orig Max Mag", "Mod Max Mag");
		
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			ProbEqkSource source = erf.getSource(sourceID);
			boolean aleatory = mod.isAleatory(source);
			csv.addLine("Source:", sourceID+"", "Aleatory:", aleatory+"",
					"Class:", ClassUtils.getClassNameWithoutPackage(source.getClass()),
					"Name:", source.getName());
			if (aleatory) {
				csv.addLine("", "Source ID", "Rup ID", "Mag", "Orig Prob", "Mod Prob");
				double sumOrig = 0d;
				double sumMod = 0d;
				for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
					ProbEqkRupture rup = source.getRupture(rupID);
					double origProb = rup.getProbability();
					sumOrig += origProb;
					double modProb = mod.getModifiedProb(sourceID, rupID, origProb);
					sumMod += modProb;
					double mag = rup.getMag();
					
					csv.addLine("", sourceID+"", rupID+"", mag+"", origProb+"", modProb+"");
				}
				csv.addLine("");
				csv.addLine("", "", "", "SUM:", sumOrig+"", sumMod+"");
			}
			double origMaxMag = 0d;
			double modMaxMag = 0d;
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				ProbEqkRupture rup = source.getRupture(rupID);
				origMaxMag = Math.max(origMaxMag, rup.getMag());
				if (mod.getModifiedProb(sourceID, rupID, rup.getProbability()) > 0)
					modMaxMag = Math.max(modMaxMag, rup.getMag());
			}
			summaryCSV.addLine(sourceID+"", source.getName(), origMaxMag+"", modMaxMag+"");
			csv.addLine("");
		}
		csv.writeToFile(new File(plotDir, "diagnostics.csv"));
		summaryCSV.writeToFile(new File(plotDir, "diagnostics_summary.csv"));
	}
	
	private static void writeMagAreaDiagnostics(File outputFile, ERF erf, UCERF2_AleatoryMagVarRemovalMod mod) throws IOException {
		CSVFile<String> csv = new CSVFile<String>(true);
		
		ScalingRelationships[] scalars =  { ScalingRelationships.AVE_UCERF2, ScalingRelationships.MEAN_UCERF3,
				ScalingRelationships.ELLSWORTH_B, ScalingRelationships.HANKS_BAKUN_08, ScalingRelationships.SHAW_2009_MOD };
		MagAreaRelationship[] extraMAs = { new HanksBakun2002_MagAreaRel(), new Somerville_2006_MagAreaRel() };
		
		List<String> header = Lists.newArrayList("Source ID", "Rupture ID", "Type", "Area (km^2)",
				"Length (km)", "DDW (km)", "UCERF2 Mag");
		
		for (ScalingRelationships scalar : scalars)
			header.add(scalar.getShortName());
		for (MagAreaRelationship ma : extraMAs)
			header.add(ma.getName());
		
		csv.addLine(header);
		
		for (int sourceID=0; sourceID<erf.getNumSources(); sourceID++) {
			ProbEqkSource source = erf.getSource(sourceID);
			boolean aleatory = mod.isAleatory(source);
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				ProbEqkRupture rup = source.getRupture(rupID);
				double origProb = rup.getProbability();
				double modProb = mod.getModifiedProb(sourceID, rupID, origProb);
				double mag = rup.getMag();
				RuptureSurface surf = rup.getRuptureSurface();
				double area = surf.getArea(); // convert to M^2
				double len = surf.getAveLength(); // convert to M
				double ddw = surf.getAveWidth();
				
				String type;
				if (aleatory) {
					if (modProb == origProb)
						type = "REGULAR";
					else if (modProb == 0)
						type = "ALEATORY";
					else
						type = "MEDIAN";
				} else {
					type = "REGULAR";
				}
				
				List<String> line = Lists.newArrayList(sourceID+"", rupID+"", type, (float)area+"", (float)len+"", (float)ddw+"", (float)mag+"");
				
				for (ScalingRelationships scalar : scalars) {
					double scalarMag = scalar.getMag(area * 1000000, ddw * 1000, rup.getAveRake()); // in S-I units
					
					line.add((float)scalarMag+"");
				}
				for (MagAreaRelationship ma : extraMAs) {
					double scalarMag = ma.getMedianMag(area);
					
					line.add((float)scalarMag+"");
				}
				
				csv.addLine(line);
			}
		}
		
		csv.writeToFile(outputFile);
	}
	
	public static void main(String[] args) throws IOException {
		ERF erf = MeanUCERF2_ToDB.createUCERF2ERF();
		UCERF2_AleatoryMagVarRemovalMod mod = new UCERF2_AleatoryMagVarRemovalMod(erf);
		
		File plotDir = new File("/home/kevin/CyberShake/ucerf3/aleatory_test_ucerf2");
		
		writeDiagnostics(plotDir, erf, mod);
		writeMagAreaDiagnostics(new File(plotDir, "mag_area_diagnostics.csv"), erf, mod);
		erf.setParameter(MeanUCERF2.CYBERSHAKE_DDW_CORR_PARAM_NAME, false);
		erf.updateForecast();
		writeMagAreaDiagnostics(new File(plotDir, "mag_area_diagnostics_origddw.csv"), erf, mod);
	}
}
