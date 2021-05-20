package org.opensha.sha.cybershake.calc.mcer;

import java.io.File;
import java.io.IOException;

import org.opensha.sha.calc.mcer.CachedCurveBasedMCErProbabilisticCalc;
import org.opensha.sha.calc.mcer.CachedMCErDeterministicCalc;

public class GMPE_CacheCombine {

	public static void main(String[] args) throws IOException {
		File mainDir = new File("/home/kevin/CyberShake/MCER/gmpe_cache_gen/2015_09_29-ucerf3_full_ngaw2");
		File runningDir = new File(mainDir, "running_results");
		
		for (File file : mainDir.listFiles()) {
			String name = file.getName();
			if (!name.endsWith("_deterministic.xml"))
				continue;
			
			System.out.println("Working on to "+file.getAbsolutePath());
			
			String prefix = name.substring(0, name.indexOf("_deterministic"));
			
			CachedMCErDeterministicCalc calc = new CachedMCErDeterministicCalc(null, file);
			
			for (File subFile : runningDir.listFiles()) {
				String subName = subFile.getName();
				if (!subName.contains(prefix) || !subName.endsWith("_deterministic.xml"))
					continue;
				System.out.println("Adding from "+subFile.getAbsolutePath());
				CachedMCErDeterministicCalc subCalc = new CachedMCErDeterministicCalc(null, subFile);
				calc.addToCache(subCalc);
			}
			
			System.out.println("Flushing to "+file.getAbsolutePath());
			calc.flushCache();
			
			// now probabilistic 
			file = new File(mainDir, prefix+"_probabilistic_curve.xml");
			
			System.out.println("Working on to "+file.getAbsolutePath());
			
			CachedCurveBasedMCErProbabilisticCalc probCalc = new CachedCurveBasedMCErProbabilisticCalc(null, file);
			
			for (File subFile : runningDir.listFiles()) {
				String subName = subFile.getName();
				if (!subName.contains(prefix) || !subName.endsWith("_probabilistic_curve.xml"))
					continue;
				System.out.println("Adding from "+subFile.getAbsolutePath());
				CachedCurveBasedMCErProbabilisticCalc subCalc = new CachedCurveBasedMCErProbabilisticCalc(null, subFile);
				probCalc.addToCache(subCalc);
			}
			
			System.out.println("Flushing to "+file.getAbsolutePath());
			probCalc.flushCache();
		}
	}

}
