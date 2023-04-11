package scratch.kevin.cybershake;

import java.awt.Color;
import java.io.File;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.jfree.data.Range;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotUtils;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;

import com.google.common.base.Preconditions;

import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.HazardCurve2DB;

public class CSSiteCurveCompare {

	public static void main(String[] args) throws IOException {
		CyberShakeStudy mainStudy = CyberShakeStudy.STUDY_22_12_HF;
		CyberShakeStudy compStudy = CyberShakeStudy.STUDY_15_12;
		
		File outputDir = new File("/tmp/cs_curves");
		Preconditions.checkState(outputDir.exists() || outputDir.mkdir());
		
		String siteName = "s155";
		
		double[] periods = { 0.1, 0.5, 3d };
//		double[] periods = { 0.1 };
		CyberShakeComponent component = CyberShakeComponent.RotD50;
		
		Range yRange = new Range(1e-6, 1);
		
		boolean logX = false;
		boolean logY = true;
		
		CybershakeRun mainRun = mainStudy.runFetcher().forSiteNames(siteName).fetch().get(0);
		CybershakeRun compRun = compStudy.runFetcher().forSiteNames(siteName).fetch().get(0);
		
		HazardCurve2DB mainCurves2DB = new HazardCurve2DB(mainStudy.getDB());
		HazardCurve2DB compCurves2DB = new HazardCurve2DB(compStudy.getDB());
		
		DBAccess.PRINT_ALL_QUERIES = true;
		
		DecimalFormat oDF = new DecimalFormat("0.##");
		
		for (double period : periods) {
			CybershakeIM im = CybershakeIM.getSA(component, period);
			
			int mainCurveID = mainCurves2DB.getHazardCurveID(mainRun.getRunID(), im.getID());
			DiscretizedFunc mainCurve = mainCurves2DB.getHazardCurve(mainCurveID);
			
			int compCurveID = compCurves2DB.getHazardCurveID(compRun.getRunID(), im.getID());
			DiscretizedFunc compCurve = compCurves2DB.getHazardCurve(compCurveID);
			
			List<DiscretizedFunc> funcs = new ArrayList<>();
			List<PlotCurveCharacterstics> chars = new ArrayList<>();
			
			mainCurve.setName(mainStudy.getName());
			funcs.add(mainCurve);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.RED));
			
			compCurve.setName(compStudy.getName());
			funcs.add(compCurve);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 3f, Color.BLUE));
			
			PlotSpec spec = new PlotSpec(funcs, chars, siteName+" Comparison",
					oDF.format(period)+"s SA", "Annual Probability of Exceedance");
			spec.setLegendInset(true);
			
			Range xRange;
			if (logX) {
				if (period < 1d)
					xRange = new Range(1e-2, 2d);
				else
					xRange = new Range(1e-3, 2d);
			} else {
				if (period < 1d)
					xRange = new Range(0, 3d);
				else if (period < 5d)
					xRange = new Range(0, 2d);
				else
					xRange = new Range(0, 1d);
			}
			
			HeadlessGraphPanel gp = PlotUtils.initHeadless();
			
			gp.drawGraphPanel(spec, logX, logY, xRange, yRange);
			
			String prefix = siteName+"_"+oDF.format(period)+"s";
			
			PlotUtils.writePlots(outputDir, prefix, gp, 800, 750, true, true, true);
		}
		
		mainStudy.getDB().destroy();
		compStudy.getDB().destroy();
	}

}
