package scratch.kevin.cybershake.simCompare;

import java.awt.Color;
import java.awt.geom.Point2D;
import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.data.Site;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.geo.Location;
import org.opensha.commons.gui.plot.GraphWindow;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeIM.IMType;

import com.google.common.primitives.Doubles;

import scratch.kevin.cybershake.simCompare.StudyGMPE_Compare.Vs30_Source;
import scratch.kevin.simCompare.SimulationHazardCurveCalc;

public class OldVsNewCalcCheck {

	public static void main(String[] args) throws SQLException, IOException {
		File ampsCacheDir = new File("/data/kevin/cybershake/amps_cache/");
		
		CyberShakeStudy study = CyberShakeStudy.STUDY_18_4_RSQSIM_PROTOTYPE_2457;
		Vs30_Source vs30Source = Vs30_Source.Simulation;
		CyberShakeStudy[] compStudies = { CyberShakeStudy.STUDY_15_4 };
		
		String siteName = "USC";
		
		double[] periods = { 3, 5, 7.5, 10 };
		CybershakeIM[] rd50_ims = new PeakAmplitudesFromDB(study.getDB()).getIMs(Doubles.asList(periods),
				IMType.SA, CyberShakeComponent.RotD50).toArray(new CybershakeIM[0]);
		
		Site site = new Site(new Location(34.0192, -118.286));
		
		StudyRotDProvider prov = StudySiteHazardCurvePageGen.getSimProv(study, siteName, ampsCacheDir, periods, rd50_ims, site);
		SimulationHazardCurveCalc<CSRupture> newCalc = new SimulationHazardCurveCalc<>(prov);
		
		int runID = prov.getRunIDsMap().values().iterator().next();
		
		for (int p=0; p<periods.length; p++) {
			double period = periods[p];
			DiscretizedFunc newCurve = newCalc.calc(site, period, 1d);
			
			HazardCurveComputation oldCalc = new HazardCurveComputation(study.getDB());
			ArrayList<Double> imlVals = new ArrayList<>();
			for (Point2D pt : newCurve)
				imlVals.add(pt.getX());
			DiscretizedFunc oldCurve = oldCalc.computeHazardCurve(imlVals, runID, rd50_ims[p]);
			
			List<DiscretizedFunc> funcs = new ArrayList<>();
			List<PlotCurveCharacterstics> chars = new ArrayList<>();
			
			funcs.add(newCurve);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, Color.BLACK));
			
			funcs.add(oldCurve);
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, Color.BLUE));
			
			GraphWindow gw = new GraphWindow(funcs, "Curve Comparison", chars);
			gw.setXLog(true);
			gw.setYLog(true);
			gw.setX_AxisRange(1e-3, 1e1);
			gw.setY_AxisRange(1e-8, 1e0);
			gw.setDefaultCloseOperation(GraphWindow.EXIT_ON_CLOSE);
		}
		
		study.getDB().destroy();
	}

}
