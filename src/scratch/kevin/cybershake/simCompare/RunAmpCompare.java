package scratch.kevin.cybershake.simCompare;

import java.awt.Color;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.data.function.DefaultXY_DataSet;
import org.opensha.commons.data.function.XY_DataSet;
import org.opensha.commons.gui.plot.GraphWindow;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.gui.plot.PlotSymbol;
import org.opensha.commons.util.DataUtils.MinMaxAveTracker;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.CachedPeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;

import com.google.common.base.Preconditions;

public class RunAmpCompare {

	public static void main(String[] args) {
		CyberShakeStudy study = CyberShakeStudy.STUDY_19_3_RSQSIM_ROT_2585;
		int runID1 = 7014;
		int runID2 = 7054;
		
//		CyberShakeStudy study = CyberShakeStudy.STUDY_18_8;
//		int runID1 = 6707;
//		int runID2 = 7036;
		
		CybershakeIM[] ims = {
				CybershakeIM.getSA(CyberShakeComponent.RotD100, 3d),
		};
//		CybershakeIM[] ims = {
//				CybershakeIM.getSA(CyberShakeComponent.RotD50, 3d),
//				CybershakeIM.getSA(CyberShakeComponent.RotD50, 5d),
//				CybershakeIM.getSA(CyberShakeComponent.RotD50, 10d)
//		};
		
		DBAccess db = study.getDB();
		Runs2DB runs2db = new Runs2DB(db);
		
		CybershakeRun run1 = runs2db.getRun(runID1);
		CybershakeRun run2 = runs2db.getRun(runID2);
		
		Preconditions.checkNotNull(run1);
		Preconditions.checkNotNull(run2);
		
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(db, null, study.getERF());
		
		for (CybershakeIM im : ims) {
			double[][][] amps1;
			double[][][] amps2;
			try {
				amps1 = amps2db.getAllIM_Values(run1.getRunID(), im);
				amps2 = amps2db.getAllIM_Values(run2.getRunID(), im);
			} catch (SQLException e) {
				e.printStackTrace();
				continue;
			}
			
			System.out.println(im);
			MinMaxAveTracker absDiffTrack = new MinMaxAveTracker();
			MinMaxAveTracker ratioTrack = new MinMaxAveTracker();
			
			Preconditions.checkState(amps1.length == amps2.length,
					"Source lenghts inconsistent, %s != %s", amps1.length, amps2.length);
			
			DefaultXY_DataSet scatter = new DefaultXY_DataSet();
			
			double maxAbsDiff = 0d;
			String maxAbsStr = null;
			double maxRatio = 1d;
			String maxRatioStr = null;
			
			for (int sourceID=0; sourceID<amps1.length; sourceID++) {
				if (amps1[sourceID] == null) {
					Preconditions.checkState(amps2[sourceID] == null, "no sourceID=%s amps for run %s, but %s amps for run %s",
							sourceID, runID1, amps2[sourceID] == null ? 0 : amps2[sourceID].length, runID2);
					continue;
				}
				Preconditions.checkArgument(amps1[sourceID].length == amps2[sourceID].length,
						"run %s has %s rups for source %s, but %s has %s",
						runID1, amps1[sourceID].length, sourceID, runID2, amps2[sourceID].length);
				for (int rupID=0; rupID<amps1[sourceID].length; rupID++) {
					if (amps1[sourceID][rupID] == null) {
						Preconditions.checkState(amps2[sourceID][rupID] == null,
								"no sourceID=%s, rupID=%s amps for run %s, but %s amps for run %s",
								sourceID, rupID, runID1, amps2[sourceID][rupID] == null ? 0 : amps2[sourceID][rupID].length, runID2);
						continue;
					}
					Preconditions.checkArgument(amps1[sourceID][rupID].length == amps2[sourceID][rupID].length,
							"run %s has %s rups for source %s, but %s has %s",
							runID1, amps1[sourceID][rupID].length, sourceID, runID2, amps2[sourceID][rupID].length);
					for (int rvID=0; rvID<amps1[sourceID][rupID].length; rvID++) {
						double v1 = amps1[sourceID][rupID][rvID];
						double v2 = amps2[sourceID][rupID][rvID];
						scatter.set(v1, v2);
						
						double absDiff = Math.abs(v1 - v2);
						absDiffTrack.addValue(absDiff);
						double ratio = v1 > v2 ? v1/v2 : v2/v1;
						ratioTrack.addValue(ratio);
						if (absDiff > maxAbsDiff) {
							maxAbsDiff = absDiff;
							maxAbsStr = "Source "+sourceID+", Rup "+rupID+", RV "+rvID+": |"+v1+" - "+v2+"| = "+absDiff;
						}
						if (ratio > maxRatio) {
							maxRatio = ratio;
							if (v1 > v2)
								maxRatioStr = "Source "+sourceID+", Rup "+rupID+", RV "+rvID+": "+v1+" / "+v2+" = "+ratio;
							else
								maxRatioStr = "Source "+sourceID+", Rup "+rupID+", RV "+rvID+": "+v2+" / "+v1+" = "+ratio;
						}
					}
				}
			}
			
			System.out.println("Absolute Differences:");
			System.out.println("\t"+absDiffTrack);
			System.out.println("\tLargest: "+maxAbsStr);
			System.out.println("Ratio:");
			System.out.println("\t"+ratioTrack);
			System.out.println("\tLargest: "+maxRatioStr);
			
			List<XY_DataSet> funcs = new ArrayList<>();
			List<PlotCurveCharacterstics> chars = new ArrayList<>();
			
			funcs.add(scatter);
			chars.add(new PlotCurveCharacterstics(PlotSymbol.CROSS, 2f, Color.BLACK));
			PlotSpec spec = new PlotSpec(funcs, chars, "Run ID Comparison, "+(float)im.getVal()+"s "+im.getComponent(),
					"Run "+runID1, "Run "+runID2);
			GraphWindow gw = new GraphWindow(spec, false);
			gw.setXLog(true);
			gw.setYLog(true);
			gw.setVisible(true);
			gw.setDefaultCloseOperation(GraphWindow.EXIT_ON_CLOSE);
		}
		
		db.destroy();
	}

}
