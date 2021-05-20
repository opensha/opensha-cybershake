package scratch.kevin.cybershake;

import java.awt.Color;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.StatUtils;
import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.commons.data.TimeSpan;
import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.commons.gui.plot.HeadlessGraphPanel;
import org.opensha.commons.gui.plot.PlotCurveCharacterstics;
import org.opensha.commons.gui.plot.PlotLineType;
import org.opensha.commons.gui.plot.PlotSpec;
import org.opensha.commons.param.impl.DoubleParameter;
import org.opensha.commons.util.cpt.CPT;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.PeakAmplitudesFromDB;
import org.opensha.sha.cybershake.db.Runs2DB;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.FocalMechanism;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.faultSurface.PointSurface;
import org.opensha.sha.faultSurface.RuptureSurface;
import org.opensha.sha.simulators.srf.SRF_PointData;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;

public class GriddedPointSourceFakeERF extends AbstractERF {
	
	private Location siteLoc;
	private SRF_PointData inputSRF;
	private double[] distances;
	private double[] depths;
	private double[] azimuths;
	
	private List<TranslatedPointSource> sources;
	
	static final String RUP_SURF_RESOLUTION_PARAM_NAME = "Rupture Surface Resolution";
	private DoubleParameter rupSurfResParam;

	public GriddedPointSourceFakeERF(Location siteLoc, SRF_PointData inputSRF,
			double[] distances, double[] depths, double[] azimuths) {
		this.siteLoc = siteLoc;
		this.inputSRF = inputSRF;
		this.distances = distances;
		this.depths = depths;
		this.azimuths = azimuths;
		
		sources = new ArrayList<>();
		
		rupSurfResParam = new DoubleParameter(RUP_SURF_RESOLUTION_PARAM_NAME, inputSRF.getArea() / 1e-10);
		adjustableParams.addParameter(rupSurfResParam);
		
		this.timeSpan = new TimeSpan(TimeSpan.NONE, TimeSpan.YEARS);
		this.timeSpan.setDuration(1d);
		
		for (double distance : distances)
			for (double azimuth : azimuths)
				sources.add(new TranslatedPointSource(distance, azimuth));
	}
	
	public class TranslatedPointSource extends ProbEqkSource {
		
		private Location surfaceLoc;
		private double distance;
		private double azimuth;
		private Location[] depthLocs;
		
		public TranslatedPointSource(double distance, double azimuth) {
			this.distance = distance;
			this.azimuth = azimuth;
			surfaceLoc = LocationUtils.location(siteLoc, Math.toRadians(azimuth), distance);
			
			depthLocs = new Location[depths.length];
			for (int i=0; i<depths.length; i++)
				depthLocs[i] = new Location(surfaceLoc.getLatitude(), surfaceLoc.getLongitude(), depths[i]);
		}
		
		public FocalMechanism getFocalMech() {
			return inputSRF.getFocalMech();
		}

		@Override
		public LocationList getAllSourceLocs() {
			return null;
		}

		@Override
		public RuptureSurface getSourceSurface() {
			return new PointSurface(surfaceLoc);
		}

		@Override
		public double getMinDistance(Site site) {
			Location oLoc = site.getLocation();
			if ((float)oLoc.getLatitude() == (float)siteLoc.getLatitude()
					&& (float)oLoc.getLongitude() == (float)siteLoc.getLongitude())
				return distance;
			return Double.POSITIVE_INFINITY;
		}

		@Override
		public int getNumRuptures() {
			return depths.length;
		}

		@Override
		public ProbEqkRupture getRupture(int nRupture) {
			double aveRake = inputSRF.getFocalMech().getRake();
			double probability = 1d/(getNumRuptures()*getNumSources());
			Location loc = new Location(surfaceLoc.getLatitude(), surfaceLoc.getLongitude(), depths[nRupture]);
			return new ProbEqkRupture(0d, aveRake, probability, new PointSurface(loc), loc);
		}
		
	}

	@Override
	public int getNumSources() {
		return sources.size();
	}

	@Override
	public ProbEqkSource getSource(int idx) {
		return sources.get(idx);
	}

	@Override
	public void updateForecast() {}

	@Override
	public String getName() {
		return "Translated PtSrc ERF";
	}
	
	public void writePointsAndSRFs(File outputDir) throws IOException {
		CSVFile<String> mappingCSV = new CSVFile<>(true);
		
		mappingCSV.addLine("Source ID", "Rupture ID", "Distance", "Azimuth", "Latitude", "Longitude", "Depth");
		
		for (int sourceID=0; sourceID<sources.size(); sourceID++) {
			File sourceDir = new File(outputDir, sourceID+"");
			Preconditions.checkState(sourceDir.exists() || sourceDir.mkdir());
			
			TranslatedPointSource source = sources.get(sourceID);
			for (int rupID=0; rupID<depths.length; rupID++) {
				File rupDir = new File(sourceDir, rupID+"");
				Preconditions.checkState(rupDir.exists() || rupDir.mkdir());
				
				File pointFile = new File(rupDir, sourceID+"_"+rupID+".txt");
				
				ProbEqkRupture rup = source.getRupture(rupID);

				Location loc = rup.getHypocenterLocation();
				
				mappingCSV.addLine(sourceID+"", rupID+"", (float)source.distance+"", (float)source.azimuth+"",
						(float)loc.getLatitude()+"", (float)loc.getLongitude()+"", (float)loc.getDepth()+"");
				
				FileWriter fw = new FileWriter(pointFile);
				fw.write("Probability = "+(float)rup.getProbability()+"\n");
				fw.write("Magnitude = "+(float)rup.getMag()+"\n");
				
				double aveArea = inputSRF.getArea() / 1e-10; // cm^2 to km^2
				
				fw.write("AveArea = "+(float)aveArea+"\n");
				fw.write("NumPoints = 1\n");
				
				fw.write("#   Lat         Lon         Depth      Rake    Dip     Strike"+"\n");
				FocalMechanism mech = inputSRF.getFocalMech();
				fw.write((float)loc.getLatitude()+"    "+(float)loc.getLongitude()+"    "
						+(float)loc.getDepth()+"    "+(float)mech.getRake()+"    "+(float)mech.getDip()
						+"    "+(float)mech.getStrike()+"\n");
				
				fw.close();
				
				SRF_PointData translated = inputSRF.translated(loc);
				
				File srfFile = new File(rupDir, sourceID+"_"+rupID+"_event_0.srf");
				
				SRF_PointData.writeSRF(srfFile, Lists.newArrayList(translated), 1d);
			}
		}
		
		mappingCSV.writeToFile(new File(outputDir, "source_rup_info.csv"));
	}
	
	private void insertRVs(DBAccess db, int erfID) {
		int rupVarScenID = 8;
		
		int bundleSize = 1000;
		for (int sourceID=0; sourceID<getNumSources(); sourceID++) {
			ProbEqkSource source = getSource(sourceID);
			List<List<Integer>> bundles = new ArrayList<>();
			List<Integer> curBundle = null;
			for (int rupID=0; rupID<source.getNumRuptures(); rupID++) {
				if (curBundle == null) {
					curBundle = new ArrayList<>();
					bundles.add(curBundle);
				}
				curBundle.add(rupID);
				if (curBundle.size() == bundleSize)
					curBundle = null;
			}
			System.out.println("Inserting "+bundles.size()+" bundle with "+source.getNumRuptures()
				+" ruptures for source "+sourceID);
			for (List<Integer> bundle : bundles) {
				StringBuffer sql = new StringBuffer();
				sql.append("INSERT INTO Rupture_Variations (ERF_ID,Rup_Var_Scenario_ID,Source_ID,Rupture_ID,"
						+ "Rup_Var_ID,Rup_Var_LFN,Hypocenter_Lat,Hypocenter_Lon,Hypocenter_Depth) VALUES");
				boolean first = true;
				for (int rupID : bundle) {
					ProbEqkRupture rup = source.getRupture(rupID);
					String lfn = "e"+erfID+"_rv"+rupVarScenID+"_"+sourceID+"_"+rupID+"_event_0.srf";
					Location hypo = rup.getHypocenterLocation();
					if (!first)
						sql.append(",");
					sql.append("\n('"+erfID+"','"+rupVarScenID+"','"+sourceID+"','"+rupID+"','"+0+"','"+lfn+"','"
							+hypo.getLatitude()+"','"+hypo.getLongitude()+"','"+hypo.getDepth()+"')");
					first = false;
				}
				try {
					db.insertUpdateOrDeleteData(sql.toString());
				} catch (SQLException e) {
					e.printStackTrace();
					db.destroy();
					System.exit(1);
				}
			}
		}
	}
	
	public void plotAzimuthDependence(File outputDir, String prefix, CybershakeRun run, PeakAmplitudesFromDB amps2db, CybershakeIM im,
			double depth, double[] distances, String title) throws SQLException, IOException {
		List<DiscretizedFunc> funcs = new ArrayList<>();
		List<PlotCurveCharacterstics> chars = new ArrayList<>();
		
		CPT cpt = new CPT(StatUtils.min(distances), StatUtils.max(distances), Color.GRAY, Color.BLACK);
		
		for (double distance : distances) {
			funcs.add(new ArbitrarilyDiscretizedFunc((float)distance+" km"));
			chars.add(new PlotCurveCharacterstics(PlotLineType.SOLID, 2f, cpt.getColor((float)distance)));
		}
		
		int depthIndex = -1;
		for (int i=0; i<depths.length; i++) {
			if (depths[i] == depth)
				depthIndex = i;
		}
		Preconditions.checkState(depthIndex >= 0);
		
		for (int i = 0; i < sources.size(); i++) {
			TranslatedPointSource source = sources.get(i);
			if (!Doubles.contains(distances, source.distance))
				continue;
			double imVal = amps2db.getIM_Value(run.getRunID(), i, depthIndex, 0, im) / HazardCurveComputation.CONVERSION_TO_G;
			double az = source.azimuth;
			if (az > 180)
				az -=360;
			for (int d=0; d<distances.length; d++)
				if (distances[d] == source.distance)
					funcs.get(d).set(az, imVal);
		}
		
		PlotSpec spec = new PlotSpec(funcs, chars, title, "Azimuth", (float)im.getVal()+"s SA");
		spec.setLegendVisible(true);
		
		HeadlessGraphPanel gp = new HeadlessGraphPanel();
		gp.setTickLabelFontSize(18);
		gp.setAxisLabelFontSize(24);
		gp.setPlotLabelFontSize(24);
		gp.setLegendFontSize(20);
		gp.setBackgroundColor(Color.WHITE);
		
		gp.drawGraphPanel(spec, false, true, null, null);
		
		gp.getChartPanel().setSize(800, 800);
		File file = new File(outputDir, prefix);
		gp.saveAsPNG(file.getAbsolutePath()+".png");
		gp.saveAsPDF(file.getAbsolutePath()+".pdf");
	}
	
	public static double[] getDiscretized(double min, int num, double delta, boolean wrap360) {
		double[] array = new double[num];
		for (int i=0; i<num; i++) {
			array[i] = min + i*delta;
			if (wrap360 && array[i] >= 360)
				array[i] -= 360;
		}
		return array;
	}

	public static void main(String[] args) throws IOException, SQLException {
		double[] depths = { 1,2,4,6,8,10,12,15,20,25 };
		double[] dists = GriddedPointSourceFakeERF.getDiscretized(10, 20, 10, false);
		double[] azimuths = GriddedPointSourceFakeERF.getDiscretized(315, 19, 5, true);
		System.out.println("Depths: "+Joiner.on(",").join(Doubles.asList(depths)));
		System.out.println("Distances: "+Joiner.on(",").join(Doubles.asList(dists)));
		System.out.println("Azimuths: "+Joiner.on(",").join(Doubles.asList(azimuths)));
		
		File mainDir = new File("/home/kevin/CyberShake/point_source_grid_erf");
		File sourceRupDir = new File(mainDir, "cs_input_files");
		Preconditions.checkState(sourceRupDir.exists() || sourceRupDir.mkdir());
		
		File inputSRF_file = new File(mainDir, "point-dt0.05.srf");
		SRF_PointData inputSRF = SRF_PointData.readSRF(inputSRF_file).get(0);
		
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
		
		String siteName = "s1262";
		Location siteLoc = new SiteInfo2DB(db).getLocationForSite(siteName);
		
		GriddedPointSourceFakeERF erf = new GriddedPointSourceFakeERF(siteLoc, inputSRF, dists, depths, azimuths);
		
		PeakAmplitudesFromDB amps2db = new PeakAmplitudesFromDB(db);
		
		String globalPrefix = "azimuth_dependence";
		String[] vmPefixes = { "1D", "3D" };
		int[] runIDs = { 7028, 7027 };
		double[] plotDepths = { 1d, 4d, 20d, 25d };
		
		double[] plotDists = { 20, 40, 60, 80, 100 };
		
		Runs2DB runs2db = new Runs2DB(db);
		CybershakeIM im = CybershakeIM.getSA(CyberShakeComponent.RotD50, 3d);
		
		for (int r=0; r<vmPefixes.length; r++) {
			int runID = runIDs[r];
			CybershakeRun run = runs2db.getRun(runID);
			
			for (double depth : plotDepths) {
				String prefix = globalPrefix+"_"+vmPefixes[r].toLowerCase()+"_depth"+(int)depth+"km";
				String title = vmPefixes[r]+" Azimuth Dependence, "+(int)depth+"km Source Depth";
				erf.plotAzimuthDependence(new File("/tmp"), prefix, run, amps2db, im, depth, plotDists, title);
			}
		}
		
//		erf.writePointsAndSRFs(sourceRupDir);
		
//		DBAccess writeDB = Cybershake_OpenSHA_DBApplication.getAuthenticatedDBAccess(
//				true, true, Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
//		erf.insertRVs(writeDB, 52);
//		writeDB.destroy();
		
		db.destroy();
	}

}
