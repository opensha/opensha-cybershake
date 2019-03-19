package scratch.kevin.cybershake;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.data.Site;
import org.opensha.commons.geo.Location;
import org.opensha.commons.geo.LocationList;
import org.opensha.commons.geo.LocationUtils;
import org.opensha.sha.cybershake.constants.CyberShakeStudy;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.SiteInfo2DB;
import org.opensha.sha.earthquake.AbstractERF;
import org.opensha.sha.earthquake.FocalMechanism;
import org.opensha.sha.earthquake.ProbEqkRupture;
import org.opensha.sha.earthquake.ProbEqkSource;
import org.opensha.sha.faultSurface.PointSurface;
import org.opensha.sha.faultSurface.RuptureSurface;
import org.opensha.sha.simulators.srf.SRF_PointData;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

public class GriddedPointSourceFakeERF extends AbstractERF {
	
	private Location siteLoc;
	private SRF_PointData inputSRF;
	private double[] distances;
	private double[] depths;
	private double[] azimuths;
	
	private List<TranslatedPointSource> sources;

	public GriddedPointSourceFakeERF(Location siteLoc, SRF_PointData inputSRF,
			double[] distances, double[] depths, double[] azimuths) {
		this.siteLoc = siteLoc;
		this.inputSRF = inputSRF;
		this.distances = distances;
		this.depths = depths;
		this.azimuths = azimuths;
		
		sources = new ArrayList<>();
		
		for (double distance : distances)
			for (double azimuth : azimuths)
				sources.add(new TranslatedPointSource(distance, azimuth));
	}
	
	private class TranslatedPointSource extends ProbEqkSource {
		
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
			return new ProbEqkRupture(6d, aveRake, probability, new PointSurface(loc), loc);
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
		return "Trans PtSrc ERF";
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
				
				File srfFile = new File(rupDir, sourceID+"_"+rupID+".srf");
				
				SRF_PointData.writeSRF(srfFile, Lists.newArrayList(translated), 1d);
			}
		}
		
		mappingCSV.writeToFile(new File(outputDir, "source_rup_info.csv"));
	}

	public static void main(String[] args) throws IOException {
		double[] depths = { 1,2,4,6,8,10,12,15,20,25 };
		double[] dists = { 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160, 170, 180 };
		double[] azimuths = { 0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90 };
		
		File mainDir = new File("/home/kevin/CyberShake/point_source_grid_erf");
		File sourceRupDir = new File(mainDir, "cs_input_files");
		Preconditions.checkState(sourceRupDir.exists() || sourceRupDir.mkdir());
		
		File inputSRF_file = new File(mainDir, "point-dt0.05.srf");
		SRF_PointData inputSRF = SRF_PointData.readSRF(inputSRF_file).get(0);
		
		String siteName = "USC";
		DBAccess db = CyberShakeStudy.STUDY_17_3_3D.getDB();
		Location siteLoc = new SiteInfo2DB(db).getLocationForSite(siteName);
		
		GriddedPointSourceFakeERF erf = new GriddedPointSourceFakeERF(siteLoc, inputSRF, dists, depths, azimuths);
		
		erf.writePointsAndSRFs(sourceRupDir);
		
		db.destroy();
	}

}
