package scratch.kevin.cybershake.simCompare;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.opensha.commons.data.region.CaliforniaRegions;
import org.opensha.commons.geo.Region;
import org.opensha.commons.util.ComparablePairing;
import org.opensha.commons.util.FileNameComparator;
import org.opensha.sha.cybershake.db.CybershakeVelocityModel;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.Runs2DB;

import com.google.common.base.Preconditions;

import scratch.kevin.cybershake.simCompare.StudyGMPE_Compare.Vs30_Source;
import scratch.kevin.util.MarkdownUtils;
import scratch.kevin.util.MarkdownUtils.TableBuilder;

public enum CyberShakeStudy {
	
	STUDY_15_4(cal(2015, 4), 57, "Study 15.4", "study_15_4",
			"Los Angeles region with CVM-S4.26 Velocity Model, 1hz", 5,
			new CaliforniaRegions.CYBERSHAKE_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.ARCHIVE_HOST_NAME),
	STUDY_17_3_1D(cal(2017, 3), 80, "Study 17.3 1-D",
			"study_17_3_1d", "Central California with CCA-1D Velocity Model, 1hz", 9,
			new CaliforniaRegions.CYBERSHAKE_CCA_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME),
	STUDY_17_3_3D(cal(2017, 3), 81, "Study 17.3 3-D",
			"study_17_3_3d", "Central California with CCA-06 Velocity Model, 1hz", 10,
			new CaliforniaRegions.CYBERSHAKE_CCA_MAP_REGION(),
			Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
	
	private int datasetID;
	private String name;
	private String dirName;
	private String description;
	private int velocityModelID;
	private Region region;
	private String dbHost;
	private GregorianCalendar date;
	
	private DBAccess db;
	private Runs2DB runs2db;
	
	private CyberShakeStudy(GregorianCalendar date, int datasetID, String name, String dirName, String description,
			int velocityModelID, Region region, String dbHost) {
		this.date = date;
		this.datasetID = datasetID;
		this.name = name;
		this.dirName = dirName;
		this.description = description;
		this.velocityModelID = velocityModelID;
		this.region = region;
		this.dbHost = dbHost;
	}
	
	private static GregorianCalendar cal(int year, int month) {
		return new GregorianCalendar(year, month-1, 1);
	}
	
	private static DateFormat dateFormat = new SimpleDateFormat("MMM yyyy");

	public int getDatasetID() {
		return datasetID;
	}

	public String getName() {
		return name;
	}

	public String getDirName() {
		return dirName;
	}

	public String getDescription() {
		return description;
	}

	public int getVelocityModelID() {
		return velocityModelID;
	}

	public Region getRegion() {
		return region;
	}

	public String getDBHost() {
		return dbHost;
	}
	
	public synchronized DBAccess getDB() {
		if (db == null)
			db = Cybershake_OpenSHA_DBApplication.getDB(dbHost);
		return db;
	}
	
	private synchronized Runs2DB getRunsDB() {
		if (runs2db == null)
			runs2db = new Runs2DB(getDB());
		return runs2db;
	}
	
	public CybershakeVelocityModel getVelocityModel() {
		return getRunsDB().getVelocityModel(velocityModelID);
	}
	
	public List<String> getMarkdownMetadataTable() {
		TableBuilder builder = MarkdownUtils.tableBuilder();
		builder.addLine("**Name**", getName());
		builder.addLine("**Date**", dateFormat.format(date.getTime()));
		String regStr;
		if (getRegion() instanceof CaliforniaRegions.CYBERSHAKE_MAP_REGION)
			regStr = "Los Angeles Box";
		else if (getRegion() instanceof CaliforniaRegions.CYBERSHAKE_CCA_MAP_REGION)
			regStr = "Central California Box";
		else
			regStr = getRegion().getName();
		builder.addLine("**Region**", regStr);
		builder.addLine("**Description**", getDescription());
		CybershakeVelocityModel vm = getVelocityModel();
		builder.addLine("**Velocity Model**", vm.getName()+", "+vm.getVersion());
		return builder.build();
	}
	
	public void writeMarkdownSummary(File dir) throws IOException {
		List<String> lines = new LinkedList<>();
		String topLink = "*[(top)](#"+MarkdownUtils.getAnchorName(getName())+")*";
		lines.add("# "+getName());
		lines.add("## Metadata");
		lines.addAll(getMarkdownMetadataTable());
		lines.add("");
		int tocIndex = lines.size();
		
		Map<Vs30_Source, List<String>> gmpeLinksMap = new HashMap<>();
		Map<Vs30_Source, List<String>> gmpeNamesMap = new HashMap<>();
		
		String rotDDLink = null;
		
		File[] dirList = dir.listFiles();
		Arrays.sort(dirList, new FileNameComparator());
		for (File subDir : dirList) {
			if (!subDir.isDirectory())
				continue;
			File mdFile = new File(subDir, "README.md");
			if (!mdFile.exists())
				continue;
			String name = subDir.getName();
			if (name.startsWith("gmpe_comparisons_") && name.contains("_Vs30")) {
				String gmpeName = name.substring("gmpe_comparisons_".length());
				gmpeName = gmpeName.substring(0, gmpeName.indexOf("_Vs30"));
				String vs30Name = name.substring(name.indexOf("_Vs30")+5);
				Vs30_Source vs30 = Vs30_Source.valueOf(vs30Name);
				Preconditions.checkNotNull(vs30);
				
				if (!gmpeLinksMap.containsKey(vs30)) {
					gmpeLinksMap.put(vs30, new ArrayList<>());
					gmpeNamesMap.put(vs30, new ArrayList<>());
				}
				
				gmpeLinksMap.get(vs30).add(name);
				gmpeNamesMap.get(vs30).add(gmpeName);
			} else if (name.equals("rotd_ratio_comparisons")) {
				Preconditions.checkState(rotDDLink == null, "Duplicate RotDD dirs! %s and %s", name, rotDDLink);
				rotDDLink = name;
			}
		}
		
		if (!gmpeLinksMap.isEmpty()) {
			lines.add("");
			lines.add("## GMPE Comparisons");
			lines.add(topLink);
			lines.add("");
			for (Vs30_Source vs30 : gmpeLinksMap.keySet()) {
				lines.add("### Vs30 model for GMPE comparisons: "+vs30);
				lines.add("");
				List<String> gmpeNames = gmpeNamesMap.get(vs30);
				List<String> gmpeLinks = gmpeLinksMap.get(vs30);
				
				for (int i=0; i<gmpeNames.size(); i++)
					lines.add("* ["+gmpeNames.get(i)+"]("+gmpeLinks.get(i)+"/)");
			}
		}
		if (rotDDLink != null) {
			lines.add("");
			lines.add("## RotD100/RotD50 Ratios");
			lines.add(topLink);
			lines.add("");
			lines.add("[RotD100/RotD50 Ratios Plotted Here]("+rotDDLink+"/)");
		}
		
		lines.addAll(tocIndex, MarkdownUtils.buildTOC(lines, 2));
		
		MarkdownUtils.writeReadmeAndHTML(lines, dir);
	}
	
	public static void writeCatalogsIndex(File dir) throws IOException {
		// sort by date, newest first
		List<Long> times = new ArrayList<>();
		List<CyberShakeStudy> studies = new ArrayList<>();
		for (File subDir : dir.listFiles()) {
			if (!subDir.isDirectory())
				continue;
			CyberShakeStudy study = null;
			for (CyberShakeStudy s : values()) {
				if (subDir.getName().equals(s.getDirName())) {
					study = s;
					break;
				}
			}
			if (study == null)
				continue;
			studies.add(study);
			times.add(study.date.getTimeInMillis());
		}
		studies = ComparablePairing.getSortedData(times, studies);
		Collections.reverse(studies);
		
		TableBuilder table = MarkdownUtils.tableBuilder();
		table.addLine("Date", "Name", "Description");
		for (CyberShakeStudy study : studies) {
			table.initNewLine();
			
			table.addColumn(dateFormat.format(study.date.getTime()));
			table.addColumn("["+study.getName()+"]("+study.getDirName()+"#"+MarkdownUtils.getAnchorName(study.getName())+")");
			table.addColumn(study.getDescription());
			
			table.finalizeLine();
		}
		
		List<String> lines = new LinkedList<>();
		lines.add("# CyberShake Study Analysis");
		lines.add("");
		lines.addAll(table.build());
		
		MarkdownUtils.writeReadmeAndHTML(lines, dir);
	}
	
	public static void main(String[] args) throws IOException {
		File gitDir = new File("/home/kevin/git/cybershake-analysis");
		
		for (CyberShakeStudy study : CyberShakeStudy.values()) {
			File studyDir = new File(gitDir, study.getDirName());
			if (studyDir.exists())
				study.writeMarkdownSummary(studyDir);
		}
		
		writeCatalogsIndex(gitDir);
		
		for (CyberShakeStudy study : CyberShakeStudy.values())
			study.getDB().destroy();
	}

}
