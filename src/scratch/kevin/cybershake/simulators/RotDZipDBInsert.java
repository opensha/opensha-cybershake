package scratch.kevin.cybershake.simulators;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;
import org.opensha.sha.cybershake.db.CybershakeIM;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;

import com.google.common.base.Preconditions;

import scratch.kevin.cybershake.dataParse.CyberShakeRotDFile;
import scratch.kevin.cybershake.dataParse.CyberShakeSeismogramHeader;

public class RotDZipDBInsert {

	public static void main(String[] args) throws IOException {

//		int runID = 7014;
//		File zipFile = new File("/data-0/kevin/simulators/catalogs/rundir2585_1myr/cybershake_rotation_inputs/USC_7014_rotd.zip");
//		int runID = 7016;
//		File zipFile = new File("/data-0/kevin/simulators/catalogs/rundir2585_1myr/cybershake_rotation_inputs/SBSM_7016_rotd.zip");
//		int runID = 7017;
//		File zipFile = new File("/data-0/kevin/simulators/catalogs/rundir2585_1myr/cybershake_rotation_inputs/WNGC_7017_rotd.zip");
//		int runID = 7018;
//		File zipFile = new File("/data-0/kevin/simulators/catalogs/rundir2585_1myr/cybershake_rotation_inputs/STNI_7018_rotd.zip");
//		int runID = 7019;
//		File zipFile = new File("/data-0/kevin/simulators/catalogs/rundir2585_1myr/cybershake_rotation_inputs/SMCA_7019_rotd.zip");
		
		File rotDir = new File("/data-0/kevin/simulators/catalogs/rundir4860_multi_combine/cybershake_rotation_inputs");
		
//		int runID = 7090;
//		File zipFile = new File(rotDir, "OSI_7090_rotd.zip");
//		int runID = 7091;
//		File zipFile = new File(rotDir, "SMCA_7091_rotd.zip");
//		int runID = 7092;
//		File zipFile = new File(rotDir, "SBSM_7092_rotd.zip");
//		int runID = 7093;
//		File zipFile = new File(rotDir, "WSS_7093_rotd.zip");
//		int runID = 7094;
//		File zipFile = new File(rotDir, "WNGC_7094_rotd.zip");
//		int runID = 7095;
//		File zipFile = new File(rotDir, "STNI_7095_rotd.zip");
//		int runID = 7096;
//		File zipFile = new File(rotDir, "PDE_7096_rotd.zip");
//		int runID = 7097;
//		File zipFile = new File(rotDir, "LAF_7097_rotd.zip");
		int runID = 7098;
		File zipFile = new File(rotDir, "s022_7098_rotd.zip");
		
		Preconditions.checkState(zipFile.getName().contains(runID+""));
		
		ZipFile zip = new ZipFile(zipFile);
		
		boolean skipBad = false;
		
		if (!skipBad) {
			if (!testRead(zip)) {
				zip.close();
				System.exit(1);
			}
		}
		
		DBAccess db = Cybershake_OpenSHA_DBApplication.getAuthenticatedDBAccess(
				true, true, Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
		
		CyberShakeComponent comp = CyberShakeComponent.RotD50;
		CybershakeIM[] ims = {
				CybershakeIM.getSA(comp, 3d),
				CybershakeIM.getSA(comp, 4d),
				CybershakeIM.getSA(comp, 5d),
				CybershakeIM.getSA(comp, 7.5d),
				CybershakeIM.getSA(comp, 10d)};
		
		Enumeration<? extends ZipEntry> entries = zip.entries();
		
		int maxInsert = 1000;
		
		List<CyberShakeSeismogramHeader> headers = new ArrayList<>();
		List<DiscretizedFunc> rds = new ArrayList<>();
		
		String name = null;
		try {
			while (entries.hasMoreElements())  {
				if (headers.size() * ims.length >= maxInsert) {
					insert(db, runID, headers, rds, ims);
					headers.clear();
					rds.clear();
				}
				
				ZipEntry entry = entries.nextElement();
				name = entry.getName();
				if (!name.endsWith(".rotd"))
					continue;
				
				CyberShakeRotDFile rd;
				try {
					rd = CyberShakeRotDFile.read(zip.getInputStream(entry));
				} catch (Exception e) {
					if (skipBad) {
						System.err.println("Skipping bad file: "+name);
						continue;
					} else {
						throw e;
					}
				}
				headers.add(rd.getHeader());
				switch (comp) {
				case RotD50:
					rds.add(rd.getRotD50());
					break;
				case RotD100:
					rds.add(rd.getRotD100());
					break;

				default:
					throw new IllegalStateException("Bad component: "+comp);
				}
			}
			if (!headers.isEmpty())
				insert(db, runID, headers, rds, ims);
		} catch (Exception e) {
			System.err.println("Error on entry: "+name);
			e.printStackTrace();
		} finally {
			db.destroy();
			zip.close();			
			System.exit(0);
		}
	}
	
	private static boolean testRead(ZipFile zip) {
		Enumeration<? extends ZipEntry> entries = zip.entries();
		
		int count = 0;
		int fails = 0;
		
		String name = null;
		while (entries.hasMoreElements())  {
			ZipEntry entry = entries.nextElement();
			name = entry.getName();
			if (!name.endsWith(".rotd"))
				continue;

			try {
				CyberShakeRotDFile.read(zip.getInputStream(entry));
			} catch (Exception e) {
				System.err.println("Error on entry: "+name);
//				e.printStackTrace();
				System.err.flush();
				fails++;
			}
			count++;
		}
		System.out.println("Found "+count+" valid RotDs");
		if (fails > 0)
			System.out.println("Failed on "+fails+" :(");
		return fails == 0;
	}
	
	private static void insert(DBAccess db, int runID, List<CyberShakeSeismogramHeader> headers, List<DiscretizedFunc> rds, CybershakeIM[] ims)
			throws SQLException {
		Preconditions.checkState(headers.size() == rds.size());
		StringBuilder sql = new StringBuilder("INSERT INTO PeakAmplitudes (Source_ID,Rupture_ID,Rup_Var_ID,Run_ID,IM_Type_ID,IM_Value)\nVALUES\n");
		for (int i=0; i<headers.size(); i++) {
			CyberShakeSeismogramHeader header = headers.get(i);
			String commonHeader = header.getSourceID()+","+header.getRupID()+","+header.getRVID()+","+runID+",";
			DiscretizedFunc rd = rds.get(i);
			for (int j=0; j<ims.length; j++) {
				double val = rd.getInterpolatedY(ims[j].getVal()) * HazardCurveComputation.CONVERSION_TO_G;
				sql.append("\n(").append(commonHeader).append(ims[j].getID()).append(",").append(val).append(")");
				if (j == ims.length-1 && i == headers.size()-1)
					sql.append(";");
				else
					sql.append(",");
			}
		}
		String sqlStr = sql.toString();
		Preconditions.checkState(sqlStr.endsWith(";"));
		int expected = ims.length * headers.size();
		System.out.println("Inserting "+expected+" IMs");
		int numRows = db.insertUpdateOrDeleteData(sqlStr);
		Preconditions.checkState(numRows == expected, "Expected %s * %s = %s inserts, actual: %s", headers.size(), ims.length, expected, numRows);
	}

}
