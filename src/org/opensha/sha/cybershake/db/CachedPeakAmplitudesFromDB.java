package org.opensha.sha.cybershake.db;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.opensha.commons.data.CSVFile;
import org.opensha.commons.util.ExceptionUtils;
import org.opensha.sha.cybershake.db.CybershakeIM.CyberShakeComponent;
import org.opensha.sha.cybershake.db.CybershakeIM.IMType;
import org.opensha.sha.earthquake.ERF;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.google.common.primitives.Doubles;

public class CachedPeakAmplitudesFromDB extends PeakAmplitudesFromDB {
	
	private static final boolean D = true;
	public static boolean DD = false;
	
	private File cacheDir;
	/**
	 * mapping from runID,im to [sourceID][rupID][rvID]
	 */
//	private Table<Integer, CybershakeIM, double[][][]> cache;
	private LoadingCache<CacheKey, double[][][]> cache;
	private static int maxRuns = 5;
	private ERF erf;
	
	private SiteInfo2DB sites2db;
	private Runs2DB runs2db;
	
	private static int max_rups_per_query = 500;
	
	private class CacheKey {
		private Integer runID;
		private CybershakeIM im;
		public CacheKey(Integer runID, CybershakeIM im) {
			super();
			this.runID = runID;
			this.im = im;
		}
		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + getOuterType().hashCode();
			result = prime * result + ((im == null) ? 0 : im.hashCode());
			result = prime * result + ((runID == null) ? 0 : runID.hashCode());
			return result;
		}
		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			CacheKey other = (CacheKey) obj;
			if (!getOuterType().equals(other.getOuterType()))
				return false;
			if (im == null) {
				if (other.im != null)
					return false;
			} else if (!im.equals(other.im))
				return false;
			if (runID == null) {
				if (other.runID != null)
					return false;
			} else if (!runID.equals(other.runID))
				return false;
			return true;
		}
		private CachedPeakAmplitudesFromDB getOuterType() {
			return CachedPeakAmplitudesFromDB.this;
		}
	}
	
	private class CustomLoader extends CacheLoader<CacheKey, double[][][]> {

		@Override
		public double[][][] load(CacheKey key) throws Exception {
			return getAllIM_Values(key.runID, key.im);
		}
		
	}

	public CachedPeakAmplitudesFromDB(DBAccess dbaccess, File cacheDir, ERF erf) {
		super(dbaccess);
		
		this.cacheDir = cacheDir;
		cache = CacheBuilder.newBuilder().maximumSize(maxRuns).build(new CustomLoader());
		this.erf = erf;
		
		sites2db = new SiteInfo2DB(dbaccess);
		runs2db = new Runs2DB(dbaccess);
	}

	@Override
	public List<Double> getIM_Values(int runID, int srcId, int rupId,
			CybershakeIM im) throws SQLException {
		double[][][] runVals;
		try {
			runVals = cache.get(new CacheKey(runID, im));
		} catch (ExecutionException e) {
			throw ExceptionUtils.asRuntimeException(e);
		}
		
		return Doubles.asList(runVals[srcId][rupId]);
	}
	
	public synchronized double[][][] getAllIM_Values(int runID, CybershakeIM im) throws SQLException {
		double[][][] vals;
		File cacheFile = getCacheFile(runID, im);
		if (cacheFile != null && cacheFile.exists()) {
			try {
				vals = loadCacheFile(cacheFile);
			} catch (IOException e) {
				throw ExceptionUtils.asRuntimeException(e);
			}
		} else {
			// need to get it from the db
			int tries = 3;
			vals = null;
			SQLException ex = null;
			while (tries >= 0 && vals == null) {
				try {
					vals = loadAmpsFromDB(runID, im);
				} catch (SQLException e) {
					ex = e;
					try {
						Thread.sleep(200);
					} catch (InterruptedException e1) {}
				}
				tries--;
			}
			if (vals == null) {
				System.out.println("Cache failed after 3 tries!");
				throw ex;
			}
			if (cacheFile != null) {
				try {
					writeCacheFile(vals, cacheFile);
				} catch (IOException e) {
					throw ExceptionUtils.asRuntimeException(e);
				}
			}
		}
		
		return vals;
	}
	
	private double[][][] loadAmpsFromDB(int runID, CybershakeIM im) throws SQLException {
		if (D) System.out.println("Loading amps for "+runID);
		
		double[][][] vals = new double[erf.getNumSources()][][];
		
		CybershakeRun run = runs2db.getRun(runID);
		Preconditions.checkNotNull(run, "No run found for "+runID+"?");
		if (D) System.out.println("Getting source list");
		List<Integer> sourcesLeft = Lists.newArrayList(sites2db.getSrcIdsForSite(run.getSiteID(), run.getERFID()));
		Preconditions.checkState(!sourcesLeft.isEmpty());
		
		while (!sourcesLeft.isEmpty()) {
			List<Integer> sources = Lists.newArrayList();
			int numRups = 0;
			while (numRups < max_rups_per_query && !sourcesLeft.isEmpty()) {
				int sourceID = sourcesLeft.remove(0);
				Preconditions.checkState(sourceID<vals.length);
				numRups += erf.getNumRuptures(sourceID);
				sources.add(sourceID);
			}
//			if (D) System.out.println("Getting amps for "+sources.size()+" sources ("+numRups+" rups)");
			fillInAmpsFromDB(runID, im, sources, vals);
			for (int sourceID : sources)
				Preconditions.checkState(vals[sourceID] != null,
				"Amps not filled in for run="+runID+", im="+im.getID()+", source="+sourceID+". Amps table incomplete?");
		}
//		if (D) System.out.println("Done loading vals for "+runID);
		
		return vals;
	}
	
	private void fillInAmpsFromDB(int runID, CybershakeIM im, List<Integer> sources, double[][][] vals)
			throws SQLException {
		Preconditions.checkArgument(!sources.isEmpty());
		String sql = "SELECT Source_ID,Rupture_ID,Rup_Var_ID,IM_Value from "+TABLE_NAME+" where Run_ID="+runID
				+" and IM_Type_ID = '"+im.getID()+"' and Source_ID IN ("+Joiner.on(",").join(sources)
				+")";
		// not explicitly sorting because it's much faster this way but including checks to ensure that it's in order already
		if (DD) System.out.println(sql);
		ResultSet rs = null;
		try {
			rs = dbaccess.selectData(sql, max_rups_per_query*10);
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
//		if (D) System.out.println("Done selecting");
		boolean valid = rs.first();
		if (!valid) {
			rs.close();
			// no matches
			return;
		}
		
		int prevSourceID = -1;
		int prevRupID = -1;
		List<Double> curIMs = null;

		while (valid) {
			int sourceID = rs.getInt(1);
			int rupID = rs.getInt(2);
			int rvID = rs.getInt(3);
			double imVal = rs.getDouble(4);
			
			if (prevSourceID != sourceID) {
				// new source
				Preconditions.checkState(vals[sourceID] == null, "duplicate source?");
				vals[sourceID] = new double[erf.getNumRuptures(sourceID)][];
				Preconditions.checkState(sourceID >= prevSourceID, "Source IDs not sorted?");
			}
			
			if (prevSourceID != sourceID || prevRupID != rupID) {
				if (prevSourceID == sourceID)
					Preconditions.checkState(rupID >= prevRupID, "Rup IDs not sorted?");
				if (curIMs != null) {
					Preconditions.checkState(vals[prevSourceID].length > prevRupID);
					Preconditions.checkState(vals[prevSourceID][prevRupID] == null, "duplicate rup");
					vals[prevSourceID][prevRupID] = Doubles.toArray(curIMs);
				}
				prevSourceID = sourceID;
				prevRupID = rupID;
				curIMs = Lists.newArrayList();
			}
			Preconditions.checkState(rvID == curIMs.size(), "RV IDs not returned in order");
			curIMs.add(imVal);
			valid = rs.next();
		}
		if (!curIMs.isEmpty()) {
			Preconditions.checkState(vals[prevSourceID].length > prevRupID);
			Preconditions.checkState(vals[prevSourceID][prevRupID] == null, "duplicate rup");
			vals[prevSourceID][prevRupID] = Doubles.toArray(curIMs);
		}
		rs.close();
	}
	
	private File getCacheFile(int runID, CybershakeIM im) {
		if (cacheDir == null)
			return null;
		return new File(cacheDir, "run_"+runID+"_im_"+im.getID()+".bin");
	}
	
	public boolean isFileCached(int runID, CybershakeIM im) {
		return getCacheFile(runID, im).exists();
	}
	
	private static void writeCacheFile(double[][][] cache, File file) throws IOException {
		if (D) System.out.println("Writing cache to "+file.getName());
		// make sure not empty
		boolean notNull = false;
		checkLoop:
		for (int i=0; i<cache.length; i++) {
			if (cache[i] != null) {
				for (int j=0; j<cache[i].length; j++) {
					if (cache[i][j] != null && cache[i][j].length > 0) {
						notNull = true;
						break checkLoop;
					}
				}
			}
		}
		Preconditions.checkState(notNull, "No valid values found!");
		DataOutputStream out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));

		out.writeInt(cache.length);

		for (double[][] array2 : cache) {
			// source array
			if (array2 == null) {
				out.writeInt(0);
				continue;
			}
			out.writeInt(array2.length);
			for (double[] array : array2) {
				if (array == null) {
					out.writeInt(0);
					continue;
				}
				out.writeInt(array.length);
				for (double val : array)
					out.writeDouble(val);
			}
		}

		out.close();
	}
	
	private static double[][][] loadCacheFile(File file) throws IOException {
		if (D) System.out.println("Loading cache from "+file.getName());
		long len = file.length();
		Preconditions.checkState(len > 0, "file is empty!");
		Preconditions.checkState(len % 4 == 0, "file size isn't evenly divisible by 4, " +
		"thus not a sequence of double & integer values.");

		InputStream is = new FileInputStream(file);
		Preconditions.checkNotNull(is, "InputStream cannot be null!");
		is = new BufferedInputStream(is);

		DataInputStream in = new DataInputStream(is);

		int size = in.readInt();

		Preconditions.checkState(size > 0, "Size must be > 0!");
		
		double[][][] ret = new double[size][][];

		for (int i=0; i<size; i++) {
			int arraySize = in.readInt();
			if (arraySize == 0)
				continue;
			
			ret[i] = new double[arraySize][];
			
			for (int j=0; j<arraySize; j++) {
				int array2Size = in.readInt();
				if (array2Size == 0)
					continue;
				
				ret[i][j] = new double[array2Size];
				for (int k=0; k<array2Size; k++)
					ret[i][j][k] = in.readDouble();
			}
		}

		in.close();

		return ret;
	}
	
	public void clearCache() {
		cache.invalidateAll();
	}
	
	public DBAccess getDBAccess() {
		return dbaccess;
	}
	
	public static void main(String[] args) {
		CachedPeakAmplitudesFromDB amps2db = new CachedPeakAmplitudesFromDB(Cybershake_OpenSHA_DBApplication.getDB(),
				new File("/tmp/amp_cache"), MeanUCERF2_ToDB.createUCERF2ERF());
		CybershakeIM im = new CybershakeIM(146, IMType.SA, 3d, null, CyberShakeComponent.RotD100);
		int runID = 2703;
		try {
			Stopwatch watch = Stopwatch.createStarted();
			amps2db.getAllIM_Values(runID, im);
			watch.stop();
			System.out.println("Took "+watch.elapsed(TimeUnit.SECONDS)+" secs");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			Cybershake_OpenSHA_DBApplication.getDB().destroy();
		}
	}

}
