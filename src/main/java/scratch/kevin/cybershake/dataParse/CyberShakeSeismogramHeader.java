package scratch.kevin.cybershake.dataParse;

import java.io.DataInput;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import com.google.common.io.LittleEndianDataInputStream;

public class CyberShakeSeismogramHeader {
	
	public enum Component {
		X(1),
		Y(2),
		Z(4);
		
		private int flag;
		private Component(int flag) {
			this.flag = flag;
		}
	}
	
	private String version;
	private String siteName;
	private int sourceID;
	private int rupID;
	private int rvID;
	private float dt;
	private int nt;
	private Component[] comps;
	private float detMaxFreq;
	private float stochMaxFreq;
	
	public CyberShakeSeismogramHeader(String version, String siteName, int sourceID, int rupID, int rvID, float dt, int nt,
			Component[] comps, float detMaxFreq, float stochMaxFreq) {
		super();
		this.version = version;
		this.siteName = siteName;
		this.sourceID = sourceID;
		this.rupID = rupID;
		this.rvID = rvID;
		this.dt = dt;
		this.nt = nt;
		this.comps = comps;
		this.detMaxFreq = detMaxFreq;
		this.stochMaxFreq = stochMaxFreq;
	}
	
	@Override
	public String toString() {
		return "SeismogramHeader [version=" + version + ", siteName=" + siteName + ", sourceID=" + sourceID + ", rupID="
				+ rupID + ", rvID=" + rvID + ", dt=" + dt + ", nt=" + nt + ", comps=" + Arrays.toString(comps)
				+ ", detMaxFreq=" + detMaxFreq + ", stochMaxFreq=" + stochMaxFreq + "]";
	}
	
	private static String readString(DataInput din, int bytes) throws IOException {
		String ret = "";
		for (int i=0; i<bytes; i++) {
			byte b = din.readByte();
//			System.out.println(i+". "+(char)b);
			if (b >= 0 && b < 127) // 127 is DEL
				ret += (char)b;
		}
		return ret.trim();
	}

	public static CyberShakeSeismogramHeader read(DataInput din) throws IOException {
		String version = readString(din, 8);
		String siteName = readString(din, 8);
//		System.out.println("Version: "+version);
//		System.out.println("Site Name: "+siteName);
		// padding
		readString(din, 8);
		int sourceID = din.readInt();
		int rupID = din.readInt();
		int rvID = din.readInt();
		float dt = din.readFloat();
		int nt = din.readInt();
//		System.out.println("dt="+dt+", nt="+nt);
		int comps = din.readInt();
		float detMaxFreq = din.readFloat();
		float stochMaxFreq = din.readFloat();
		
		return new CyberShakeSeismogramHeader(version, siteName, sourceID, rupID, rvID, dt, nt, decodeComps(comps), detMaxFreq, stochMaxFreq);
	}
	
	private static int encodeComps(Component... comps) {
		int ret = 0;
		for (int i=0; i<comps.length; i++) {
			Component comp = comps[i];
			if (i == 0)
				ret = comp.flag;
			else
				ret = ret | comp.flag;
		}
		return ret;
	}
	
	private static Component[] decodeComps(int flag) {
		switch (flag) {
		case 1:
			return new Component[] { Component.X };
		case 2:
			return new Component[] { Component.Y };
		case 4:
			return new Component[] { Component.Z };
		case 3:
			return new Component[] { Component.X, Component.Y };
		case 6:
			return new Component[] { Component.X, Component.Y, Component.Z };

		default:
			throw new IllegalStateException("Unknown flag: "+flag);
		}
	}
	
	public String getVersion() {
		return version;
	}

	public String getSiteName() {
		return siteName;
	}

	public int getSourceID() {
		return sourceID;
	}

	public int getRupID() {
		return rupID;
	}

	public int getRVID() {
		return rvID;
	}

	public float getDt() {
		return dt;
	}

	public int getNt() {
		return nt;
	}

	public Component[] getComps() {
		return comps;
	}

	public float getDetMaxFreq() {
		return detMaxFreq;
	}

	public float getStochMaxFreq() {
		return stochMaxFreq;
	}

	public static void main(String[] args) throws IOException {
//		System.out.println(encodeComps(Component.X));
//		System.out.println(encodeComps(Component.Y));
//		System.out.println(encodeComps(Component.Z));
//		System.out.println(encodeComps(Component.X, Component.Y));
//		System.out.println(encodeComps(Component.X, Component.Y, Component.Z));
		
		File file = new File("/home/kevin/CyberShake/data_access/2017_10_24-assignment/PSA_PARK_4673_71_1_57.rotd");
		LittleEndianDataInputStream din = new LittleEndianDataInputStream(new FileInputStream(file));
		
		System.out.println(read(din));
		
		din.close();
	}

}
