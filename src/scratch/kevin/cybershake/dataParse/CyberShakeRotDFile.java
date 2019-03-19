package scratch.kevin.cybershake.dataParse;

import java.io.Closeable;
import java.io.DataInput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;

import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;

import com.google.common.io.LittleEndianDataInputStream;

public class CyberShakeRotDFile {

	private CyberShakeSeismogramHeader header;
	private DiscretizedFunc rd50;
	private DiscretizedFunc rd100;
	private DiscretizedFunc rd100Angle;

	public CyberShakeRotDFile(CyberShakeSeismogramHeader header, DiscretizedFunc rd50,
			DiscretizedFunc rd100, DiscretizedFunc rd100Angle) {
		this.header = header;
		this.rd50 = rd50;
		this.rd100 = rd100;
		this.rd100Angle = rd100Angle;
	}
	
	public static CyberShakeRotDFile read(File file) throws IOException {
		return read(new FileInputStream(file));
	}
	
	public static CyberShakeRotDFile read(InputStream is) throws IOException {
		DataInput din = new LittleEndianDataInputStream(is);
		return read(din);
	}
	
	public static CyberShakeRotDFile read(DataInput din) throws IOException {
		CyberShakeSeismogramHeader header = CyberShakeSeismogramHeader.read(din);
		
		int numRecs = din.readInt();
		
		DiscretizedFunc rd50 = new ArbitrarilyDiscretizedFunc();
		DiscretizedFunc rd100 = new ArbitrarilyDiscretizedFunc();
		DiscretizedFunc rd100Angle = new ArbitrarilyDiscretizedFunc();
		
		for (int i=0; i<numRecs; i++) {
			float p = din.readFloat();
			rd100.set(p, din.readFloat());
			rd100Angle.set(p, (double)din.readInt());
			rd50.set(p, din.readFloat());
		}
		
		if (din instanceof Closeable)
			((Closeable)din).close();
		
		return new CyberShakeRotDFile(header, rd50, rd100, rd100Angle);
	}
	
	public CyberShakeSeismogramHeader getHeader() {
		return header;
	}

	public DiscretizedFunc getRotD50() {
		return rd50;
	}

	public DiscretizedFunc getRotD100() {
		return rd100;
	}

	public DiscretizedFunc getRotD100Angle() {
		return rd100Angle;
	}

	private static final DecimalFormat df = new DecimalFormat("0.######");
	
	@Override
	public String toString() {
		StringBuffer str = new StringBuffer(header.toString());
		str.append("\n").append("Period\tRotD100\tAngle\tRotD50");
		
		for (int i=0; i<rd100.size(); i++) {
			str.append("\n");
			str.append(df.format(rd100.getX(i))).append("\t");
			str.append(df.format(rd100.getY(i))).append("\t");
			str.append(df.format(rd100Angle.getY(i))).append("\t");
			str.append(df.format(rd50.getY(i)));
		}
		
		return str.toString();
	}

	public static void main(String[] args) throws IOException {
		File file = new File("/home/kevin/CyberShake/data_access/2017_10_24-assignment/PSA_PARK_5564_71_1_57.rotd");
		CyberShakeRotDFile rotd = read(file);
		System.out.println(rotd);
		System.out.println("Closest X to 0.011079: "+rotd.getRotD50().getClosestXtoY(0.011079));
	}

}
