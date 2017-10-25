package scratch.kevin.cybershake.dataParse;

import java.io.Closeable;
import java.io.DataInput;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.sha.cybershake.calc.HazardCurveComputation;

import com.google.common.base.Preconditions;
import com.google.common.io.LittleEndianDataInputStream;

import scratch.kevin.cybershake.dataParse.CyberShakeSeismogramHeader.Component;

public class CyberShakePSAFile {
	
	private static double[] periods = {10.0, 9.5, 9.0, 8.5, 8.0, 7.5, 7.0, 6.5, 6.0, 5.5, 5.0, 4.8, 4.6, 4.4,
			4.2, 4.0, 3.8, 3.6, 3.4, 3.2, 3.0, 2.8, 2.6, 2.4, 2.2, 2.0, 1.6667, 1.42857, 1.25, 1.1111,
			1.0, 0.6667, 0.5, 0.4, 0.3333, 0.285714, 0.25, 0.2222, 0.2, 0.1667, 0.142857, 0.125, 0.1111, 0.1};

	private CyberShakeSeismogramHeader header;
	private DiscretizedFunc[] values;
	
	private boolean unitsG = false;

	public CyberShakePSAFile(CyberShakeSeismogramHeader header, DiscretizedFunc[] values, boolean unitsG) {
		this.header = header;
		this.values = values;
		this.unitsG = unitsG;
		Preconditions.checkState(values.length > 0);
		Preconditions.checkState(header.getComps().length == values.length);
	}
	
	public CyberShakeSeismogramHeader getHeader() {
		return header;
	}

	public DiscretizedFunc[] getValues() {
		return values;
	}
	
	public DiscretizedFunc getValues(Component comp) {
		for (int i=0; i<header.getComps().length; i++)
			if (header.getComps()[i] == comp)
				return values[i];
		throw new IllegalStateException("Doesn't have component "+comp);
	}
	
	public DiscretizedFunc getGeometricMean() {
		DiscretizedFunc xFunc = getValues(Component.X);
		DiscretizedFunc yFunc = getValues(Component.Y);
		DiscretizedFunc geoMean = new ArbitrarilyDiscretizedFunc();
		
		for (int i=0; i<xFunc.size(); i++) {
			double x = xFunc.getY(i);
			double y = yFunc.getY(i);
			geoMean.set(xFunc.getX(i), Math.sqrt(x*y));
		}
		
		return geoMean;
	}
	
	public CyberShakePSAFile asUnitsG() {
		if (unitsG)
			return this;
		DiscretizedFunc[] conv = new DiscretizedFunc[values.length];
		for (int i=0; i<conv.length; i++) {
			conv[i] = values[i].deepClone();
			for (int p=0; p<conv[i].size(); p++)
				conv[i].set(p, conv[i].getY(p)/HazardCurveComputation.CONVERSION_TO_G);
		}
		
		return new CyberShakePSAFile(getHeader(), conv, true);
	}

	public static List<CyberShakePSAFile> read(File file) throws IOException {
		DataInput din = new LittleEndianDataInputStream(new FileInputStream(file));
		return read(din);
	}
	
	public static List<CyberShakePSAFile> read(DataInput din) throws IOException {
		List<CyberShakePSAFile> ret = new ArrayList<>();
		boolean reading = false;
		try {
			while (true) {
				CyberShakeSeismogramHeader header = CyberShakeSeismogramHeader.read(din);
				reading = true;
				DiscretizedFunc[] values = new DiscretizedFunc[header.getComps().length];
				for (int i=0; i<values.length; i++) {
					values[i] = new ArbitrarilyDiscretizedFunc();
					for (int p=0; p<periods.length; p++)
						values[i].set(periods[p], din.readFloat());
				}
				reading = false;
				ret.add(new CyberShakePSAFile(header, values, false));
			}
		} catch(EOFException e) {
			Preconditions.checkState(!reading, "EOF while reading record");
		}
		return ret;
	}
	
	private static final DecimalFormat df = new DecimalFormat("0.####");
	
	@Override
	public String toString() {
		StringBuffer str = new StringBuffer(header.toString()).append("\nPeriod");
		for (Component comp : header.getComps())
			str.append("\t").append(comp);
		DiscretizedFunc geoMean = null;
		try {
			geoMean = getGeometricMean();
		} catch (Exception e) {}
		if (geoMean != null)
			str.append("\tGEOMEAN");
		
		for (int i=0; i<values[0].size(); i++) {
			str.append("\n");
			str.append(df.format(values[0].getX(i)));
			for (DiscretizedFunc v : values)
				str.append("\t").append(df.format(v.getY(i)));
			if (geoMean != null)
				str.append("\t").append(df.format(geoMean.getY(i)));
		}
		
		return str.toString();
	}

	public static void main(String[] args) throws IOException {
		File file = new File("/home/kevin/CyberShake/data_access/2017_10_24-assignment/PSA_PARK_4673_71_1_57.bsa");
		CyberShakePSAFile psa = read(file).get(0);
		System.out.println(psa.asUnitsG());
	}

}
