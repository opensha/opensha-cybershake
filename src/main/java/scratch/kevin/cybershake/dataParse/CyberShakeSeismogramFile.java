package scratch.kevin.cybershake.dataParse;

import java.io.Closeable;
import java.io.DataInput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.opensha.commons.data.function.ArbitrarilyDiscretizedFunc;
import org.opensha.commons.data.function.DiscretizedFunc;
import org.opensha.commons.data.function.EvenlyDiscretizedFunc;

import com.google.common.io.LittleEndianDataInputStream;

public class CyberShakeSeismogramFile {

	private CyberShakeSeismogramHeader[] headers;
	private DiscretizedFunc xs[];
	private DiscretizedFunc ys[];

	public CyberShakeSeismogramFile(CyberShakeSeismogramHeader[] headers, DiscretizedFunc[] xs,
			DiscretizedFunc[] ys) {
		this.headers = headers;
		this.xs = xs;
		this.ys = ys;
	}
	
	public static CyberShakeSeismogramFile read(File file) throws IOException {
		return read(new FileInputStream(file));
	}
	
	public static CyberShakeSeismogramFile read(InputStream is) throws IOException {
		DataInput din = new LittleEndianDataInputStream(is);
		return read(din);
	}
	
	public static CyberShakeSeismogramFile read(DataInput din) throws IOException {
		List<CyberShakeSeismogramHeader> headers = new ArrayList<>();
		List<DiscretizedFunc> xs = new ArrayList<>();
		List<DiscretizedFunc> ys = new ArrayList<>();
		
		while (true) {
			CyberShakeSeismogramHeader header;
			try {
				header = CyberShakeSeismogramHeader.read(din);
			} catch (Exception e) {
				e.printStackTrace();
				break;
			}
			DiscretizedFunc x = new EvenlyDiscretizedFunc(0d, header.getNt(), header.getDt());
			DiscretizedFunc y = new EvenlyDiscretizedFunc(0d, header.getNt(), header.getDt());
			
			for (int i=0; i<x.size(); i++)
				x.set(i, din.readFloat());
			for (int i=0; i<y.size(); i++)
				y.set(i, din.readFloat());
		}
		
		if (din instanceof Closeable)
			((Closeable)din).close();
		
		return new CyberShakeSeismogramFile(headers.toArray(new CyberShakeSeismogramHeader[0]),
				xs.toArray(new DiscretizedFunc[0]), ys.toArray(new DiscretizedFunc[0]));
	}
	
	public int size() {
		return headers.length;
	}
	
	public CyberShakeSeismogramHeader getHeader(int index) {
		return headers[index];
	}

	public DiscretizedFunc getX(int index) {
		return xs[index];
	}

	public DiscretizedFunc getY(int index) {
		return ys[index];
	}
	
	@Override
	public String toString() {
		StringBuffer str = new StringBuffer();
		
		DecimalFormat df = new DecimalFormat("0.000");
		
		for (int i=0; i<size(); i++) {
			str.append(headers[i].toString());
			str.append("\n").append("Time (s)\tX\tY\t");
			str.append("\n");
			str.append(df.format(xs[i].getX(i))).append("\t");
			str.append(df.format(xs[i].getY(i))).append("\t");
			str.append(df.format(ys[i].getY(i))).append("\t");
		}
		
		return str.toString();
	}

	public static void main(String[] args) throws IOException {
		File file = new File("/data-0/kevin/simulators/catalogs/rundir4983_stitched/Seismogram_LAF_882_1.grm");
		CyberShakeSeismogramFile seis = read(file);
		System.out.println(seis);
	}

}
