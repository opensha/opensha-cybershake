package scratch.kevin.cybershake.dataParse;

import java.io.Closeable;
import java.io.DataInput;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.google.common.io.LittleEndianDataInputStream;

import scratch.kevin.cybershake.dataParse.CyberShakeSeismogramHeader.Component;

public class CyberShakeDurationFile {
	
	private enum Type {
		ARAIS_INTENSITY(0),
		ENERGY_INTEGRAL(1),
		CAV(2),
		DV(3),
		DA(4);
		
		private int flag;
		private Type(int flag) {
			this.flag = flag;
		}
	}
	
	public enum DurationMeasure {
		D5_75(5),
		D5_95(6),
		D20_80(7);
		
		private int flag;
		private DurationMeasure(int flag) {
			this.flag = flag;
		}
	}
	
	private CyberShakeSeismogramHeader header;
	
	private Map<Component, Double> ariasMap;
	private Map<Component, Double> energyMap;
	private Map<Component, Double> cavMap;
	private Table<Component, DurationMeasure, Double> dvTable;
	private Table<Component, DurationMeasure, Double> daTable;
	
	private CyberShakeDurationFile(CyberShakeSeismogramHeader header) {
		this.header = header;
		
		ariasMap = new HashMap<>();
		energyMap = new HashMap<>();
		cavMap = new HashMap<>();
		
		dvTable = HashBasedTable.create();
		daTable = HashBasedTable.create();
	}
	
	public double getAriasIntensity(Component comp) {
		return ariasMap.get(comp);
	}
	
	public double getEnergyIntegral(Component comp) {
		return energyMap.get(comp);
	}
	
	public double getCumulativeAbsoluteVelocity(Component comp) {
		return cavMap.get(comp);
	}
	
	public double getAccelartionDuration(Component comp, DurationMeasure type) {
		return daTable.get(comp, type);
	}
	
	public double getVelocityDuration(Component comp, DurationMeasure type) {
		return dvTable.get(comp, type);
	}

	public static CyberShakeDurationFile read(File file) throws IOException {
		DataInput din = new LittleEndianDataInputStream(new FileInputStream(file));
		return read(din);
	}
	
	public static CyberShakeDurationFile read(DataInput din) throws IOException {
		
		CyberShakeSeismogramHeader header = CyberShakeSeismogramHeader.read(din);
		
		int num = din.readInt()*2; // x2 for each component
		
		CyberShakeDurationFile ret = new CyberShakeDurationFile(header);
		
		for (int i=0; i<num; i++) {
			int tFlag = din.readInt();
			Type type = null;
			for (Type t : Type.values()) {
				if (t.flag == tFlag) {
					type = t;
					break;
				}
			}
			Preconditions.checkNotNull(type, "Type not found for flag %s", tFlag);
			int vFlag = din.readInt();
			DurationMeasure typeValue = null;
			if (type == Type.DA || type == Type.DV) {
				for (DurationMeasure v : DurationMeasure.values()) {
					if (v.flag == vFlag) {
						typeValue = v;
						break;
					}
				}
			}
			int cFlag = din.readInt();
			Component comp;
			switch (cFlag) {
			case 0:
				comp = Component.X;
				break;
			case 1:
				comp = Component.Y;
				break;

			default:
				throw new IllegalStateException("Unknown component flag: "+cFlag);
			}
			
//			System.out.println("Reading\t"+type+"\t"+comp+"\t"+typeValue+"\t");
			
			double value = din.readFloat();
			
			switch (type) {
			case ARAIS_INTENSITY:
				ret.ariasMap.put(comp, value);
				break;
			case ENERGY_INTEGRAL:
				ret.energyMap.put(comp, value);
				break;
			case CAV:
				ret.cavMap.put(comp, value);
				break;
			case DV:
				ret.dvTable.put(comp, typeValue, value);
				break;
			case DA:
				ret.daTable.put(comp, typeValue, value);
				break;

			default:
				break;
			}
		}
		
		if (din instanceof Closeable)
			((Closeable)din).close();
		
		return ret;
	}
	
	private static final DecimalFormat df = new DecimalFormat("0.####");
	
	@Override
	public String toString() {
		StringBuffer str = new StringBuffer("TYPE\tCOMP\tMEASURE\tVALUE");
		
		for (Component comp : ariasMap.keySet())
			str.append("\n").append(Type.ARAIS_INTENSITY.toString()).append("\t").append(comp.toString())
				.append("\tN/A\t").append(df.format(getAriasIntensity(comp)));
		for (Component comp : energyMap.keySet())
			str.append("\n").append(Type.ENERGY_INTEGRAL.toString()).append("\t").append(comp.toString())
				.append("\tN/A\t").append(df.format(getEnergyIntegral(comp)));
		for (Component comp : cavMap.keySet())
			str.append("\n").append(Type.CAV.toString()).append("\t").append(comp.toString())
				.append("\tN/A\t").append(df.format(getCumulativeAbsoluteVelocity(comp)));
		for (Cell<Component, DurationMeasure, Double> cell : dvTable.cellSet())
			str.append("\n").append(Type.DV.toString()).append("\t").append(cell.getRowKey().toString())
				.append("\t").append(cell.getColumnKey().toString()).append("\t").append(df.format(cell.getValue()));
		for (Cell<Component, DurationMeasure, Double> cell : daTable.cellSet())
			str.append("\n").append(Type.DA.toString()).append("\t").append(cell.getRowKey().toString())
				.append("\t").append(cell.getColumnKey().toString()).append("\t").append(df.format(cell.getValue()));
		
		return str.toString();
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		File file = new File("/home/kevin/CyberShake/data_access/2017_10_24-assignment/Duration_OSI_4384_244_12_22.dur");
		CyberShakeDurationFile dur = read(file);
		System.out.println(dur);
	}

}
