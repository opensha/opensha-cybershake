package org.opensha.sha.cybershake.db;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.opensha.commons.util.ClassUtils;
import org.opensha.sha.imr.param.IntensityMeasureParams.DurationTimeInterval;
import org.opensha.sha.imr.param.OtherParams.Component;
import org.opensha.sha.imr.param.OtherParams.ComponentParam;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;

public class CybershakeIM implements Comparable<CybershakeIM> {
	
	private static Table<CyberShakeComponent, Double, Integer> saIDsMap;
	
	static {
		// validate these mappings with the main method below
		saIDsMap = HashBasedTable.create();
		saIDsMap.put(CyberShakeComponent.GEOM_MEAN, 0.1, 99);
		saIDsMap.put(CyberShakeComponent.GEOM_MEAN, 0.2, 94);
		saIDsMap.put(CyberShakeComponent.GEOM_MEAN, 0.5, 88);
		saIDsMap.put(CyberShakeComponent.GEOM_MEAN, 1d, 86);
		saIDsMap.put(CyberShakeComponent.GEOM_MEAN, 2d, 26);
		saIDsMap.put(CyberShakeComponent.GEOM_MEAN, 3d, 21);
		saIDsMap.put(CyberShakeComponent.GEOM_MEAN, 4d, 16);
		saIDsMap.put(CyberShakeComponent.GEOM_MEAN, 5d, 11);
		saIDsMap.put(CyberShakeComponent.GEOM_MEAN, 7.5, 6);
		saIDsMap.put(CyberShakeComponent.GEOM_MEAN, 10d, 1);
		
		saIDsMap.put(CyberShakeComponent.RotD50, 0.01, 202);
		saIDsMap.put(CyberShakeComponent.RotD50, 0.02, 200);
		saIDsMap.put(CyberShakeComponent.RotD50, 0.05, 194);
		saIDsMap.put(CyberShakeComponent.RotD50, 0.1, 175);
		saIDsMap.put(CyberShakeComponent.RotD50, 0.2, 174);
		saIDsMap.put(CyberShakeComponent.RotD50, 0.5, 173);
		saIDsMap.put(CyberShakeComponent.RotD50, 1d, 169);
		saIDsMap.put(CyberShakeComponent.RotD50, 2d, 167);
		saIDsMap.put(CyberShakeComponent.RotD50, 3d, 162);
		saIDsMap.put(CyberShakeComponent.RotD50, 4d, 160);
		saIDsMap.put(CyberShakeComponent.RotD50, 5d, 158);
		saIDsMap.put(CyberShakeComponent.RotD50, 7.5, 154);
		saIDsMap.put(CyberShakeComponent.RotD50, 10d, 152);
		
		saIDsMap.put(CyberShakeComponent.RotD100, 0.01, 203);
		saIDsMap.put(CyberShakeComponent.RotD100, 0.02, 201);
		saIDsMap.put(CyberShakeComponent.RotD100, 0.05, 195);
		saIDsMap.put(CyberShakeComponent.RotD100, 0.1, 172);
		saIDsMap.put(CyberShakeComponent.RotD100, 0.2, 171);
		saIDsMap.put(CyberShakeComponent.RotD100, 0.5, 170);
		saIDsMap.put(CyberShakeComponent.RotD100, 1d, 168);
		saIDsMap.put(CyberShakeComponent.RotD100, 2d, 151);
		saIDsMap.put(CyberShakeComponent.RotD100, 3d, 146);
		saIDsMap.put(CyberShakeComponent.RotD100, 4d, 144);
		saIDsMap.put(CyberShakeComponent.RotD100, 5d, 142);
		saIDsMap.put(CyberShakeComponent.RotD100, 7.5, 138);
		saIDsMap.put(CyberShakeComponent.RotD100, 10d, 136);
	}
	
	public static final CybershakeIM PGV = new CybershakeIM(185, IMType.SA, -1, "cm per sec", CyberShakeComponent.RotD50);
	
	public static CybershakeIM getSA(CyberShakeComponent comp, double period) {
		Integer id = saIDsMap.get(comp, period);
		Preconditions.checkNotNull(id, "ID not yet hardcoded for comp=%s, period=%s", comp, period);
		return new CybershakeIM(id, IMType.SA, period, "cm per sec squared", comp);
	}
	
	private static Table<CyberShakeComponent, DurationTimeInterval, Integer> durIDsMap;
	private static Table<CyberShakeComponent, DurationTimeInterval, Integer> velDurIDsMap;
	
	static {
		// validate these mappings with the main method below
		durIDsMap = HashBasedTable.create();
		velDurIDsMap = HashBasedTable.create();
		durIDsMap.put(CyberShakeComponent.X, DurationTimeInterval.INTERVAL_5_75, 176);
		durIDsMap.put(CyberShakeComponent.X, DurationTimeInterval.INTERVAL_5_95, 177);
		velDurIDsMap.put(CyberShakeComponent.X, DurationTimeInterval.INTERVAL_5_75, 178);
		velDurIDsMap.put(CyberShakeComponent.X, DurationTimeInterval.INTERVAL_5_95, 179);
		
		durIDsMap.put(CyberShakeComponent.Y, DurationTimeInterval.INTERVAL_5_75, 180);
		durIDsMap.put(CyberShakeComponent.Y, DurationTimeInterval.INTERVAL_5_95, 181);
		velDurIDsMap.put(CyberShakeComponent.Y, DurationTimeInterval.INTERVAL_5_75, 182);
		velDurIDsMap.put(CyberShakeComponent.Y, DurationTimeInterval.INTERVAL_5_95, 183);
	}
	
	public static CybershakeIM getDuration(CyberShakeComponent comp, DurationTimeInterval interval) {
		Integer id = durIDsMap.get(comp, interval);
		Preconditions.checkNotNull(id, "ID not yet hardcoded for comp=%s, interval=%s", comp, interval);
		IMType type;
		switch (interval) {
		case INTERVAL_5_75:
			type = IMType.ACCEL_DUR_5_75;
			break;
		case INTERVAL_5_95:
			type = IMType.ACCEL_DUR_5_95;
			break;

		default:
			throw new IllegalStateException("Duration interval not supported: "+interval);
		}
		return new CybershakeIM(id, type, Double.NaN, "seconds", comp);
	}
	
	public static CybershakeIM getVelDuration(CyberShakeComponent comp, DurationTimeInterval interval) {
		Integer id = velDurIDsMap.get(comp, interval);
		Preconditions.checkNotNull(id, "ID not yet hardcoded for comp=%s, interval=%s", comp, interval);
		IMType type;
		switch (interval) {
		case INTERVAL_5_75:
			type = IMType.VEL_DUR_5_75;
			break;
		case INTERVAL_5_95:
			type = IMType.VEL_DUR_5_95;
			break;

		default:
			throw new IllegalStateException("Duration interval not supported: "+interval);
		}
		return new CybershakeIM(id, type, Double.NaN, "seconds", comp);
	}
	
	public enum IMType implements DBField {
		SA("spectral acceleration", "SA"),
		PGV("PGV", "PGV"),
		VEL_DUR_5_95("velocity significant duration, 5% to 95%", "VelDur5-95"),
		VEL_DUR_5_75("velocity significant duration, 5% to 75%", "VelDur5-75"),
		ACCEL_DUR_5_95("acceleration significant duration, 5% to 95%", "Dur5-95"),
		ACCEL_DUR_5_75("acceleration significant duration, 5% to 75%", "Dur5-75");
		
		private String dbName, shortName;
		private IMType(String dbName, String shortName) {
			this.dbName = dbName;
			this.shortName = shortName;
		}
		@Override
		public String getDBName() {
			return dbName;
		}
		@Override
		public String getShortName() {
			return shortName;
		}
		@Override
		public String toString() {
			return getShortName();
		}
	}
	
	public enum CyberShakeComponent implements DBField {
		GEOM_MEAN("geometric mean", "GEOM", Component.AVE_HORZ, Component.GMRotI50),
		X("X", "X", Component.RANDOM_HORZ),
		Y("Y", "Y", Component.RANDOM_HORZ),
		RotD100("RotD100", "RotD100", Component.RotD100),
		RotD50("RotD50", "RotD50", Component.RotD50);

		private String dbName, shortName;
		// supported GMPE components
		private Component[] gmpeComponents;
		private CyberShakeComponent(String dbName, String shortName, Component... gmpeComponents) {
			this.dbName = dbName;
			this.shortName = shortName;
			this.gmpeComponents = gmpeComponents;
			Preconditions.checkNotNull(gmpeComponents);
		}
		@Override
		public String getDBName() {
			return dbName;
		}
		@Override
		public String getShortName() {
			return shortName;
		}
		@Override
		public String toString() {
			return getShortName();
		}
		public Component[] getGMPESupportedComponents() {
			return gmpeComponents;
		}
		public Component getSupportedComponent(ComponentParam compParam) {
			return getSupportedComponent(compParam.getConstraint().getAllowedValues());
		}
		public Component getSupportedComponent(List<Component> components) {
			for (Component gmpeComponent : gmpeComponents)
				if (components.contains(gmpeComponent))
					return gmpeComponent;
			// no supported
			return null;
		}
		public boolean isComponentSupported(Component component) {
			for (Component supported : gmpeComponents)
				if (supported == component)
					return true;
			return false;
		}
	}
	
	private interface DBField {
		public String getDBName();
		public String getShortName();
	}
	
	public static <E extends Enum<E>> E fromDBField(String dbName, Class<E> clazz) {
		for (E e : clazz.getEnumConstants()) {
			Preconditions.checkState(e instanceof DBField);
			DBField db = (DBField)e;
			if (db.getDBName().equals(dbName.trim()))
				return e;
		}
		throw new IllegalStateException("DB field for type "
				+ClassUtils.getClassNameWithoutPackage(clazz)+" not found: "+dbName);
	}
	
	public static <E extends Enum<E>> E fromShortName(String shortName, Class<E> clazz) {
		for (E e : clazz.getEnumConstants()) {
			Preconditions.checkState(e instanceof DBField);
			DBField db = (DBField)e;
			if (db.getShortName().toLowerCase().equals(shortName.trim().toLowerCase()))
				return e;
		}
		throw new IllegalArgumentException("Short name for type "
				+ClassUtils.getClassNameWithoutPackage(clazz)+" not found: "+shortName);
	}
	
	public static <E extends Enum<E>> List<String> getShortNames(Class<E> clazz) {
		List<String> names = Lists.newArrayList();
		for (E e : clazz.getEnumConstants()) {
			Preconditions.checkState(e instanceof DBField);
			DBField db = (DBField)e;
			names.add(db.getShortName());
		}
		return names;
	}
	
	private int id;
	private IMType measure;
	private double val;
	private String units;
	private CyberShakeComponent component;
	
	public CybershakeIM(int id, IMType measure, double val, String units, CyberShakeComponent component) {
		this.id = id;
		this.measure = measure;
		this.val = val;
		this.units = units;
		this.component = component;
	}

	public int getID() {
		return id;
	}

	public IMType getMeasure() {
		return measure;
	}
	
	public CyberShakeComponent getComponent() {
		return component;
	}

	public double getVal() {
		return val;
	}

	public String getUnits() {
		return units;
	}
	
	public String toString() {
		return "ID: "+id+", "+this.measure+" ("+component+"): "+this.val+" ("+this.units+")";
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
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
		CybershakeIM other = (CybershakeIM) obj;
		if (id != other.id)
			return false;
		return true;
	}

	public int compareTo(CybershakeIM im) {
		int ret = measure.compareTo(im.measure);
		if (ret != 0)
			return ret;
		ret = component.compareTo(im.component);
		if (ret != 0)
			return ret;
		return Double.compare(val, im.val);
	}
	
	public static CybershakeIM fromResultSet(ResultSet rs) throws SQLException {
		int id = rs.getInt("IM_Type_ID");
		String measureStr = rs.getString("IM_Type_Measure");
		IMType measure = fromDBField(measureStr, IMType.class);
		Double value = rs.getDouble("IM_Type_Value");
		String units = rs.getString("Units");
		String componentStr = rs.getString("IM_Type_Component");
		CyberShakeComponent component = fromDBField(componentStr, CyberShakeComponent.class);
		return new CybershakeIM(id, measure, value, units, component);
	}
	
	public static void main(String[] args) {
		// validate hardcoded IDs
		DBAccess db = Cybershake_OpenSHA_DBApplication.getDB(Cybershake_OpenSHA_DBApplication.PRODUCTION_HOST_NAME);
		
		HazardCurve2DB curves2db = new HazardCurve2DB(db);

		for (Cell<CyberShakeComponent, Double, Integer> cell : saIDsMap.cellSet()) {
			CybershakeIM im = curves2db.getIMFromID(cell.getValue());
			Preconditions.checkState(cell.getRowKey().equals(im.component),
					"Component mismatch for %s. Hardcoded is %s, DB is %s", im.getID(), cell.getRowKey(), im.getComponent());
			int p1 = (int)Math.round(cell.getColumnKey()*100d);
			int p2 = (int)Math.round(im.val*100d);
			Preconditions.checkState(p1 == p2,
					"Period mismatch for %s. Hardcoded is %s, DB is %s", im.getID(), cell.getColumnKey(), im.val);
		}
		for (Cell<CyberShakeComponent, DurationTimeInterval, Integer> cell : durIDsMap.cellSet()) {
			CybershakeIM im = curves2db.getIMFromID(cell.getValue());
			Preconditions.checkState(cell.getRowKey().equals(im.component),
					"Component mismatch for %s. Hardcoded is %s, DB is %s", im.getID(), cell.getRowKey(), im.getComponent());
			CybershakeIM oIM = getDuration(cell.getRowKey(), cell.getColumnKey());
			Preconditions.checkState(im.getMeasure() == oIM.getMeasure(),
					"IMType mismatch for %s. Hardcoded is %s, DB is %s", im.getID(), oIM.getMeasure(), im.getMeasure());
		}
		for (Cell<CyberShakeComponent, DurationTimeInterval, Integer> cell : velDurIDsMap.cellSet()) {
			CybershakeIM im = curves2db.getIMFromID(cell.getValue());
			Preconditions.checkState(cell.getRowKey().equals(im.component),
					"Component mismatch for %s. Hardcoded is %s, DB is %s", im.getID(), cell.getRowKey(), im.getComponent());
			CybershakeIM oIM = getVelDuration(cell.getRowKey(), cell.getColumnKey());
			Preconditions.checkState(im.getMeasure() == oIM.getMeasure(),
					"IMType mismatch for %s. Hardcoded is %s, DB is %s", im.getID(), oIM.getMeasure(), im.getMeasure());
		}
		
		db.destroy();
	}
}
