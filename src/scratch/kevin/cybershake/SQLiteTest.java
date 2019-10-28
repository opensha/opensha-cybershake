package scratch.kevin.cybershake;

import java.io.File;
import java.io.IOException;

import org.opensha.sha.cybershake.db.CybershakeSite;
import org.opensha.sha.cybershake.db.Cybershake_OpenSHA_DBApplication;
import org.opensha.sha.cybershake.db.DBAccess;
import org.opensha.sha.cybershake.db.SiteInfo2DB;

public class SQLiteTest {
	
	public static void main(String[] args) throws IOException {
		File sqliteFile = new File("/tmp/test.sqlite");
		DBAccess db = Cybershake_OpenSHA_DBApplication.getSQLiteDB(sqliteFile);
		
		SiteInfo2DB sites2db = new SiteInfo2DB(db);
		for (CybershakeSite site : sites2db.getAllSitesFromDB()) {
			System.out.println(site);
		}
		
		db.destroy();
	}

}
