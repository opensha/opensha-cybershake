package org.opensha.sha.cybershake.calc.mcer;

import org.opensha.commons.data.Site;
import org.opensha.sha.cybershake.db.CybershakeRun;
import org.opensha.sha.cybershake.db.CybershakeSite;

public class CyberShakeSiteRun extends Site {
	
	private CybershakeSite site;
	private CybershakeRun run;
	
	public CyberShakeSiteRun(CybershakeSite site, CybershakeRun run) {
		super(site.createLocation(), site.short_name);
		this.site = site;
		this.run = run;
	}

	public CybershakeSite getCS_Site() {
		return site;
	}

	public CybershakeRun getCS_Run() {
		return run;
	}

}
