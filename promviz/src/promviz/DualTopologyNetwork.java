package promviz;

import java.util.List;
import java.util.Map;
import java.util.Set;

import promviz.DEMManager.Prefix;
import promviz.util.DefaultMap;
import promviz.util.Logging;


public class DualTopologyNetwork extends TopologyNetwork {

	TopologyNetwork up;
	TopologyNetwork down;
	
	public DualTopologyNetwork(DEMManager dm, boolean cache) {
		up = new TopologyNetwork(true, dm);
		down = new TopologyNetwork(false, dm);
		if (cache) {
			up.enableCache();
			down.enableCache();
		}
	}
	
	public DualTopologyNetwork() {
		
	}
	
	public static DualTopologyNetwork load(DEMManager dm) {
		DualTopologyNetwork dtn = new DualTopologyNetwork();
		dtn.up = new PagedTopologyNetwork(true, dm);
		dtn.down = new PagedTopologyNetwork(false, dm);
		return dtn;
	}
	
	public void buildPartial(PagedMesh m, List<DEMFile.Sample> newPage) {
		long start = System.currentTimeMillis();
		
		up.buildPartial(m, newPage);
		down.buildPartial(m, newPage);

		Logging.log("@buildPartial: " + (System.currentTimeMillis() - start));
	}

	public Map<Set<Prefix>, Integer> tallyPending(Set<Prefix> allPrefixes) {
		long start = System.currentTimeMillis();
				
		Map<Set<Prefix>, Integer> frontierTotals = new DefaultMap<Set<Prefix>, Integer>() {
			@Override
			public Integer defaultValue(Set<Prefix> _) {
				return 0;
			}
		};
		up.tallyPending(allPrefixes, frontierTotals);
		down.tallyPending(allPrefixes, frontierTotals);

		Logging.log("@tallyPending: " + (System.currentTimeMillis() - start));
		
		return frontierTotals;
	}


}
