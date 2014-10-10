package promviz;

import java.util.List;
import java.util.Map;
import java.util.Set;

import promviz.DEMManager.Prefix;
import promviz.util.DefaultMap;


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
		dtn.up = TopologyNetwork.load(true, dm);
		dtn.down = TopologyNetwork.load(false, dm);
		return dtn;
	}
	
	public void buildPartial(PagedMesh m, List<DEMFile.Sample> newPage) {
		up.buildPartial(m, newPage);
		down.buildPartial(m, newPage);
	}

	public Map<Set<Prefix>, Integer> tallyPending(Set<Prefix> allPrefixes) {
		Map<Set<Prefix>, Integer> frontierTotals = new DefaultMap<Set<Prefix>, Integer>() {
			@Override
			public Integer defaultValue() {
				return 0;
			}
		};
		up.tallyPending(allPrefixes, frontierTotals);
		down.tallyPending(allPrefixes, frontierTotals);
		return frontierTotals;
	}


}
