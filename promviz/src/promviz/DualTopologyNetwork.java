package promviz;

import java.util.List;
import java.util.Map;
import java.util.Set;

import promviz.DEMManager.Prefix;
import promviz.util.DefaultMap;


public class DualTopologyNetwork extends TopologyNetwork {

	TopologyNetwork up;
	TopologyNetwork down;
	
	public DualTopologyNetwork(DEMManager dm) {
		up = new TopologyNetwork(true, dm);
		down = new TopologyNetwork(false, dm);
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
