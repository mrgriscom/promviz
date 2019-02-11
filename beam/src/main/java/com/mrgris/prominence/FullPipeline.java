package com.mrgris.prominence;

import com.mrgris.prominence.PathsPipeline.PathPipeline;
import com.mrgris.prominence.ProminencePipeline.PromPipeline;
import com.mrgris.prominence.TopologyNetworkPipeline.TopoPipeline;

public class FullPipeline {
	  public static void main(String[] args) {
		  TopoPipeline tp = new TopoPipeline(args);
		  tp.freshRun(true);
		  PromPipeline pp = new PromPipeline(tp);
		  pp.freshRun(true);
		  PathPipeline pthp = new PathPipeline(pp);
		  pthp.freshRun();
		  pthp.p.run();
	  }

}
