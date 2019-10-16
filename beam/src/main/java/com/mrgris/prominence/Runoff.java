package com.mrgris.prominence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.mrgris.prominence.PathsPipeline.BasinSaddleEdge;
import com.mrgris.prominence.PathsPipeline.PathSearcher;
import com.mrgris.prominence.Prominence.PromFact.Saddle;

/* TODO
 * test trisaddle
 * test multi-convergence
 * test resume of completed trace
 * test explicit eow in traces
 * 
 * true polygonization (needs saddle trace #)
 * bulk processing
 */

public class Runoff {
	
	// what is happening with segments with only 1 point?
	
	public static List<List<Long>> runoff(List<Saddle> seeds, PathSearcher mst) {
		List<Trace> traces = new ArrayList<>();
		Set<Trace> completed = new HashSet<>();
		Set<Trace> clockwise = new HashSet<>();
		for (Saddle s : seeds) {
			long ix = s.s.ix;
			int[] traceNums = mst.anchors.get(ix).toArr();
  	        // anchors may be empty if no runoff resolves in opposite world (or basin saddle conflicted with mst)
			if (traceNums.length == 0) {
				continue;
			}
			if (traceNums.length != 2) {
				throw new RuntimeException();
			}
			if (s.traceNumTowardsP == Edge.TAG_NULL) {
				throw new RuntimeException();
			}
			for (int traceNum : traceNums) {
				if (traceNum == Edge.TAG_NULL) {
					throw new RuntimeException();
				}
			}
			
			int[] anchorTags = new int[] {traceNums[0], traceNums[1]};
			Arrays.sort(anchorTags);
			// (assumes trace #s increase clockwise)
			int traceClockwise;
			if (s.traceNumTowardsP > anchorTags[0] && s.traceNumTowardsP < anchorTags[1]) {
				traceClockwise = anchorTags[0];
			} else {
				traceClockwise = anchorTags[1];				
			}
		
			for (int traceNum : traceNums) {
				BasinSaddleEdge bse = new BasinSaddleEdge(ix, traceNum);
				long anch = mst.get(bse);
				if (anch == PointIndex.NULL) {
					continue;
				}
				
				Trace t = new Trace();
				t.add(ix);
				t.add(anch);

				// this should have been prevented in the construction of the mst backtrace
				if (mst.get(t.head()) == ix) {
					throw new RuntimeException();
				}
				
				traces.add(t);
				if (traceNum == traceClockwise) {
					clockwise.add(t);
				}
			}
		}

		while (completed.size() < traces.size()) {
			for (Trace t : traces) {
				if (completed.contains(t)) {
					continue;
				}
				long cur = t.head();
				
				// check if we've intersected another trace
				// (do this first because the initial state of the traces may already be self-intersecting)
				Trace intersected = null;
				// note: this would be more efficient with an index, but the # of traces is generally small, so meh
				for (Trace other : traces) {
					if (t == other) {
						continue;
					}
					if (other.contains(cur)) {
						if (other.head() == cur && completed.contains(other)) {
							// the intersected trace has already intersected another trace at this same point;
							// this trace is thus the odd one out; keep searching
							continue;
						}
						intersected = other;
						break;
					}
				}
				if (intersected != null) {
					// check if that trace had already intersected another trace; if so, resume
					// the other intersected trace
					if (completed.contains(intersected)) {
						assert t.head() != intersected.head();
						Trace intersectedComplement = null;
						for (Trace other : completed) {
							if (intersected == other) {
								continue;
							}
							if (other.head() == intersected.head() && other.head() != PointIndex.NULL) {
								intersectedComplement = other;
								break;
							}
						}
						if (intersectedComplement != null) {
							completed.remove(intersectedComplement);
						}
					} else {
						completed.add(intersected);
					}
					intersected.trimAfter(cur);
					completed.add(t);
					continue;
				}
				
				// no intersection; keep tracing
				t.add(mst.get(cur));
				// note: null index is added to trace regardless so that intersection detection still
				// works correctly even if the intersection occurs at the last node before EOW
				if (t.head() == PointIndex.NULL) {
					completed.add(t);
				}				
			}
		}
		
		List<List<Long>> ro = new ArrayList<List<Long>>();
		for (Trace t : traces) {
			// remove terminal nulls from final result
			if (t.find(PointIndex.NULL) != -1) {
				t.trimAt(-1);
			}
			ro.add(t.path);
		}
		return ro;
	}

	static class Trace {
		List<Long> path = new ArrayList<Long>();
		Set<Long> set = new HashSet<Long>();
		
		boolean contains(long ix) {
			return set.contains(ix);
		}
		
		void add(long ix) {
			if (contains(ix)) {
				throw new RuntimeException("already in trace " + this.head() + " " + ix);
			}
			path.add(ix);
			set.add(ix);
		}
		
		int len() {
			return path.size();
		}

		int pos(int i) {
			return i < 0 ? len() + i : i;
		}
		
		long get(int i) {
			return path.get(pos(i));
		}
		
		int find(long ix) {
			return path.indexOf(ix);
		}
		
		long head() {
			return get(-1);
		}

		List<Long> trimAt(int i) { // removes path[i] and after
			i = pos(i);
			List<Long> trimmed = new ArrayList<Long>(path.subList(i, path.size()));
			set.removeAll(trimmed); // requires no loop
			path = new ArrayList<Long>(path.subList(0, i));
			return trimmed;
		}
		
		List<Long> trimAfter(long ix) {
			int i = find(ix);
			assert i >= 0;
			return trimAt(i + 1);
		}
	}

}
