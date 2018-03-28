package com.mrgris.prominence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import com.mrgris.prominence.PathsPipeline.PathSearcher;

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
	
	public static List<List<Long>> runoff(HashMap<Long, ArrayList<Long>> seeds, PathSearcher mst) {
		List<Trace> traces = new ArrayList<Trace>();
		Set<Trace> completed = new HashSet<Trace>();
		for (Entry<Long, ArrayList<Long>> e : seeds.entrySet()) {
			long ix = e.getKey();
			for (long anch : e.getValue()) {
				Trace t = new Trace();
				t.add(ix);
				t.add(anch);

				// weirdness can occur at the edge of the data area where peaks are 'orphaned' and so the
				// mst networks actually cross. adding 'end-of-world' saddles should fix, but keep this
				// safeguard here
				if (mst.get(t.head()) == ix) {
					continue;
				}
				
				traces.add(t);
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

	// TODO investigate continued need for this?
	static final long END_OF_WORLD = PointIndex.NULL - 1; // sentinel
}
