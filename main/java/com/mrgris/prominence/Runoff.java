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

	public static List<List<Long>> runoff(HashMap<Long, ArrayList<Long>> seeds, PathSearcher mst) {
		List<Trace> traces = new ArrayList<Trace>();
		Set<Trace> completed = new HashSet<Trace>();
		for (Entry<Long, ArrayList<Long>> e : seeds.entrySet()) {
			long ix = e.getKey();
			for (long anch : e.getValue()) {
				Trace t = new Trace();
				t.add(ix);
				t.add(anch);
				traces.add(t); // TODO could be loop?
			}
		}

		/*
		for (Trace t : traces) {
			int i = 0;
			while (true) {
				long next = mst.get(t.head());
				if (next == PointIndex.NULL) {
					break;
				} else {
					t.add(next);
				}
				
				i++;
				if (i > 10000000) {
					throw new RuntimeException("mst loop!?");
				}
			}
		}
		*/

		/*
		while (completed.size() < traces.size()) {
			int shortest = -1;
			for (Trace t : traces) {
				if (!completed.contains(t) && (shortest == -1 || t.len() < shortest)) {
					shortest = t.len();
				}
			}
			
			for (Trace t : traces) {
				if (completed.contains(t)) {
					continue;
				}
				// let shorter traces catch up so ideally all active traces are the same length
				if (t.len() != shortest) {
					continue;
				}
				
				long cur = t.head();
				
				// check if we've intersected another trace
				Trace intersected = null;
				// TODO replace iteration over traces with ix -> trace(s) index. probably not necessary as # traces is usually small
				for (Trace other : traces) {
					if (t == other) {
						continue;
					}
					if (other.contains(cur)) {
						if (other.head() == cur && completed.contains(other)) {
							// this is a multi-intersection and current trace is odd one out; continue trace
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
							if (other != intersected && other.head() == intersected.head()) {
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
				long next = mst.get(cur);
				if (next == PointIndex.NULL) {
					completed.add(t);
				} else {
					t.add(next);					
				}
			}
		}
		*/
		
		List<List<Long>> ro = new ArrayList<List<Long>>();
		for (Trace t : traces) {
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
				throw new RuntimeException("already in trace " + ix);
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
