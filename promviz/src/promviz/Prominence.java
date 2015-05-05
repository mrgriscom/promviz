package promviz;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;

import promviz.Prominence.Backtrace.BacktracePruner;
import promviz.debug.Harness;
import promviz.dem.DEMFile;
import promviz.util.DefaultMap;
import promviz.util.Logging;
import promviz.util.ReverseComparator;
import promviz.util.SaneIterable;
import promviz.util.WorkerPoolDebug;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/* goals
 * 
 * determine base-level prominence - prom, saddle, threshold, and path
 * write out MST
 * 
 * next:
 * mst dump
 * how to handle global max?
 */

public class Prominence {

	static final int COALESCE_STEP = 2;
	
	public static interface OnProm {
		void onprom(PromInfo pi);
	}
	
	public static void promSearch(List<DEMFile> DEMs, boolean up, double cutoff) {
		Map<Prefix, Set<DEMFile>> coverage = PagedElevGrid.partitionDEM(DEMs);
		
		FileUtil.ensureEmpty(FileUtil.PHASE_PROMTMP, up);
		FileUtil.ensureEmpty(FileUtil.PHASE_MST, up);
		
		PromSearch searcher = new PromSearch(Main.NUM_WORKERS, coverage, up, cutoff, new OnProm() {
			public void onprom(PromInfo pi) {
				Harness.outputPromPoint(pi);
				
				// note: if we ever have varying cutoffs this will take extra care
				// to make sure we always capture the parent/next highest peaks for
				// peaks right on the edge of the lower-cutoff region
				// TODO: cache out prom values and threshold points for processing in later phases?
			}
		});
		searcher.baseCellSearch();
		while (searcher.needsCoalesce()) {
			searcher.coalesce();
		}
	}
	
	static class ChunkInput {
		Prefix p;
		boolean up;
		boolean baseLevel;
		boolean finalLevel;
		double cutoff;
		Map<Prefix, Set<DEMFile>> coverage;
	}
	
	static class ChunkOutput {
		Prefix p;
		List<PromInfo> proms;
		Collection<Front> fronts;
		// mst?
	}
	
//	static class PromSearch extends WorkerPool<ChunkInput, ChunkOutput> {
	static class PromSearch extends WorkerPoolDebug<ChunkInput, ChunkOutput> {

		int numWorkers;
		Map<Prefix, Set<DEMFile>> coverage;
		boolean up;
		double cutoff;
		OnProm onprom;

		int level;
		Set<Prefix> chunks;
		
		public PromSearch(int numWorkers, Map<Prefix, Set<DEMFile>> coverage, boolean up, double cutoff, OnProm onprom) {
			this.numWorkers = numWorkers;
			this.coverage = coverage;
			this.up = up;
			this.cutoff = cutoff;
			this.onprom = onprom;
		}

		public void baseCellSearch() {
			this.level = TopologyBuilder.CHUNK_SIZE_EXP;
			this.chunks = TopologyBuilder.enumerateChunks(coverage);
			Logging.log(chunks.size() + " network chunks");
			
			launch(numWorkers, Iterables.transform(chunks, new Function<Prefix, ChunkInput>() {
				public ChunkInput apply(Prefix p) {
					return makeInput(p, up, cutoff);
				}
			}));
		}
		
		public boolean needsCoalesce() {
			return this.chunks.size() > 1;
		}
		
		public void coalesce() {
			this.level += COALESCE_STEP;
			this.chunks = TopologyBuilder.clusterPrefixes(this.chunks, this.level);
			Logging.log(chunks.size() + " chunks @ L" + level);
			
			launch(numWorkers, Iterables.transform(chunks, new Function<Prefix, ChunkInput>() {
				public ChunkInput apply(Prefix p) {
					return makeInput(p, up, cutoff);
				}
			}));
		}
		
		public ChunkOutput process(ChunkInput input) {
			return new ChunkProcessor(input).build();
		}

		public void postprocess(int i, ChunkOutput output) {
			for (PromInfo pi : output.proms) {
				this.onprom.onprom(pi);
			}
			writeFronts(output.p, output.fronts, up);
			
			Logging.log((i+1) + " " + output.p);
		}

		public static void writeFronts(Prefix prefix, Collection<Front> fronts, final boolean up) {
			if (fronts.isEmpty()) {
				return;
			}
			
			try {
				DataOutputStream out = new DataOutputStream(new FileOutputStream(FileUtil.segmentPath(up, prefix, FileUtil.PHASE_PROMTMP), true));
				for (Front f : fronts) {
					f.write(out);
				}
				out.close();
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}				
		}
		
		ChunkInput makeInput(Prefix p, boolean up, double cutoff) {
			ChunkInput ci = new ChunkInput();
			ci.p = p;
			ci.up = up;
			ci.baseLevel = (p.res == TopologyBuilder.CHUNK_SIZE_EXP);
			ci.finalLevel = (chunks.size() == 1);
			ci.cutoff = cutoff;
			ci.coverage = (HashMap)((HashMap)coverage).clone();
			return ci;
		}
	}
	
	static class ChunkProcessor {
		ChunkInput input;
		Prefix prefix;
		boolean up;
		PagedElevGrid mesh;

		Set<Front> fronts;
		// the highest saddle directly connecting two fronts (both forward and reverse mapping)
		Map<MeshPoint, Set<Front>> connectorsBySaddle;
		Map<Set<Front>, MeshPoint> connectorsByFrontPair;
		// front pairs that can and must be coalesced
		Set<FrontMerge> pendingMerges;
		
		public ChunkProcessor(ChunkInput input) {
			this.input = input;
			this.prefix = input.p;
			this.up = input.up;
			this.mesh = TopologyBuilder._createMesh(input.coverage);
		}
		
		boolean inChunk(long ix) {
			return prefix.isParent(ix);	
		}
				
		public ChunkOutput build() {
			List<PromInfo> proms = new ArrayList<PromInfo>();

			load();
			
			Logging.log("before " + fronts.size());
			while (pendingMerges.size() > 0) {
				Iterator<FrontMerge> it = pendingMerges.iterator();
				FrontMerge toMerge = it.next();
				it.remove();
				
				PromInfo pi = mergeFronts(toMerge, input.cutoff);
				if (pi != null) {
					proms.add(pi);
				}
			}
			Logging.log("after " + fronts.size());
			
			if (input.finalLevel) {
				finalizeRemaining(proms);
				fronts.removeAll(fronts);
			}

			// TODO make MST for chunk
			for (Front f : fronts) {
				f.prune();
			}

			ChunkOutput output = new ChunkOutput();
			output.p = prefix;
			output.proms = proms;
			output.fronts = fronts;
			return output;
		}
		
		static class FrontMerge {
			Front child;
			Front parent;
			
			FrontMerge(Front child, Front parent) {
				this.child = child;
				this.parent = parent;
			}
			
			public boolean equals(Object o) {
				FrontMerge fm = (FrontMerge)o;
				return this.parent.equals(fm.parent) && this.child.equals(fm.child);
			}
			
			public int hashCode() {
				return parent.hashCode() ^ child.hashCode();
			}
		}
		
		public void load() {
			fronts = new HashSet<Front>();
			connectorsBySaddle = new HashMap<MeshPoint, Set<Front>>();
			connectorsByFrontPair = new HashMap<Set<Front>, MeshPoint>();
			pendingMerges = new HashSet<FrontMerge>();
			
			if (input.baseLevel) {
				loadForBase();
			} else {
				loadForCoalesce();
			}
			
			// map saddles to fronts that that saddle appears in
			Map<MeshPoint, Set<Front>> saddlesToFronts = new DefaultMap<MeshPoint, Set<Front>>() {
				public Set<Front> defaultValue(MeshPoint key) {
					return new HashSet<Front>();
				}
			};
			for (Front f : fronts) {
				for (MeshPoint saddle : f.set){
					saddlesToFronts.get(saddle).add(f);
				}
			}
			// map sets of fronts to the saddles that appear in those fronts
			// these saddles only ever appear together in the same fronts -- i.e., the intersection of the two fronts
			Map<Set<Front>, List<MeshPoint>> frontPairsToSaddles = new DefaultMap<Set<Front>, List<MeshPoint>>() {
				public List<MeshPoint> defaultValue(Set<Front> key) {
					return new ArrayList<MeshPoint>();
				}
			};
			for (Entry<MeshPoint, Set<Front>> e : saddlesToFronts.entrySet()) {
				MeshPoint saddle = e.getKey();
				Set<Front> fs = e.getValue();
				assert fs.size() == 1 || fs.size() == 2;
				if (fs.size() != 2) {
					continue;
				}
				frontPairsToSaddles.get(fs).add(saddle);
			}
			// for every front intersection, eliminate all but the highest saddle and store in the connector indexes
			for (Entry<Set<Front>, List<MeshPoint>> e : frontPairsToSaddles.entrySet()) {
				Set<Front> fp = e.getKey();
				List<MeshPoint> saddles = e.getValue();
				MeshPoint highest = Collections.max(saddles, fp.iterator().next().c);
				for (MeshPoint saddle : saddles) {
					if (saddle == highest) {
						continue;
					}
					for (Front f : fp) {
						f.remove(saddle);
					}
				}
				connectorsPut(highest, fp);
			}
			// pre-fill all pending merges for adjacent fronts
			for (Front f : fronts) {
				newPendingMerge(f);
			}
			
			Logging.log("" + pendingMerges.size());
		}

		void loadForBase() {
			List<Edge> edges = Lists.newArrayList(FileUtil.loadEdges(up, prefix, FileUtil.PHASE_RAW));
			
			// load necessary DEM pages for graph
			final Set<Prefix> pages = new HashSet<Prefix>();
			processEdges(edges, new EdgeProcessor() {
				public void process(long summitIx, long saddleIx) {
					pages.add(PagedElevGrid.segmentPrefix(summitIx));
					pages.add(PagedElevGrid.segmentPrefix(saddleIx));
				}
			});
			mesh.bulkLoadPage(pages);

			// build atomic fronts (summit + immediate saddles)
			final Map<MeshPoint, Front> frontsByPeak = new DefaultMap<MeshPoint, Front>() {
				public Front defaultValue(MeshPoint key) {
					return new Front(key, up);
				}
			};
			processEdges(edges, new EdgeProcessor() {
				public void process(long summitIx, long saddleIx) {
					MeshPoint summit = mesh.get(summitIx);
					MeshPoint saddle = mesh.get(saddleIx);
					frontsByPeak.get(summit).add(saddle);
				}
			});
			fronts.addAll(frontsByPeak.values());
		}

		static interface EdgeProcessor {
			void process(long summitIx, long saddleIx);
		}
		
		void processEdges(Iterable<Edge> edges, EdgeProcessor ep) {
			for (Edge e : edges) {
				if (inChunk(e.a)) {
					ep.process(e.a, e.saddle);
				}
				if (!e.pending() && inChunk(e.b)) {
					ep.process(e.b, e.saddle);
				}
			}
		}

		void loadForCoalesce() {
			for (Prefix subChunk : prefix.children(COALESCE_STEP)) {
				for (Front f : FileUtil.loadFronts(up, subChunk, FileUtil.PHASE_PROMTMP)) {
					fronts.add(f);
				}
				// could delete chunk file now
			}
		}
		
		void newPendingMerge(Front f) {
			Front primary = primaryFront(f);
			if (primary != null && f.c.compare(primary.peak, f.peak) > 0) {
				pendingMerges.add(new FrontMerge(f, primary));
			}
		}
		
		Front primaryFront(Front f) {
			return connectingFront(f, f.first());
		}
		
		Front connectingFront(Front f, MeshPoint saddle) {
			Set<Front> fp = connectorsBySaddle.get(saddle);
			if (fp == null) {
				return null;
			}
			for (Front other : fp) {
				if (!other.equals(f)) {
					return other;
				}
			}
			throw new RuntimeException("can't happen");
		}
		
		Set<Front> frontPair(Front a, Front b) {
			Set<Front> pair = new HashSet<Front>();
			pair.add(a);
			pair.add(b);
			return pair;
		}
		
		PromInfo mergeFronts(FrontMerge fm, double cutoff) {
			Front parent = fm.parent;
			Front child = fm.child;
			
			// unlist child front, merge into parent, and remove connection between the two
			fronts.remove(child);
			MeshPoint saddle = child.pop();
			boolean newParentMerge = parent.mergeFrom(child, saddle);
			connectorsClear(saddle, frontPair(parent, child));
			
			// update for all fronts adjacent to child (excluding 'parent')
			for (MeshPoint subsaddle : child.set) {
				Front neighbor = connectingFront(child, subsaddle);
				if (neighbor == null) {
					continue;
				}
				// must test this before manipulating 'connectors'
				boolean mergesTowardsChild = child.equals(primaryFront(neighbor));
				
				Set<Front> childAndNeighbor = frontPair(child, neighbor);
				Set<Front> parentAndNeighbor = frontPair(parent, neighbor);
				connectorsClear(subsaddle, childAndNeighbor);
				MeshPoint existingParentNeighborSaddle = connectorsByFrontPair.get(parentAndNeighbor);
				if (existingParentNeighborSaddle == null) {
					connectorsPut(subsaddle, parentAndNeighbor);			
				} else {
					// parent was already connected to 'neighbor' before child merge -- only keep the
					// better of the two saddles (original via 'parent' or new via 'child')
					MeshPoint redundantSaddle;
					if (parent.c.compare(existingParentNeighborSaddle, subsaddle) > 0) {
						redundantSaddle = subsaddle;
					} else {
						redundantSaddle = existingParentNeighborSaddle;
						connectorsClear(existingParentNeighborSaddle, parentAndNeighbor);
						connectorsPut(subsaddle, parentAndNeighbor);
					}
					parent.remove(redundantSaddle);
					neighbor.remove(redundantSaddle);
				}				
				
				// neighbor may now be mergeable with parent
				// must do this after updating 'connectors'
				if (mergesTowardsChild) {
					FrontMerge existingMerge = new FrontMerge(neighbor, child);
					if (pendingMerges.contains(existingMerge)) {
						// was mergeable with 'child'; transfer merge directly
						pendingMerges.remove(existingMerge);
						pendingMerges.add(new FrontMerge(neighbor, parent));
					} else {
						// may be mergeable with newly merged parent+child
						newPendingMerge(neighbor);
					}
				}
			}

			// parent's primary front has changed
			if (newParentMerge) {
				// parent could not have had an existing pending merge, so no need to remove anything
				newPendingMerge(parent);
			}

			PromInfo pi = new PromInfo(up, child.peak, saddle);
			if (pi.prominence() >= cutoff) {
				pi.finalizeBackward(parent);
				return pi;
			} else {
				return null;
			}
		}
		
		void connectorsPut(MeshPoint saddle, Set<Front> fronts) {
			assert !connectorsByFrontPair.containsKey(fronts);
			connectorsBySaddle.put(saddle, fronts);
			connectorsByFrontPair.put(fronts, saddle);
		}
		
		void connectorsClear(MeshPoint saddle, Set<Front> fronts) {
			assert fronts.equals(connectorsBySaddle.get(saddle));
			connectorsBySaddle.remove(saddle);
			connectorsByFrontPair.remove(fronts);
		}
		
		void finalizeRemaining(List<PromInfo> proms) {
			Logging.log("finalizing remaining");
			for (Front f : fronts) {
				if (f.first() == null) {
					// global max
					continue;
				}

				PromInfo pi = new PromInfo(up, f.peak, f.first());
				pi.min_bound_only = true;
				if (pi.prominence() >= input.cutoff) {
					pi.path = pathToUnknown(f);
					proms.add(pi);
				}
			}
		}
		
		List<Long> pathToUnknown(Front f) {
			List<Point> path = new ArrayList<Point>();
			Point start = f.peak;
			while (f != null) {
				List<Point> segment = Lists.newArrayList(f.bt.getAtoB(start, f.first()));
				if (!path.isEmpty()) {
					segment = segment.subList(1, segment.size());
				}
				path.addAll(segment);
				
				start = f.first();
				f = primaryFront(f);
			}

			List<Long> ixPath = new ArrayList<Long>();
			for (Point p : path) {
				ixPath.add(p.ix);
			}
			return Lists.reverse(ixPath);
		}
	}
	
	public static class PromInfo {
		public boolean up;
		public MeshPoint p;
		public MeshPoint saddle;
		public boolean global_max;
		public boolean min_bound_only;
		public List<Long> path;
		public double thresholdFactor = -1;
		
		public PromInfo(boolean up, MeshPoint peak, MeshPoint saddle) {
			this.up = up;
			this.p = peak;
			this.saddle = saddle;
		}
		
		public double prominence() {
			return Math.abs(p.elev - saddle.elev);
		}
		
		public void finalizeForward(Front front, MeshPoint horizon) {
			Path _ = new Path(front.bt.getAtoB(horizon, this.p), this.p);
			this.path = _.path;
			this.thresholdFactor = _.thresholdFactor;
		}

		public void finalizeBackward(Front front) {
			Point thresh = front.searchThreshold(this.p, this.saddle);			
			Path _ = new Path(front.bt.getAtoB(thresh, this.p), this.p);
			this.path = _.path;
			this.thresholdFactor = _.thresholdFactor;
		}
		
		public void _finalizeDumb() {
			this.path = new ArrayList<Long>();
			this.path.add(this.p.ix);
			this.path.add(this.saddle.ix);
		}
		
	}

	static class Path {
		List<Long> path;
		double thresholdFactor = -1;
		
		public Path(Iterable<Point> path, Point ref) {
			this.path = new ArrayList<Long>();
			Point[] endSeg = new Point[2];
			int i = 0;
			for (Point p : path) {
				this.path.add(p.ix);
				
				if (i < 2) {
					endSeg[i] = p;
				}
				i++;
			}

			if (ref != null && this.path.size() >= 2) {
				Point last = endSeg[0];
				Point nextToLast = endSeg[1];
				thresholdFactor = (ref.elev - nextToLast.elev) / (last.elev - nextToLast.elev);
			}
		}
	}
	
	static class Backtrace {
		Map<Point, Point> backtrace; // can't just store ixs since searchThreshold() needs elev values
			// (because the saddle maps only store entries above the prominence cutoff)
		Point root;
		
		public Backtrace() {
			this.backtrace = new HashMap<Point, Point>();
		}
		
		public void add(Point p, Point parent) {
			if (parent == null) {
				root = p;
			} else {
				backtrace.put(p, parent);
			}
		}
		
		public Point get(Point p) {
			Point parent = backtrace.get(p);
			if (parent == null && !p.equals(root)) {
				throw new RuntimeException("point not loaded");
			}
			return parent;
		}
		
//		public boolean isLoaded(Point p) {
//			return backtrace.containsKey(p) || p.equals(root);
//		}
		
		public int size() {
			return backtrace.size();
		}
		
		public Iterable<Point> mergeFrom(Backtrace other, MeshPoint saddle) {
			// this pruning is causing errors for some reason
//			for (Iterator<Point> it = other.backtrace.keySet().iterator(); it.hasNext(); ) {
//				Point from = it.next();
//				if (this.backtrace.containsKey(from) && !from.equals(saddle)) {
//					this.backtrace.remove(from);
//					it.remove();
//				}					
//			}
			for (Entry<Point, Point> e : other.backtrace.entrySet()) {
				Point from = e.getKey();
				Point to = e.getValue();
				if (from.equals(saddle)) {
					continue;
				}
				this.add(from, to);
			}
			
			List<Point> toReverse = Lists.newArrayList(other.trace(saddle)); 
			Point from = null;
			for (Point to : toReverse) {
				if (from != null) {
					this.add(to, from);
				}
				from = to;
			}
			return toReverse.subList(1, toReverse.size() - 1); // exclude connecting saddle and old front peak
		}
		
		public Iterable<Point> trace(final Point start) {
			return new SaneIterable<Point>() {
				Point cur = null;
				public Point genNext() {
					cur = (cur == null ? start : get(cur));
					if (cur == null) {
						throw new NoSuchElementException();
					}
					return cur;
				}
			};
		}
		
		static interface BacktracePruner {
			void markPoint(Point p);
			void prune();
		}
		
		public BacktracePruner pruner() {
			final Set<Point> backtraceKeep = new HashSet<Point>();

			return new BacktracePruner() {
				public void markPoint(Point p) {
					for (Point t : trace(p)) {
						boolean newItem = backtraceKeep.add(t);
						if (!newItem) {
							break;
						}
					}
				}

				public void prune() {
					for (Iterator<Point> it = backtrace.keySet().iterator(); it.hasNext(); ) {
						Point p = it.next();
						if (!backtraceKeep.contains(p)) {
					        it.remove();
					    }
					}
				}
			};
		}
		
		public Iterable<Point> getAtoB(Point pA, Point pB) {
			if (pB == null) {
				return trace(pA);
			}
			
			List<Point> fromA = new ArrayList<Point>();
			List<Point> fromB = new ArrayList<Point>();
			Set<Point> inFromA = new HashSet<Point>();
			Set<Point> inFromB = new HashSet<Point>();

			Point intersection;
			Point curA = pA;
			Point curB = pB;
			while (true) {
				if (curA != null && curB != null && curA.equals(curB)) {
					intersection = curA;
					break;
				}
				
				if (curA != null) {
					fromA.add(curA);
					inFromA.add(curA);
					curA = this.get(curA);
				}
				if (curB != null) {
					fromB.add(curB);
					inFromB.add(curB);
					curB = this.get(curB);
				}
					
				if (inFromA.contains(curB)) {
					intersection = curB;
					break;
				} else if (inFromB.contains(curA)) {
					intersection = curA;
					break;
				}
			}

			List<Point> path = new ArrayList<Point>();
			int i = fromA.indexOf(intersection);
			path.addAll(i != -1 ? fromA.subList(0, i) : fromA);
			path.add(intersection);
			List<Point> path2 = new ArrayList<Point>();
			i = fromB.indexOf(intersection);
			path2 = (i != -1 ? fromB.subList(0, i) : fromB);
			Collections.reverse(path2);
			path.addAll(path2);
			return path;
		}

//		void load(MeshPoint p, TopologyNetwork tree, boolean isPeak) {
//			MeshPoint next;
//			while (!isLoaded(p)) {
//				if (isPeak) {
//					next = tree.get(p.getByTag(1, true));
//				} else {
//					next = tree.get(p.getByTag(0, false));					
//				}
//				this.add(p, next);
//				p = next;
//				isPeak = !isPeak;
//			}
//		}		
	}
	
	static class Front {
		MeshPoint peak;
		PriorityQueue<MeshPoint> queue; // the set of saddles delineating the cell for which 'peak' is the highest point
		Set<MeshPoint> set; // set of all points in 'queue'
		Backtrace bt;

		Map<MeshPoint, MeshPoint> forwardSaddles;
		Map<MeshPoint, MeshPoint> backwardSaddles;
		
		Comparator<Point> c;

		public Front(MeshPoint peak, boolean up) {
			this.peak = peak;
			this.c = Point.cmpElev(up);
			queue = new PriorityQueue<MeshPoint>(10, new ReverseComparator<Point>(c));
			set = new HashSet<MeshPoint>();
			bt = new Backtrace();
			bt.add(peak, null);
			
			forwardSaddles = new HashMap<MeshPoint, MeshPoint>();
			backwardSaddles = new HashMap<MeshPoint, MeshPoint>();
		}
		
		public boolean add(MeshPoint p) {
			boolean newItem = set.add(p);
			if (newItem) {
				queue.add(p);
				bt.add(p, peak);
			}
			return newItem;
		}

		// remove element, return whether item existed
		// does not affect backtrace, etc.
		public boolean remove(MeshPoint p) {
			boolean removed = set.remove(p);
			ensureNextIsValid();
			return removed;
		}
		
		public MeshPoint pop() {
			MeshPoint p = queue.poll();
			if (p != null) {
				set.remove(p);
				ensureNextIsValid();
			}
			return p;
		}
		
		// we can't remove from the middle of the queue, so when we do remove an item, we
		// must ensure that peek() always yields a valid item, at least
		void ensureNextIsValid() {
			while (true) {
				MeshPoint _n = first();
				if (_n == null || set.contains(_n)) {
					break;
				}
				pop();
			}
		}
		
		public MeshPoint first() {
			return queue.peek();
		}

		// assumes 'saddle' has already been popped from 'other'
		public boolean mergeFrom(Front other, MeshPoint saddle) {
			boolean firstChanged = first().equals(saddle);
			remove(saddle);
			
			for (MeshPoint s : other.set) {
				add(s);
			}
			
			Iterable<Point> swappedNodes = bt.mergeFrom(other.bt, saddle);
			for (Point p : swappedNodes) {
				if (forwardSaddles.containsKey(p)) {
					backwardSaddles.put((MeshPoint)p, forwardSaddles.remove(p));
				} else if (backwardSaddles.containsKey(p)) {
					forwardSaddles.put((MeshPoint)p, backwardSaddles.remove(p));					
				}
			}
			backwardSaddles.put(saddle, other.peak);
			
			return firstChanged;
		}

		public boolean equals(Object o) {
			return o != null && this.peak.equals(((Front)o).peak);
		}
		
		public int hashCode() {
			return peak.hashCode();
		}
		
		public void prune() {
			BacktracePruner btp = bt.pruner();

			for (Point p : set) {
				btp.markPoint(p);
			}
			Set<Point> bookkeeping = new HashSet<Point>();
			Set<Point> significantSaddles = new HashSet<Point>();
			for (Point p : set) {
				bulkSearchThresholdStart(p, btp, bookkeeping, significantSaddles);
			}
			btp.prune();

			for (Iterator<MeshPoint> it = forwardSaddles.keySet().iterator(); it.hasNext(); ) {
				Point p = it.next();
				if (!significantSaddles.contains(p)) {
			        it.remove();
			    }
			}
			for (Iterator<MeshPoint> it = backwardSaddles.keySet().iterator(); it.hasNext(); ) {
				Point p = it.next();
				if (!significantSaddles.contains(p)) {
			        it.remove();
			    }
			}
		}
		
		public int size() {
			return queue.size();
		}
		
		public Point searchThreshold(Point p, Point saddle) {
			/*
			 * we have mapping saddle->peak: forwardSaddles, backwardSaddles
			 * forwardSaddles is saddles fixed via finalizeForward, etc.
			 * backwardSaddles also includes all pending saddle/peak pairs
			 * 
			 * strict definition:
			 * forwardSaddles means: given the saddle, the peak is located in the direction of the backtrace
			 * backwardSaddles means: peak is located in opposite direction to the backtrace
             */

			Point start = bt.get(saddle);
			Point target = null;
			for (int i = 0; i < 1000; i++) {
				Iterable<Point> path = bt.getAtoB(start, target);
				start = null;
				
				boolean isPeak = true;
				Point prev = null;
				Point lockout = null;
				for (Point cur : path) {
					if (lockout != null && this.c.compare(cur, lockout) < 0) {
						lockout = null;
					}
					if (lockout == null) {
						if (isPeak) {
							if (start == null || this.c.compare(cur, start) > 0) {
								start = cur;
								if (this.c.compare(start, p) > 0) {
									return start;
								}
							}
						} else {
							Point pf = forwardSaddles.get(cur);
							Point pb = backwardSaddles.get(cur);
							boolean dirForward = (prev.equals(this.bt.get(cur)));
							Point peakAway = (dirForward ? pf : pb);
							Point peakToward = (dirForward ? pb : pf);
							if (peakToward != null && this.c.compare(peakToward, start) > 0) {
								lockout = cur;
							} else if (peakAway != null && this.c.compare(peakAway, p) > 0) {
								target = peakAway;
								break;
							}
						}
					}

					isPeak = !isPeak;
					prev = cur;
				}
			}
			throw new RuntimeException("infinite loop failsafe exceeded");
		}

		public void bulkSearchThresholdStart(Point saddle, BacktracePruner btp, Set<Point> bookkeeping, Set<Point> significantSaddles) {
			bulkSearchThreshold(bt.get(saddle), null, null, btp, bookkeeping, significantSaddles);
		}
		
		public void bulkSearchThreshold(Point start, Point target, Point minThresh, BacktracePruner btp, Set<Point> bookkeeping, Set<Point> significantSaddles) {
			// minThresh is the equivalent of 'p' in non-bulk mode
			
			boolean withBailout = (bookkeeping != null);
			
			Iterable<Point> path = bt.getAtoB(start, target);
			if (target != null) {
				btp.markPoint(target);
			}
			start = null;
						
			boolean isPeak = true;
			Point prev = null;
			Point lockout = null;
			for (Point cur : path) {
				boolean bailoutCandidate = false;
				
				if (lockout != null && this.c.compare(cur, lockout) < 0) {
					lockout = null;
				}
				if (lockout == null) {
					if (isPeak) {
						if (start == null || this.c.compare(cur, start) > 0) {
							start = cur;
							if (minThresh == null || this.c.compare(start, minThresh) > 0) {
								minThresh = start;
								bailoutCandidate = true;
							}
						}
					} else {
						Point pf = forwardSaddles.get(cur);
						Point pb = backwardSaddles.get(cur);
						boolean dirForward = (prev.equals(this.bt.get(cur)));
						Point peakAway = (dirForward ? pf : pb);
						Point peakToward = (dirForward ? pb : pf);
						if (peakToward != null) {
							// i don't think we can filter based on 'start' like in non-bulk mode because
							// different paths might have differing 'start's at any given time even though
							// they ultimately find the same peaks. if the processing order changed things
							// would break? unfortunately that means every 'toward' saddle is significant
							significantSaddles.add(cur);
							lockout = cur;
						} else if (peakAway != null && this.c.compare(peakAway, minThresh) > 0) {
							significantSaddles.add(cur);
							Point newTarget = peakAway;
							bulkSearchThreshold(start, newTarget, minThresh, btp, bookkeeping, significantSaddles);
							minThresh = newTarget;
							bailoutCandidate = true;
						}
					}
				}
				if (bailoutCandidate && withBailout) {
					if (bookkeeping.contains(cur)) {
						return;
					} else {
						bookkeeping.add(cur);
					}
				}
				
				isPeak = !isPeak;
				prev = cur;
			}
			// reached target
		}
		
		public void write(DataOutputStream out) throws IOException {
			Set<Point> mesh = new HashSet<Point>();
			mesh.add(peak);
			mesh.addAll(set);
			mesh.addAll(forwardSaddles.keySet());
			mesh.addAll(forwardSaddles.values());
			mesh.addAll(backwardSaddles.keySet());
			mesh.addAll(backwardSaddles.values());
			mesh.addAll(bt.backtrace.keySet());
			mesh.addAll(bt.backtrace.values());

			out.writeInt(mesh.size());
			for (Point p : mesh) {
				p.write(out);
			}			

			out.writeBoolean(c == Point.cmpElev(true));
			out.writeLong(peak.ix);

			out.writeInt(set.size());
			for (MeshPoint p : set) {
				out.writeLong(p.ix);
			}

			writePointMap(out, forwardSaddles);
			writePointMap(out, backwardSaddles);
			writePointMap(out, bt.backtrace);
		}
		
		public void writePointMap(DataOutputStream out, Map<? extends Point, ? extends Point> pointMap) throws IOException {
			out.writeInt(pointMap.size());
			for (Entry<? extends Point, ? extends Point> e : pointMap.entrySet()) {
				out.writeLong(e.getKey().ix);
				out.writeLong(e.getValue().ix);
			}
		}
				
		public static Front read(DataInputStream in) throws IOException {
			Map<Long, MeshPoint> mesh = new HashMap<Long, MeshPoint>();
			int nPoints = in.readInt();
			for (int i = 0; i < nPoints; i++) {
				MeshPoint p = MeshPoint.read(in);
				mesh.put(p.ix, p);
			}
			
			boolean up = in.readBoolean();
			Front f = new Front(mesh.get(in.readLong()), up);

			int nFront = in.readInt();
			for (int i = 0; i < nFront; i++) {
				MeshPoint p = mesh.get(in.readLong());
				f.queue.add(p);
				f.set.add(p);
			}

			readPointMap(in, mesh, f.forwardSaddles);
			readPointMap(in, mesh, f.backwardSaddles);
			readPointMap(in, mesh, f.bt.backtrace);
			
			return f;
		}
		
		static void readPointMap(DataInputStream in, Map<Long, MeshPoint> mesh, Map pointMap) throws IOException {
			int nEntries = in.readInt();
			for (int i = 0; i < nEntries; i++) {
				MeshPoint k = mesh.get(in.readLong());
				MeshPoint v = mesh.get(in.readLong());
				pointMap.put(k, v);
			}
		}
	}
	

}
