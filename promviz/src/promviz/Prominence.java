package promviz;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.AbstractMap;
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
import java.util.Objects;
import java.util.Set;

import promviz.Prominence.Backtrace.BacktracePruner;
import promviz.debug.Harness;
import promviz.debug.Harness.DomainSaddleInfo;
import promviz.dem.DEMFile;
import promviz.util.DefaultMap;
import promviz.util.Logging;
import promviz.util.MutablePriorityQueue;
import promviz.util.ReverseComparator;
import promviz.util.SaneIterable;
import promviz.util.WorkerPoolDebug;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class Prominence {

	static final int COALESCE_STEP = 2;

	public static double prominence(Point peak, Point saddle) {
		return (saddle != null ? Math.abs(peak.elev - saddle.elev) : Double.POSITIVE_INFINITY);
	}
	
	public static class PromPair implements Comparable<PromPair> {
		private Point peak;
		private Point saddle;
		private Comparator<Point> cmp;

		public PromPair(Point peak, Point saddle) {
			this.peak = peak;
			this.saddle = saddle;
			cmp = Point.cmpElev(Point.cmpElev(true).compare(peak, saddle) > 0);
		}
		
		double prominence() {
			return Prominence.prominence(peak, saddle);
		}

		public static int compare(PromPair ppa, PromPair ppb, Comparator<Point> cmp) {
			int c = Double.compare(ppa.prominence(), ppb.prominence());
			if (c == 0) {
				int cp = cmp.compare(ppa.peak, ppb.peak);
				int cs = cmp.compare(ppa.saddle, ppb.saddle);
				if (cp > 0 && cs < 0) {
					c = 1;
				} else if (cp < 0 && cs > 0) {
					c = -1;
				}
			}
			return c;
		}
		
		public int compareTo(PromPair pp) {
			return compare(this, pp, cmp);
		}
	}
	
	public static abstract class PromFact {}
	
	public static class PromBaseInfo extends PromFact {
		
	}
	
	public static class PromPending extends PromFact {
		
	}
	
	public static class PromThresh extends PromFact {
		Point pthresh;
	}
	
	public static class PromParent extends PromFact {
		Point parent;
		List<Long> path;
	}
	
	public static class PromSubsaddle extends PromFact {
		
	}
	
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
		Map<Long, Long> pthresh;
		Map<Long, Long> parents;
		Map<Long, List<Long>> parent_paths;
		Map<Long, Set<Point[]>> subsaddles;
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
			
			for (Entry<Long, Long> e : output.pthresh.entrySet()) {
				Harness.outputPThresh(up, e.getKey(), e.getValue());
			}
			
			for (Entry<Long, Long> e : output.parents.entrySet()) {
				Harness.outputPromParentage(e.getKey(), e.getValue(), output.parent_paths.get(e.getKey()));
			}
			
			// TODO if peak already exists in proms, consolidate and save writes
			for (Entry<Long, Set<Point[]>> e : output.subsaddles.entrySet()) {
				List<DomainSaddleInfo> ssi = new ArrayList<DomainSaddleInfo>();
				for (Point[] pp : e.getValue()) {
					DomainSaddleInfo dsi = new DomainSaddleInfo();
					ssi.add(dsi);
					
					dsi.saddle = pp[0];
					dsi.peak = pp[1];
					dsi.isHigher = true;
					dsi.isDomain = false;
				}
				Harness.outputSubsaddles(e.getKey(), ssi, up);
			}
			
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
	
	static interface PromConsumer {
		boolean isNotablyProminent(PromPair pp);
		void emitFact(Point p, PromFact pf);
	}
	
	static class ChunkProcessor implements PromConsumer {
		ChunkInput input;
		Prefix prefix;
		boolean up;
		PagedElevGrid mesh;

		Set<Front> fronts;
		// the highest saddle directly connecting two fronts (both forward and reverse mapping)
		Map<MeshPoint, Set<Front>> connectorsBySaddle;
		Map<Set<Front>, MeshPoint> connectorsByFrontPair;
		// front pairs that can and must be coalesced
		MutablePriorityQueue<FrontMerge> pendingMerges;
		Map<Long, Long> pthresh;
		Map<Long, Long> parents;
		Map<Long, List<Long>> parent_paths;
		Map<Long, Set<Point[]>> subsaddles;
		
		public ChunkProcessor(ChunkInput input) {
			this.input = input;
			this.prefix = input.p;
			this.up = input.up;
			this.mesh = TopologyBuilder._createMesh(input.coverage);
		}
		
		boolean inChunk(long ix) {
			return prefix.isParent(ix);	
		}
		
		public boolean isNotablyProminent(PromPair pp) {
			return pp != null && pp.prominence() >= input.cutoff;
		}
		
		public ChunkOutput build() {
			List<PromInfo> proms = new ArrayList<PromInfo>();
			pthresh = new HashMap<Long, Long>();
			parents = new HashMap<Long, Long>();
			parent_paths = new HashMap<Long, List<Long>>();
			subsaddles = new DefaultMap<Long, Set<Point[]>>() {
				public Set<Point[]> defaultValue(Long key) {
					return new HashSet<Point[]>();
				}
			};
			
			load();
			
			Logging.log("before " + fronts.size());
			while (pendingMerges.size() > 0) {
				FrontMerge toMerge = pendingMerges.poll();
				PromInfo pi = mergeFronts(toMerge);
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
			output.pthresh = pthresh;
			output.parents = parents;
			output.parent_paths = parent_paths;
			output.subsaddles = (HashMap)((HashMap)subsaddles).clone();
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
				return Objects.hash(parent, child);
			}
		}
		
		public void load() {
			fronts = new HashSet<Front>();
			connectorsBySaddle = new HashMap<MeshPoint, Set<Front>>();
			connectorsByFrontPair = new HashMap<Set<Front>, MeshPoint>();
			pendingMerges = new MutablePriorityQueue<FrontMerge>(new Comparator<FrontMerge>() {
				// prefer highest parent, then highest child
				public int compare(FrontMerge a, FrontMerge b) {
					int c = a.parent.c.compare(a.parent.peak, b.parent.peak);
					if (c == 0) {
						c = a.child.c.compare(a.child.peak, b.child.peak);
					}
					return -c;
				}
			});
			
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
				for (MeshPoint saddle : f.queue){
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
			// there may be some duplicate edges as artifacts of the topobuild process; use set to remove them
			Set<Edge> edges = Sets.newHashSet(FileUtil.loadEdges(up, prefix, FileUtil.PHASE_RAW));
			
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
					frontsByPeak.get(summit).add(saddle, true);
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
		
		PromInfo mergeFronts(FrontMerge fm) {
			Front parent = fm.parent;
			Front child = fm.child;
			
			// unlist child front, merge into parent, and remove connection between the two
			fronts.remove(child);
			MeshPoint saddle = child.pop();
			boolean newParentMerge = parent.mergeFrom(child, saddle, subsaddles);
			connectorsClear(saddle, frontPair(parent, child));
			
			// update for all fronts adjacent to child (excluding 'parent')
			for (MeshPoint subsaddle : child.queue) {
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
				
				if (isNotablyProminent(parent.pendProm())) {
					parent.flushPendingThresh(parent.peak, this);
					parent.flushPendingParents(parent.pendProm(), parent, this);
				}
			}

			PromInfo pi = new PromInfo(up, child.peak, saddle);
			boolean save = pi.finalize(parent, child, this);
			return (save ? pi : null);
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
				if (pi.isNotablyProminent(this)) {
					pi.path = pathToUnknown(f);
					proms.add(pi);
				}
				
				Point saddle = f.first();
				Front other = primaryFront(f);
				if (other != null) {
					finalizeSubsaddles(f, other, saddle, f.peak);
					finalizeSubsaddles(other, f, saddle, f.peak);
				}
			}
		}
		
		void finalizeSubsaddles(Front f, Front other, Point saddle, Point peak) {
			Map.Entry<Point, List<Point>> e = f.thresholds.traceUntil(saddle, other.thresholds.root, f.c);
			for (Point sub : e.getValue()) {
				boolean notablyProminent = (sub == f.peak ? this.isNotablyProminent(f.pendProm()) : f.promPoints.containsKey(sub));
				if (notablyProminent) {
					subsaddles.get(sub.ix).add(new Point[] {saddle, peak});
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
		
		public void emitFact(Point p, PromFact pf) {
			if (pf instanceof PromThresh) {
				PromThresh o = (PromThresh)pf;
				pthresh.put(p.ix, o.pthresh.ix);
			} else if (pf instanceof PromParent) {
				PromParent o = (PromParent)pf;
				parents.put(p.ix, o.parent.ix);
				parent_paths.put(p.ix, o.path);
			} else {
				throw new RuntimeException();
			}
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
		
		boolean isNotablyProminent(PromConsumer context) {
			return context.isNotablyProminent(new PromPair(p, saddle));
		}
		
		public double _prominence() {
			return Prominence.prominence(p, saddle);
		}
		
		public boolean finalize(Front parent, Front child, PromConsumer context) {
			boolean aboveCutoff = isNotablyProminent(context);
			if (aboveCutoff || !child.pendingPThresh.isEmpty() || !child.pendingParent.isEmpty()) {
				Point thresh = parent.thresholds.get(this.p);
				if (aboveCutoff) {
					parent.promPoints.put(child.peak, saddle);
					Path _ = new Path(parent.bt.getAtoB(thresh, this.p), this.p);
					this.path = _.path;
					this.thresholdFactor = _.thresholdFactor;
				}
				if (aboveCutoff || !child.pendingPThresh.isEmpty()) {
					this.pthresh(thresh, parent, child, context);
				}
				if (aboveCutoff || !child.pendingParent.isEmpty()) {
					this.parentage(thresh, parent, child, context);
				}
			}
			return aboveCutoff;
		}
		
		void pthresh(Point thresh, Front parent, Front child, PromConsumer context) {
			Point pthresh = thresh;
			while (!context.isNotablyProminent(parent.getProm(pthresh)) && !pthresh.equals(parent.peak)) {
				pthresh = parent.thresholds.get(pthresh);
			}
			if (context.isNotablyProminent(parent.getProm(pthresh))) {
				if (isNotablyProminent(context)) {
					child.pendingPThresh.add(p.ix);
				}
				child.flushPendingThresh(pthresh, context);
			} else {
				if (isNotablyProminent(context)) {
					parent.pendingPThresh.add(this.p.ix);
				}
				parent.pendingPThresh.addAll(child.pendingPThresh);
			}
		}

		void parentage(Point thresh, Front f, Front other, PromConsumer context) {
			if (isNotablyProminent(context)) {
				other.pendingParent.put(this.p, this.saddle);
			}
			for (Point p : f.thresholds.trace(thresh)) {
				PromPair pp = f.getProm(p);
				if (pp == null) {
					continue;
				}
				other.flushPendingParents(pp, f, context);
			}
			f.pendingParent.putAll(other.pendingParent);
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
		// for MST could we store just <Long, Long>?
		Map<Point, Point> backtrace; 
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
				throw new RuntimeException("point not loaded [" + p + "]");
			}
			return parent;
		}
		
//		public boolean isLoaded(Point p) {
//			return backtrace.containsKey(p) || p.equals(root);
//		}
		
		public int size() {
			return backtrace.size();
		}
		
		void removeInCommon(Backtrace other, MeshPoint saddle) {
			for (Iterator<Point> it = other.backtrace.keySet().iterator(); it.hasNext(); ) {
				Point from = it.next();
				if (this.backtrace.containsKey(from) && !from.equals(saddle)) {
					this.backtrace.remove(from);
					it.remove();
				}
			}
		}
		
		public Iterable<Point> mergeFromAsNetwork(Backtrace other, MeshPoint saddle) {
			removeInCommon(other, saddle);

			List<Point> toReverse = Lists.newArrayList(other.trace(saddle)); 
			other.backtrace.remove(saddle);
			this.backtrace.putAll(other.backtrace);
			
			Point from = null;
			for (Point to : toReverse) {
				if (from != null) {
					this.add(to, from);
				}
				from = to;
			}
			return toReverse.subList(1, toReverse.size() - 1); // exclude connecting saddle and old front peak
		}
		
		public List<Point> mergeFromAsTree(Backtrace other, MeshPoint saddle, Comparator<Point> c) {
			removeInCommon(other, saddle);

			Map.Entry<Point, List<Point>> e = traceUntil(saddle, other.root, c);
			Point threshold = e.getKey();
			List<Point> belowThresh = e.getValue();
			
			this.backtrace.remove(saddle);
			other.backtrace.remove(saddle);
			this.backtrace.putAll(other.backtrace);
			this.add(other.root, threshold);
			return belowThresh;
		}
		
		public Map.Entry<Point, List<Point>> traceUntil(Point saddle, Point cutoff, Comparator<Point> c) {
			Point threshold = null;
			List<Point> belowThresh = new ArrayList<Point>();
			for (Point p : trace(saddle)) {
				if (p == saddle) {
					continue;
				}
				
				if (c.compare(p, cutoff) > 0) {
					threshold = p;
					break;
				}
				belowThresh.add(p);
			}
			return new AbstractMap.SimpleEntry<Point, List<Point>>(threshold, belowThresh);
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
		MutablePriorityQueue<MeshPoint> queue; // the set of saddles delineating the cell for which 'peak' is the highest point
		Backtrace bt;
		Backtrace thresholds;
		
		Map<MeshPoint, MeshPoint> promPoints;
		Set<Long> pendingPThresh;
		Map<MeshPoint, MeshPoint> pendingParent;
		
		Comparator<Point> c;

		public Front(MeshPoint peak, boolean up) {
			this.peak = peak;
			this.c = Point.cmpElev(up);
			queue = new MutablePriorityQueue<MeshPoint>(new ReverseComparator<Point>(c));
			bt = new Backtrace();
			bt.add(peak, null);
			thresholds = new Backtrace();
			thresholds.add(peak, null);
			
			promPoints = new HashMap<MeshPoint, MeshPoint>();
			pendingPThresh = new HashSet<Long>();
			pendingParent = new HashMap<MeshPoint, MeshPoint>();
		}
		
		public PromPair pendProm() {
			return new PromPair(peak, first());
		}
		
		public PromPair getProm(Point p) {
			if (p.equals(peak)) {
				return pendProm();
			} else if (promPoints.containsKey(p)) {
				return new PromPair(p, promPoints.get(p));
			} else {
				return null;
			}
		}
		
		public void add(MeshPoint p, boolean initial) {
			queue.add(p);
			if (initial) {
				bt.add(p, peak);
				thresholds.add(p, peak);
			}
		}
		
		// remove element, return whether item existed
		// does not affect backtrace, etc.
		public boolean remove(MeshPoint p) {
			return queue.remove(p);
		}
		
		public MeshPoint pop() {
			return queue.poll();
		}
		
		public MeshPoint first() {
			return queue.peek();
		}
		
		// assumes 'saddle' has already been popped from 'other'
		public boolean mergeFrom(Front other, MeshPoint saddle, Map<Long, Set<Point[]>> subsaddles) {
			boolean firstChanged = first().equals(saddle);
			remove(saddle);
			
			for (MeshPoint s : other.queue) {
				add(s, false);
			}

			promPoints.putAll(other.promPoints);
			
			bt.mergeFromAsNetwork(other.bt, saddle);
			
			List<Point> subbed1 = Lists.newArrayList(other.thresholds.trace(saddle));
			subbed1 = subbed1.subList(1, subbed1.size() - 1);
			List<Point> subbed2 = thresholds.mergeFromAsTree(other.thresholds, saddle, this.c);
			for (Point sub : Iterables.concat(subbed1, subbed2)) {
				if (promPoints.containsKey(sub)) {
					subsaddles.get(sub.ix).add(new Point[] {saddle, other.peak});
				}
			}
			
			return firstChanged;
		}

		public boolean equals(Object o) {
			return o != null && this.peak.equals(((Front)o).peak);
		}
		
		public int hashCode() {
			return peak.hashCode();
		}
		
		public void prune() {
			BacktracePruner threshp = thresholds.pruner();
			for (Point p : queue) {
				threshp.markPoint(p);
			}
			threshp.prune();
			
			BacktracePruner btp = bt.pruner();
			for (Point p : thresholds.backtrace.keySet()) {
				btp.markPoint(p);
			}
			for (Point p : pendingParent.keySet()) {
				btp.markPoint(p);
			}
			btp.prune();
			
			for (Iterator<MeshPoint> it = promPoints.keySet().iterator(); it.hasNext(); ) {
				if (!thresholds.backtrace.containsKey(it.next())) {
					it.remove();
				}
			}
		}
		
		public int size() {
			return queue.size();
		}
		
		public void flushPendingThresh(Point pthresh, PromConsumer context) {
			for (Iterator<Long> it = pendingPThresh.iterator(); it.hasNext(); ) {
				PromThresh pt = new PromThresh();
				pt.pthresh = pthresh;
				context.emitFact(new Point(it.next(), 0), pt);

				it.remove();
			}
		}
		
		public void flushPendingParents(PromPair cand, Front pathFront, PromConsumer context) {
			for (Iterator<Entry<MeshPoint, MeshPoint>> it = this.pendingParent.entrySet().iterator(); it.hasNext(); ) {
				Entry<MeshPoint, MeshPoint> e = it.next();
				PromPair pend = new PromPair(e.getKey(), e.getValue());
				if (cand.compareTo(pend) > 0) {
					PromParent parentInfo = new PromParent();
					parentInfo.parent = cand.peak;
					parentInfo.path = new Path(pathFront.bt.getAtoB(pend.peak, cand.peak), null).path;
					context.emitFact(pend.peak, parentInfo);
					
					it.remove();
				} else {
					// mark domain subsaddle?
				}
			}
		}
		
		// TODO i don't think backtrace 'root' gets stored?
		public void write(DataOutputStream out) throws IOException {
			Set<Point> mesh = new HashSet<Point>();
			mesh.add(peak);
			mesh.addAll(queue);
			mesh.addAll(thresholds.backtrace.keySet());
			mesh.addAll(thresholds.backtrace.values());
			mesh.addAll(bt.backtrace.keySet());
			mesh.addAll(bt.backtrace.values());
			mesh.addAll(promPoints.keySet());
			mesh.addAll(promPoints.values());
			mesh.addAll(pendingParent.keySet());
			mesh.addAll(pendingParent.values());

			out.writeInt(mesh.size());
			for (Point p : mesh) {
				p.write(out);
			}			

			out.writeBoolean(c == Point.cmpElev(true));
			out.writeLong(peak.ix);

			out.writeInt(queue.size());
			for (MeshPoint p : queue) {
				out.writeLong(p.ix);
			}

			writePointMap(out, thresholds.backtrace);
			writePointMap(out, bt.backtrace);
			writePointMap(out, promPoints);
			
			out.writeInt(pendingPThresh.size());
			for (long ix : pendingPThresh) {
				out.writeLong(ix);
			}
			
			writePointMap(out, pendingParent);
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
				f.add(p, false);
			}

			readPointMap(in, mesh, f.thresholds.backtrace);
			readPointMap(in, mesh, f.bt.backtrace);
			readPointMap(in, mesh, f.promPoints);
			
			int nPendPT = in.readInt();
			for (int i = 0; i < nPendPT; i++) {
				f.pendingPThresh.add(in.readLong());
			}
			
			readPointMap(in, mesh, f.pendingParent);
			
			return f;
		}
		
		static void readPointMap(DataInputStream in, Map<Long, MeshPoint> mesh, Map<? super MeshPoint, ? super MeshPoint> pointMap) throws IOException {
			int nEntries = in.readInt();
			for (int i = 0; i < nEntries; i++) {
				MeshPoint k = mesh.get(in.readLong());
				MeshPoint v = mesh.get(in.readLong());
				pointMap.put(k, v);
			}
		}
	}
	

}