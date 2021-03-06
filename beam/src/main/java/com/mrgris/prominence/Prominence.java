package com.mrgris.prominence;

import java.util.ArrayList;
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
import java.util.function.Predicate;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.mrgris.prominence.Edge.HalfEdge;
import com.mrgris.prominence.Prominence.Backtrace.BacktracePruner;
import com.mrgris.prominence.Prominence.Backtrace.TraceResult;
import com.mrgris.prominence.Prominence.Front.AvroFront;
import com.mrgris.prominence.Prominence.Front.PeakWithoutParent;
import com.mrgris.prominence.Prominence.PromFact;
import com.mrgris.prominence.dem.DEMFile;
import com.mrgris.prominence.util.DefaultMap;
import com.mrgris.prominence.util.MutablePriorityQueue;
import com.mrgris.prominence.util.ReverseComparator;
import com.mrgris.prominence.util.SaneIterable;
import com.mrgris.prominence.util.WorkerUtils;

public class Prominence extends DoFn<Iterable<KV<Long, Iterable<HalfEdge>>>, PromFact> {
	
    private static final Logger LOG = LoggerFactory.getLogger(Prominence.class);
	
	boolean up;
	double cutoff;
	PCollectionView<Map<Prefix, Iterable<DEMFile>>> coverageSideInput;
	TupleTag<AvroFront> pendingFrontsTag;
	TupleTag<Edge> mstTag;
	
	public Prominence(boolean up, double cutoff, PCollectionView<Map<Prefix, Iterable<DEMFile>>> coverageSideInput,
			TupleTag<AvroFront> pendingFrontsTag, TupleTag<Edge> mstTag) {
		this.up = up;
		this.cutoff = cutoff;
		this.coverageSideInput = coverageSideInput;
		this.pendingFrontsTag = pendingFrontsTag;
		this.mstTag = mstTag;
	}

    @Setup
    public void setup() {
    	WorkerUtils.initializeGDAL();
    }
	
	@DefaultCoder(AvroCoder.class)
	public static class PromFact {
		
		@DefaultCoder(AvroCoder.class)
		public static class Saddle {
			Point s;
			int traceNumTowardsP;
			
			public Saddle() {}
			
			public Saddle(Point saddle, int traceNumTowardsP) {
				this.s = new Point(saddle);
				this.traceNumTowardsP = traceNumTowardsP;
				if (traceNumTowardsP == Edge.TAG_NULL) {
					throw new RuntimeException();
				}
			}
		}
				
		Point p;
		
		@Nullable
		Saddle saddle;
		@Nullable
		Point thresh;
		
		@Nullable
		Point pthresh;
		@Nullable
		Point parent;
		@Nullable
		Integer promRank;
		
		List<Saddle> elevSubsaddles;
		List<Saddle> promSubsaddles;
		
		@Nullable
		List<Long> threshPath;
		double threshTrim = -1;
		@Nullable
		List<Long> parentPath;
		@Nullable
		List<List<Long>> domainBoundary;
		
		public PromFact() {
			elevSubsaddles = new ArrayList<>();
			promSubsaddles = new ArrayList<>();
		}
				
		public static PromFact baseFact(Point peak, Point saddle, Point thresh, int traceNum) {
			PromFact base = new PromFact();
			base.p = new Point(peak);
			base.saddle = new PromFact.Saddle(saddle, traceNum);
			base.thresh = (thresh != null ? new Point(thresh) : null);
			return base;
		}
		
		public static PromFact pthreshFact(Point p, Point pthresh) {
			PromFact pt = new PromFact();
			pt.p = new Point(p);
			pt.pthresh = new Point(pthresh);
			return pt;
		}
		
		public static PromFact parentageFact(Point p, Point parent) {
			PromFact parentInfo = new PromFact();
			parentInfo.p = new Point(p);
			parentInfo.parent = new Point(parent);
			return parentInfo;
		}
		
		final static int SS_ELEV = 1;
		final static int SS_PROM = 2;
		
		public static PromFact subsaddleFact(Point p, Point saddle, int traceNum, int type) {
			PromFact ps = new PromFact();
			List<Saddle> ss;
			if (type == SS_ELEV) {
				ss = ps.elevSubsaddles;
			} else if (type == SS_PROM) {
				ss = ps.promSubsaddles;
			} else {
				throw new RuntimeException();
			}
			ps.p = new Point(p);
			ss.add(new PromFact.Saddle(saddle, traceNum));
			return ps;
		}
	}
	
    @ProcessElement
    public void processElement(ProcessContext c) {
    	Iterable<KV<Long, Iterable<HalfEdge>>> protoFronts = c.element();
    	Map<Prefix, Iterable<DEMFile>> coverage = c.sideInput(coverageSideInput);
    	
    	new Searcher(up, cutoff, protoFronts, coverage) {
    		public void emitFact(PromFact pf) {
    			c.output(pf);
    		}
    		
    		public void emitPendingFront(Front f) {
    			c.output(pendingFrontsTag, new AvroFront(f));
    		}
    		
    		public void emitBacktraceEdge(Edge e) {
    			c.output(mstTag, e);
    		}
    	}.search();
    }    	
	
	static final int COALESCE_STEP = 2;

	public static double prominence(Point peak, Point saddle) {
		// think saddle null is to handle mt. everest?
		return (saddle != null ? Math.abs(peak.elev - saddle.elev) : Double.POSITIVE_INFINITY);
	}
	
	public static class PromPair implements Comparable<PromPair> {
		public Point peak;
		public Point saddle;
		private Comparator<Point> cmp;

		public PromPair(Point peak, Point saddle) {
			this.peak = peak;
			this.saddle = saddle;
			cmp = (saddle != null ? Point.cmpElev(Point.cmpElev(true).compare(peak, saddle) > 0) : null);
		}
		
		double prominence() {
			return Prominence.prominence(peak, saddle);
		}

		static void longSubtractWithOverflow(long a, long b, long[] result) {
			if (a > -1 && b < a - Long.MAX_VALUE) {
				result[0] = 1;
			} else if (a < -1 && b > a - Long.MIN_VALUE) {
				result[0] = -1;
			} else {
				result[0] = 0;
			}
			result[1] = a - b;
		}
		
		static int diffCompare(long a0, long a1, long b0, long b1) {
			long[] diffA = new long[2];
			long[] diffB = new long[2];
			longSubtractWithOverflow(a0, a1, diffA);
			longSubtractWithOverflow(b0, b1, diffB);
			int c = Long.compare(diffA[0], diffB[0]);
			if (c != 0) {
				return c;
			}
			return Long.compare(diffA[1], diffB[1]);
		}
		
		static int diffCompare(int a0, int a1, int b0, int b1) {
			long diffA = (long)a0 - (long)a1;
			long diffB = (long)b0 - (long)b1;
			return Long.compare(diffA, diffB);
		}
		
		public static int compare(PromPair ppa, PromPair ppb, Comparator<Point> cmp) {
			int c = Double.compare(ppa.prominence(), ppb.prominence());
			if (c != 0) {
				return c;
			}
			if (Double.isInfinite(ppa.prominence())) {
				// should only be one global max, so meaningless to sub-compare
				return 0;
			}
			
			boolean up = (cmp == Point._cmpElev);

			c = diffCompare(ppa.peak.isodist, ppa.saddle.isodist, ppb.peak.isodist, ppb.saddle.isodist);
			if (c != 0) {
				return (up ? 1 : -1) * c;
			}
			c = diffCompare(
					PointIndex.pseudorandId(ppa.peak.ix),
					PointIndex.pseudorandId(ppa.saddle.ix),
					PointIndex.pseudorandId(ppb.peak.ix),
					PointIndex.pseudorandId(ppb.saddle.ix)
			);
			if (c != 0) {
				return (up ? 1 : -1) * c;
			}
			c = diffCompare(
					PointIndex.split(ppa.peak.ix)[3],
					PointIndex.split(ppa.saddle.ix)[3],
					PointIndex.split(ppb.peak.ix)[3],
					PointIndex.split(ppb.saddle.ix)[3]
			);
			if (c != 0) {
				return (up ? 1 : -1) * c;
			}
			
			c = Long.compare(PointIndex.pseudorandId(ppa.peak.ix), PointIndex.pseudorandId(ppb.peak.ix));
			if (c != 0) {
				return (up ? 1 : -1) * c;
			}
			c = Long.compare(PointIndex.pseudorandId(ppa.saddle.ix), PointIndex.pseudorandId(ppb.saddle.ix));
			if (c != 0) {
				return (up ? 1 : -1) * c;
			}
			c = Integer.compare(PointIndex.split(ppa.saddle.ix)[3], PointIndex.split(ppb.saddle.ix)[3]);
			if (c != 0) {
				return (up ? 1 : -1) * c;
			}
			c = Integer.compare(PointIndex.split(ppa.peak.ix)[3], PointIndex.split(ppb.peak.ix)[3]);
			return c;
		}
		
		public int compareTo(PromPair pp) {
			return compare(this, pp, cmp);
		}
	}
		
	static abstract class Searcher {
		boolean up;
		double cutoff;
		PagedElevGrid mesh;
		
		Set<Front> fronts;
		// the highest saddle directly connecting two fronts (both forward and reverse mapping)
		Map<MeshPoint, Set<Front>> connectorsBySaddle;
		Map<Set<Front>, MeshPoint> connectorsByFrontPair;
		// front pairs that can and must be coalesced
		MutablePriorityQueue<FrontMerge> pendingMerges;
		
		public Searcher(boolean up, double cutoff) {
			this.up = up;
			this.cutoff = cutoff;
			
			fronts = new HashSet<Front>();
			connectorsBySaddle = new HashMap<MeshPoint, Set<Front>>();
			connectorsByFrontPair = new HashMap<Set<Front>, MeshPoint>();
			pendingMerges = new MutablePriorityQueue<FrontMerge>(new Comparator<FrontMerge>() {
				// prefer highest parent, then highest child
				// TODO try just merging these in any order
				public int compare(FrontMerge a, FrontMerge b) {
					//return Integer.compare(a.hashCode(), b.hashCode());
					
					int c = a.parent.c.compare(a.parent.peak, b.parent.peak);
					if (c == 0) {
						c = a.child.c.compare(a.child.peak, b.child.peak);
					}
					return -c;
					
				}
			});			
		}
		
		public Searcher(boolean up, double cutoff, Iterable<KV<Long, Iterable<HalfEdge>>> protoFronts, Map<Prefix, Iterable<DEMFile>> coverage) {
			this(up, cutoff);
			this.mesh = TopologyBuilder._createMesh(coverage);
			loadForBase(protoFronts);
			loadPostprocess();
		}
		
		public Searcher(boolean up, double cutoff, Iterable<AvroFront> fronts) {
			this(up, cutoff);
			loadForCoalesce(fronts);
			loadPostprocess();
		}
		
		public boolean isNotablyProminent(PromPair pp) {
			return pp != null && pp.prominence() >= cutoff;
		}
		
		// TODO what happens to rings?
		public void search() {
			//Logging.log("before " + fronts.size());
			while (pendingMerges.size() > 0) {
				FrontMerge toMerge = pendingMerges.poll();
				mergeFronts(toMerge);
			}
			//Logging.log("after " + fronts.size());
			
			for (Front f : fronts) {
				emitMST(f);
				f.prune();
				emitPendingFront(f);
			}
			
			if (this.mesh != null) {
				this.mesh.destroy();
			}
		}
		
		public void finalizePending() {
			//Logging.log("finalizing remaining");
			for (Front f : fronts) {
				if (f.first() == null) {
					//Logging.log("'global' max: ignoring");
					continue;
				}

				PromPair pp = new PromPair(f.peak, f.first());
				if (isNotablyProminent(pp)) {
					emitFact(PromFact.baseFact(pp.peak, pp.saddle, null, f.traceNums.get(pp.saddle)));
				}
				
				Point saddle = f.first();
				Front other = primaryFront(f);
				finalizeSubsaddles(f, other, saddle, f.peak);
				finalizeDomainSubsaddles(f, other, saddle, f.peak);
				if (other != null) {
					finalizeSubsaddles(other, f, saddle, f.peak);
					finalizeDomainSubsaddles(other, f, saddle, f.peak);
				}
				if (isNotablyProminent(pp)) {
					f.flushPendingParents(pp, false, this);
				}
				
				finalizeMST(f, saddle, other);
			}
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
		
		void loadForBase(Iterable<KV<Long, Iterable<HalfEdge>>> protoFronts) {
			// load necessary DEM pages for graph
			final Set<Prefix> pages = new HashSet<Prefix>();
			for (KV<Long, Iterable<HalfEdge>> front : protoFronts) {
				pages.add(PagedElevGrid.segmentPrefix(front.getKey()));
				for (HalfEdge he : front.getValue()) {
					pages.add(PagedElevGrid.segmentPrefix(he.saddle));					
				}
			}
			mesh.bulkLoadPage(pages);

			// build atomic fronts (summit + immediate saddles)
			for (KV<Long, Iterable<HalfEdge>> protoFront : protoFronts) {
				MeshPoint summit = mesh.get(protoFront.getKey());
				if (summit == null) {
					throw new RuntimeException("null summit " + protoFront.getKey());
				}
				Front f = new Front(summit, up);
				
				// TODO: clean up once ring situation is clearer?
				// need to remove duplicate saddles caused by rings (which are inherently basin saddles)
				// ideally front handles this situation directly
				Map<Long, Integer> saddleCounts = new DefaultMap<Long, Integer>() {
					@Override
					public Integer defaultValue(Long key) {
						return 0;
					}
				};
				for (HalfEdge he : protoFront.getValue()) {
					saddleCounts.put(he.saddle, saddleCounts.get(he.saddle) + 1);
				}
				Set<Long> nonBasinSaddles = new HashSet<>();
				for (Entry<Long, Integer> e : saddleCounts.entrySet()) {
					long saddleIx = e.getKey();
					int count = e.getValue();
					if (count < 1 || count > 2) {
						throw new RuntimeException();
					}
					if (count == 1) {
						nonBasinSaddles.add(saddleIx);
					}
				}
				Map<Long, Integer> traceNums = new HashMap<>();
				for (HalfEdge he : protoFront.getValue()) {
					traceNums.put(he.saddle, he.tag);
				}
				
				for (long saddleIx : nonBasinSaddles) {
					MeshPoint saddle = mesh.get(saddleIx);
					f.add(saddle, traceNums.get(saddleIx), true);
				}
				fronts.add(f);
			}
		}
		
		void loadForCoalesce(Iterable<AvroFront> serializedFronts) {
			for (AvroFront f : serializedFronts) {
				fronts.add(f.toFront());
			}
		}
		
		public void loadPostprocess() {			
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
			
			//Logging.log("" + pendingMerges.size());
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
		
		void mergeFronts(FrontMerge fm) {
			Front parent = fm.parent;
			Front child = fm.child;

			MeshPoint saddle = child.first();
			PromPair newProm = new PromPair(child.peak, saddle);
			boolean notablyProminent = isNotablyProminent(newProm);

			///////
			List<Point> childThreshes = child.thresholds.traceUntil(saddle, newProm.peak, child.c).belowThresh;
			TraceResult tr = parent.thresholds.traceUntil(saddle, newProm.peak, parent.c);
			Point thresh = tr.threshold;
			List<Point> parentThreshes = tr.belowThresh;
			int traceNumTowards = child.traceNums.get(newProm.saddle);
			int traceNumAway = parent.traceNums.get(newProm.saddle);
			
			if (notablyProminent) {
				emitFact(PromFact.baseFact(newProm.peak, newProm.saddle, thresh, traceNumTowards));
			}
			
			PromPair promThresh = null;
			for (Point sub : childThreshes) {
				PromPair subpp = child.getThreshold(sub);
				if (subpp != null) {
					emitFact(PromFact.subsaddleFact(sub, saddle, traceNumTowards, PromFact.SS_ELEV));
					if (promThresh == null || subpp.compareTo(promThresh) > 0) {
						promThresh = subpp;
						emitFact(PromFact.subsaddleFact(sub, saddle, traceNumTowards, PromFact.SS_PROM));
					}
				}
			}
			for (Point sub : parentThreshes) {
				if (parent.getThreshold(sub) != null) {
					emitFact(PromFact.subsaddleFact(sub, saddle, traceNumAway, PromFact.SS_ELEV));
				}
			}
			///////
			
			if (notablyProminent || !child.pendingPThresh.isEmpty()) {
				this.pthresh(newProm, thresh, parent, child);
			}
			if (notablyProminent) {
				child.flushPendingParents(newProm, false, this);
			}
			if (notablyProminent || !child.parentPending.isEmpty()) {
				parentage(newProm, parent, child);
			}

			// unlist child front, merge into parent, and remove connection between the two
			fronts.remove(child);
			child.pop();
			boolean newParentMerge = parent.mergeFrom(child, saddle);
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
					parent.flushPendingParents(parent.pendProm(), true, this);
				}
			}

			if (notablyProminent) {
				parent.thresholds.setProm(newProm.peak, newProm.saddle);
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

		void pthresh(PromPair pp, Point thresh, Front parent, Front child) {
			if (isNotablyProminent(pp)) {
				child.pendingPThresh.add(pp.peak.ix);
			}

			Point pthresh = parent.thresholds.traceUntil(thresh, new Predicate<Point>() {
				@Override
				public boolean test(Point p) {
					return isNotablyProminent(parent.getThreshold(p));
				}
			}, true).threshold;
			if (pthresh != null) {
				child.flushPendingThresh(pthresh, this);
			} else {
				// make part of front merge?
				parent.pendingPThresh.addAll(child.pendingPThresh);
			}
		}

		void parentage(PromPair pp, Front f, Front other) {
			if (isNotablyProminent(pp)) {
				PeakWithoutParent pwp = new PeakWithoutParent();
				pwp.prom = pp;
				pwp.minVisibility = null;
				pwp.traceNumAway = f.traceNums.get(pp.saddle);
				other.parentPending.add(pwp);
			}

			for (Point p : f.thresholds.trace(pp.saddle)) {
				if (p == pp.saddle) {
					continue;
				}

				PromPair cand = f.getThreshold(p);
				if (cand == null) {
					continue;
				}
				other.flushPendingParents(cand, p.equals(f.peak), this);
			}
			// make part of front merge?
			f.parentPending.addAll(other.parentPending);
		}
		
		void emitMST(Front f) {
			// TODO might change to emit only changed paths during coalesce steps rather than everything (but i think it'll be ok)

			// would be nice if we didn't need to compare elev and could just use the topology of backtrace to determine peaks/saddles
			for (Entry<Point, Point> e : f.bt.backtrace.entrySet()) {
				Point cur = e.getKey();
				Point next = e.getValue();
				boolean isSaddle = (Point.cmpElev(up).compare(cur, next) < 0);
				if (isSaddle) {
					continue;
				}
				
				Point nextNext = f.bt.get(next);
				emitBacktraceEdge(new Edge(cur.ix, nextNext.ix, next.ix));
			}			
		}

		void finalizeSubsaddles(Front f, Front other, Point saddle, Point peak) {
			for (Point sub : f.thresholds.traceUntil(saddle, peak, f.c).belowThresh) {
				if (this.isNotablyProminent(f.getThreshold(sub))) {
					emitFact(PromFact.subsaddleFact(sub, saddle, f.traceNums.get(saddle), PromFact.SS_ELEV));
				}
			}
		}
		
		void finalizeDomainSubsaddles(Front f, Front other, Point saddle, Point peak) {
			PromPair promThresh = null;
			for (Point sub : f.thresholds.traceUntil(saddle, peak, f.c).belowThresh) {
				PromPair subpp = f.getThreshold(sub);
				if (this.isNotablyProminent(subpp) &&
						(promThresh == null || subpp.compareTo(promThresh) > 0)) {
					promThresh = subpp;
					emitFact(PromFact.subsaddleFact(sub, saddle, f.traceNums.get(saddle), PromFact.SS_PROM));
				}
			}
		}
		
		void finalizeMST(Front f, Point saddle, Front other) {
			List<Point> path = new ArrayList<Point>();
			path.add(other != null ? other.bt.get(saddle) : null);
			for (Point p : f.bt.trace(saddle)) {
				path.add(p);
			}
			Collections.reverse(path);
			assert path.size() % 2 == 1;
			for (int i = 0; i + 1 < path.size(); i += 2) {
				Point cur = path.get(i);
				Point s = path.get(i+1);
				Point next = path.get(i+2);
				emitBacktraceEdge(new Edge(cur.ix, next != null ? next.ix : PointIndex.NULL, s.ix));
			}
		}

		public abstract void emitFact(PromFact pf);
		public abstract void emitPendingFront(Front f);
		public abstract void emitBacktraceEdge(Edge e);
	}
	
	static class Backtrace<K> {
		// for MST could we store just <Long, Long>?
		Map<Point, Point> backtrace; 
		Point root;
		Map<Point, K> extraInfo;
		
		public Backtrace() {
			this.backtrace = new HashMap<>();
			this.extraInfo = new HashMap<>();
		}
		
		public void add(Point p, Point parent) {
			if (parent == null) {
				root = p;
			} else {
				backtrace.put(p, parent);
			}
		}

		public void add(Point p, Point parent, K extra) {
			this.add(p, parent);
			extraInfo.put(p, extra);
		}
		
		public Point get(Point p) {
			Point parent = backtrace.get(p);
			if (parent == null && !p.equals(root)) {
				throw new RuntimeException("point not loaded [" + p + "]");
			}
			return parent;
		}
		
		public boolean contains(Point p) {
			return backtrace.containsKey(p) || p.equals(root);
		}
		
		public int size() {
			return backtrace.size();
		}
		
		void removeInCommon(Backtrace other, MeshPoint saddle) {
			for (Iterator<Point> it = other.backtrace.keySet().iterator(); it.hasNext(); ) {
				Point from = it.next();
				if (this.backtrace.containsKey(from) && !from.equals(saddle)) {
					this.remove(from);
					it.remove();
				}
			}
		}
		
		void remove(Point p) {
			this.backtrace.remove(p);
			this.extraInfo.remove(p);
		}
		
		public Iterable<Point> allPoints() {
			// TODO make this more efficient
			List<Point> points = new ArrayList<>(backtrace.keySet());
			points.add(root);
			return points;
		}
		
		// NOTE: does not support extraInfo
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
		
		// thresholds only
		public void mergeFromAsTree(Backtrace other, MeshPoint saddle, Comparator<Point> c) {
			removeInCommon(other, saddle);

			Point threshold = traceUntil(saddle, other.root, c).threshold;

			this.remove(saddle);
			other.remove(saddle);
			this.backtrace.putAll(other.backtrace);
			this.extraInfo.putAll(other.extraInfo);
			this.add(other.root, threshold);
		}
		
		static class TraceResult {
			// first point >= cutoff
			Point threshold;
			// all points between start and threshold, exclusive
			List<Point> belowThresh;
			
			public TraceResult() {
				belowThresh = new ArrayList<>();
			}
		}

		// thresholds only
		public TraceResult traceUntil(Point start, Predicate<Point> pred, boolean includeStart) {
			TraceResult result = new TraceResult();
			for (Point p : trace(start)) {
				if (p == start && !includeStart) {
					continue;
				}
				
				if (pred.test(p)) {
					result.threshold = p;
					break;
				}
				result.belowThresh.add(p);
			}
			return result;
		}
		
		// thresholds only
		public TraceResult traceUntil(Point saddle, Point cutoff, Comparator<Point> c) {
			return traceUntil(saddle, new Predicate<Point>() {
				@Override
				public boolean test(Point p) {
					return c.compare(p, cutoff) >= 0;
				}
			}, false);
		}
		
		// return all points between start and root, inclusive
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
					        extraInfo.remove(p);
					    }
					}
				}
			};
		}
		
		/*
		Point getCommonPoint(Point a, Point b) {
			List<Point> fromA = new ArrayList<Point>();
			List<Point> fromB = new ArrayList<Point>();
			Set<Point> inFromA = new HashSet<Point>();
			Set<Point> inFromB = new HashSet<Point>();

			Point intersection;
			Point curA = a;
			Point curB = b;
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
			return intersection;
		}
*/		
	}
	
	static class Thresholds extends Backtrace<Point> {
		public PromPair getProm(Point peak) {
			if (!contains(peak)) {
				throw new RuntimeException();
			}
			return new PromPair(peak, extraInfo.get(peak));
		}

		public void setProm(Point peak, Point saddle) {
			if (!contains(peak)) {
				throw new RuntimeException();
			}
			extraInfo.put(peak, saddle);
		}

		public Iterable<PromPair> allProms() {
			Iterator<Point> it = allPoints().iterator();
			return new SaneIterable<PromPair>() {
				@Override
				public PromPair genNext() {
					while (it.hasNext()) {
						Point p = it.next();
						PromPair pp = getProm(p);
						if (pp.saddle != null) {
							return pp;
						}
					}
					throw new NoSuchElementException();
				}
			};
		}
	}
	
	static class Front {
		// highest peak in the front
		MeshPoint peak;
		// list of saddles connecting to adjoining fronts
		MutablePriorityQueue<MeshPoint> queue; // the set of saddles delineating the cell for which 'peak' is the highest point
		// a path from saddles back to the peak
		Backtrace<Void> bt;
		
		Map<Point, Integer> traceNums;
		
		// a heap-like structure storing the 'next highest' points you'd encounter on a journey from each saddle back towards the peak
		Thresholds thresholds;
		
		// set of peaks in front that are still searching for a minimally prominent threshold ("line parent")
		Set<Long> pendingPThresh;
		// set of peaks in front that are still searching for prominence parents (mapped to their saddle)
		List<PeakWithoutParent> parentPending;
		
		Comparator<Point> c;

		static class PeakWithoutParent {
			PromPair prom;
			// below are relevant fields when this peak becomes a prominence subsaddle for other peaks
			// the highest peak that blocks reachability to this peak when delineating domain
			PromPair minVisibility;
			// trace# from this peak's saddle *away* from the peak
			int traceNumAway;
		}
		
		public Front(MeshPoint peak, boolean up) {
			this.peak = peak;
			this.c = Point.cmpElev(up);
			queue = new MutablePriorityQueue<MeshPoint>(new ReverseComparator<Point>(c));
			bt = new Backtrace<>();
			bt.add(peak, null);
			traceNums = new HashMap<>();
			thresholds = new Thresholds();
			thresholds.add(peak, null);
			
			pendingPThresh = new HashSet<Long>();
			parentPending = new ArrayList<>();
		}
		
		public PromPair pendProm() {
			return new PromPair(peak, first());
		}
		
		private PromPair getProm(PromPair pp) {
			if (pp.saddle != null) {
				return pp;
			} else if (pp.peak.equals(peak)) {
				return pendProm();
			} else {
				return null;
			}
		}
		
		public PromPair getThreshold(Point p) {
			return getProm(thresholds.getProm(p));
		}
		
		public void add(MeshPoint p, int traceNum, boolean initial) {
			queue.add(p);
			traceNums.put(p, traceNum);
			if (initial) {
				bt.add(p, peak);
				thresholds.add(p, peak);
			}
		}
		
		// remove element, return whether item existed
		// does not affect backtrace, etc.
		public boolean remove(MeshPoint p) {
			boolean inQueue = queue.remove(p);
			traceNums.remove(p);
			return inQueue;
		}
		
		public MeshPoint pop() {
			MeshPoint p = queue.poll();
			traceNums.remove(p);
			return p;
		}
		
		public MeshPoint first() {
			return queue.peek();
		}
		
		// assumes 'saddle' has already been popped from 'other'
		public boolean mergeFrom(Front other, MeshPoint saddle) {
			if (other.queue.contains(saddle)) {
				throw new RuntimeException("saddle still in other: " + saddle);
			}
			
			boolean firstChanged = first().equals(saddle);
			remove(saddle);
			
			for (MeshPoint s : other.queue) {
				add(s, other.traceNums.get(s), false);
			}

			bt.mergeFromAsNetwork(other.bt, saddle);
			thresholds.mergeFromAsTree(other.thresholds, saddle, this.c);
			
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
			for (Point p : queue) {
				btp.markPoint(p);
			}
			btp.prune();
		}
		
		public int size() {
			return queue.size();
		}
		
		public void flushPendingThresh(Point pthresh, Searcher s) {
			for (Long ix : pendingPThresh) {
				s.emitFact(PromFact.pthreshFact(new Point(ix, 0, 0), pthresh));
			}
			pendingPThresh.clear();
		}
		
		// we're in child front
		// cand is ascending thresholds up the parent
		public void flushPendingParents(PromPair cand, boolean nosubsaddle, Searcher s) {
			parentPending.removeIf(new Predicate<PeakWithoutParent>() {
				@Override
				public boolean test(PeakWithoutParent pwp) {
					PromPair pend = pwp.prom;
					if (cand.compareTo(pend) > 0) {
						s.emitFact(PromFact.parentageFact(pend.peak, cand.peak));
						return true;
					}
					return false;
				}				
			});
			if (!nosubsaddle) {
				// for those peaks whom 'cand' is not that parent, possibly emit prom subsaddles for cand
				for (PeakWithoutParent pwp : parentPending) {
					PromPair pend = pwp.prom;
					PromPair thresh = pwp.minVisibility;
					if (thresh == null || cand.compareTo(thresh) > 0) { // not gte
						s.emitFact(PromFact.subsaddleFact(cand.peak, pend.saddle, pwp.traceNumAway, PromFact.SS_PROM));
						pwp.minVisibility = cand;
					}
				}
			}
		}
		
		@DefaultCoder(AvroCoder.class)
		static class AvroFront {
			List<Point> points;
			boolean up;
			long peakIx;
			List<Long> queue;
			HashMap<Long, Long> prompairs;
			HashMap<Long, Integer> traceNums;
			HashMap<Long, Long> thresholds;
			HashMap<Long, Long> backtrace;
			List<PeakWithoutParent> parentPending;
			List<Long> pendingPThresh;

			@DefaultCoder(AvroCoder.class)
			static class PeakWithoutParent {
				long p;
				long minVisibility;
				int traceNum;
			}
			
			// for deserialization
			public AvroFront() {}
			
			public AvroFront(Front f) {
				Map<Point, Point> _prompairs = new HashMap<>();
				for (PromPair pp : f.thresholds.allProms()) {
					_prompairs.put(pp.peak, pp.saddle);
				}
				for (Front.PeakWithoutParent pwp : f.parentPending) {
					_prompairs.put(pwp.prom.peak, pwp.prom.saddle);
					if (pwp.minVisibility != null) {
						_prompairs.put(pwp.minVisibility.peak, pwp.minVisibility.saddle);						
					}
				}
				
				Set<Point> _mesh = new HashSet<Point>();
				_mesh.add(f.peak);
				_mesh.addAll(f.queue);
				_mesh.addAll(_prompairs.keySet());
				_mesh.addAll(_prompairs.values());
				f.thresholds.allPoints().forEach(_mesh::add);
				// backtrace points need elev to determine peaks/saddles
				_mesh.addAll(f.bt.backtrace.keySet());
				_mesh.addAll(f.bt.backtrace.values());
				points = new ArrayList<Point>();
				for (Point p : _mesh) {
					points.add(new Point(p));
				}

				up = (f.c == Point.cmpElev(true));
				peakIx = f.peak.ix;

				queue = new ArrayList<Long>();
				for (Point p : f.queue) {
					queue.add(p.ix);
				}

				thresholds = encodePointMap(f.thresholds.backtrace);
				prompairs = encodePointMap(_prompairs);
				backtrace = encodePointMap(f.bt.backtrace);
				
				traceNums = new HashMap<Long, Integer>();
				for (Entry<Point, Integer> e : f.traceNums.entrySet()) {
					traceNums.put(e.getKey().ix, e.getValue());
				}
				
				pendingPThresh = new ArrayList<>(f.pendingPThresh);
				parentPending = new ArrayList<>();
				for (Front.PeakWithoutParent _pwp : f.parentPending) {
					PeakWithoutParent pwp = new PeakWithoutParent();
					pwp.p = _pwp.prom.peak.ix;
					pwp.minVisibility = _pwp.minVisibility != null ? _pwp.minVisibility.peak.ix : PointIndex.NULL;
					pwp.traceNum = _pwp.traceNumAway;
					parentPending.add(pwp);
				}
			}
			
			public HashMap<Long, Long> encodePointMap(Map<? extends Point, ? extends Point> pointMap) {
				HashMap<Long, Long> ixMap = new HashMap<>();
				for (Entry<? extends Point, ? extends Point> e : pointMap.entrySet()) {
					ixMap.put(e.getKey().ix, e.getValue().ix);
				}
				return ixMap;
			}

			public Front toFront() {
				Map<Long, MeshPoint> mesh = new HashMap<>();
				for (Point p : points) {
					mesh.put(p.ix, new MeshPoint(p));
				}
				Map<Point, Point> prompoints = new HashMap<>();
				decodePointMap(prompairs, mesh, prompoints);
				
				Front f = new Front(mesh.get(peakIx), up);

				for (long ix : queue) {
					MeshPoint p = mesh.get(ix);
					f.add(p, traceNums.get(ix), false);
				}

				decodePointMap(thresholds, mesh, f.thresholds.backtrace);
				for (Point p : f.thresholds.allPoints()) {
					if (prompoints.containsKey(p)) {
						f.thresholds.extraInfo.put(p, prompoints.get(p));
					}
				}
				decodePointMap(backtrace, mesh, f.bt.backtrace);
				
				f.pendingPThresh.addAll(pendingPThresh);
				for (PeakWithoutParent _pwp : parentPending) {
					Front.PeakWithoutParent pwp = new Front.PeakWithoutParent();
					Point peak = mesh.get(_pwp.p);
					pwp.prom = new PromPair(peak, prompoints.get(peak));
					Point minVis = _pwp.minVisibility == PointIndex.NULL ? null : mesh.get(_pwp.minVisibility);
					if (minVis != null) {
						pwp.minVisibility = new PromPair(minVis, prompoints.get(minVis));
					}
					pwp.traceNumAway = _pwp.traceNum;
					f.parentPending.add(pwp);
				}
				
				return f;
			}
		
			static void decodePointMap(Map<Long, Long> ixMap, Map<Long, MeshPoint> mesh, Map<? super MeshPoint, ? super MeshPoint> pointMap) {
				for (Entry<Long, Long> e : ixMap.entrySet()) {
					MeshPoint k = mesh.get(e.getKey());
					MeshPoint v = mesh.get(e.getValue());
					pointMap.put(k, v);
				}
			}
		}
	}
	

}
