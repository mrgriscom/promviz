package com.mrgris.prominence;

import java.util.AbstractMap;
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
import com.mrgris.prominence.Prominence.Front.AvroFront;
import com.mrgris.prominence.Prominence.PromFact;
import com.mrgris.prominence.dem.DEMFile;
import com.mrgris.prominence.util.DefaultMap;
import com.mrgris.prominence.util.MutablePriorityQueue;
import com.mrgris.prominence.util.ReverseComparator;
import com.mrgris.prominence.util.SaneIterable;

public class Prominence extends DoFn<KV<Prefix, Iterable<KV<Long, Iterable<HalfEdge>>>>, PromFact> {
	
    private static final Logger LOG = LoggerFactory.getLogger(Prominence.class);
	
	boolean up;
	double cutoff;
	PCollectionView<Map<Prefix, Iterable<DEMFile>>> coverageSideInput;
	TupleTag<AvroFront> pendingFrontsTag;
	
	public Prominence(boolean up, double cutoff, PCollectionView<Map<Prefix, Iterable<DEMFile>>> coverageSideInput,
			TupleTag<AvroFront> pendingFrontsTag) {
		this.up = up;
		this.cutoff = cutoff;
		this.coverageSideInput = coverageSideInput;
		this.pendingFrontsTag = pendingFrontsTag;
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
		
		public PromFact() {
			elevSubsaddles = new ArrayList<>();
			promSubsaddles = new ArrayList<>();
		}
	}
	
    @ProcessElement
    public void processElement(ProcessContext c) {
    	Prefix prefix = c.element().getKey();
    	Iterable<KV<Long, Iterable<HalfEdge>>> protoFronts = c.element().getValue();
    	Map<Prefix, Iterable<DEMFile>> coverage = c.sideInput(coverageSideInput);
    	
    	new Searcher(up, cutoff, protoFronts, coverage) {
    		public void emitFact(PromFact pf) {
    			c.output(pf);
    		}
    		
    		public void emitPendingFront(Front f) {
    			c.output(pendingFrontsTag, new AvroFront(f));
    		}
    		
    		public void emitBacktraceEdge(Edge e) {
    			
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
		boolean baseLevel = false;
		
		Set<Front> fronts;
		// the highest saddle directly connecting two fronts (both forward and reverse mapping)
		Map<MeshPoint, Set<Front>> connectorsBySaddle;
		Map<Set<Front>, MeshPoint> connectorsByFrontPair;
		// front pairs that can and must be coalesced
		MutablePriorityQueue<FrontMerge> pendingMerges;
		Set<Edge> mst;
		// used solely for tracking segments of MST that may need to be reversed from higher coalescing steps
		Map<Front, List<PromPair>> merged;
		
		public Searcher(boolean up, double cutoff) {
			this.up = up;
			this.cutoff = cutoff;
			
			mst = new HashSet<Edge>();
			merged = new DefaultMap<Front, List<PromPair>>() {
				public List<PromPair> defaultValue(Front key) {
					return new ArrayList<PromPair>();
				}
			};

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

			//makeMST();
			
			for (Front f : fronts) {
				f.prune();
				emitPendingFront(f);
			}
		}
		
		public void finalizePending() {
			finalizeRemaining();
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
			baseLevel = true;
			
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
				
				for (long saddleIx : nonBasinSaddles) {
					MeshPoint saddle = mesh.get(saddleIx);
					f.add(saddle, true);
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
			boolean notable = isNotablyProminent(newProm);
			
			if (!baseLevel) {
				merged.get(parent).add(newProm);
				if (merged.containsKey(child)) {
					merged.get(parent).addAll(merged.get(child));
					merged.remove(child);
				}
			}

			///////
			List<Point> childThreshes = child.thresholds.traceUntil(saddle, newProm.peak, child.c).getValue();
			Entry<Point, List<Point>> e = parent.thresholds.traceUntil(saddle, newProm.peak, parent.c);
			Point thresh = e.getKey();
			List<Point> parentThreshes = e.getValue();

			PromPair promThresh = null;
			for (Point sub : childThreshes) {
				if (child.promPoints.containsKey(sub)) {
					PromFact ps = new PromFact();
					ps.p = new Point(sub);
					ps.elevSubsaddles.add(new PromFact.Saddle(saddle, -1 /* child front */));
					emitFact(ps);
					
					PromPair subpp = new PromPair(sub, child.promPoints.get(sub));
					if (promThresh == null || subpp.compareTo(promThresh) > 0) {
						promThresh = subpp;

						PromFact pps = new PromFact();
						pps.p = new Point(sub);
						pps.promSubsaddles.add(new PromFact.Saddle(saddle, -1 /* child front */));
						emitFact(pps);
					}
				}
			}
			for (Point sub : parentThreshes) {
				if (parent.promPoints.containsKey(sub)) {
					PromFact ps = new PromFact();
					ps.p = new Point(sub);
					ps.elevSubsaddles.add(new PromFact.Saddle(saddle, -1 /* parent front */));
					emitFact(ps);
				}
			}
			///////
			
			if (notable || !child.pendingPThresh.isEmpty()) {
				this.pthresh(newProm, thresh, parent, child);
			}
			if (notable) {
				child.flushPendingParents(newProm, null, null, false, this);
			}
			List<Point[]> newParents = new ArrayList<Point[]>(); // temp list because we can't do path stuff till post-merge
			if (notable || !child.pendingParent.isEmpty()) {
				parentage(newProm, parent, child, newParents);
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
					parent.flushPendingParents(parent.pendProm(), null, newParents, true, this);
				}
			}

			if (notable) {
				parent.promPoints.put((MeshPoint)newProm.peak, (MeshPoint)newProm.saddle);

				PromFact base = new PromFact();
				base.p = new Point(newProm.peak);
				base.saddle = new PromFact.Saddle(newProm.saddle, -1);
				base.thresh = new Point(thresh);
				emitFact(base);
			}
			for (Point[] childparent : newParents) {
				Point p = childparent[0];
				Point par = childparent[1];
				PromFact parentInfo = new PromFact();
				parentInfo.p = new Point(p);
				parentInfo.parent = new Point(par);
				emitFact(parentInfo);
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
			Point pthresh = thresh;
			while (!isNotablyProminent(parent.getProm(pthresh)) && !pthresh.equals(parent.peak)) {
				pthresh = parent.thresholds.get(pthresh);
			}
			if (isNotablyProminent(pp)) {
				child.pendingPThresh.add(pp.peak.ix);
			}
			if (isNotablyProminent(parent.getProm(pthresh))) {
				child.flushPendingThresh(pthresh, this);
			} else {
				parent.pendingPThresh.addAll(child.pendingPThresh);
			}
		}

		void parentage(PromPair pp, Front f, Front other, List<Point[]> newParents) {
			if (isNotablyProminent(pp)) {
				other.pendingParent.put((MeshPoint)pp.peak, (MeshPoint)pp.saddle);
				other.pendingParentThresh.put((MeshPoint)pp.peak, (MeshPoint)pp.peak);
			}

			for (Point p : f.thresholds.trace(pp.saddle)) {
				if (p == pp.saddle) {
					continue;
				}
				
				PromPair cand = f.getProm(p);
				if (cand == null) {
					continue;
				}
				other.flushPendingParents(cand, f, newParents, p.equals(f.peak), this);
			}
			f.pendingParent.putAll(other.pendingParent);
			f.pendingParentThresh.putAll(other.pendingParentThresh);
		}
		
		void makeMST() {
			if (baseLevel) {
				makeMSTBase();
			} else {
				makeMSTCoalesce();
			}
		}

		void makeMSTBase() {
			for (Front f : fronts) {
				for (Entry<Point, Point> e : f.bt.backtrace.entrySet()) {
					Point cur = e.getKey();
					Point next = e.getValue();
					boolean isSaddle = (Point.cmpElev(up).compare(cur, next) < 0);
					if (isSaddle) {
						continue;
					}
					
					Point nextNext = f.bt.get(next);
					mst.add(new Edge(cur.ix, nextNext.ix, next.ix));
				}
			}
		}
		
		void makeMSTCoalesce() {
			for (Entry<Front, List<PromPair>> e : merged.entrySet()) {
				Front f = e.getKey();
				for (PromPair pp : e.getValue()) {
					Point intersection = f.bt.getCommonPoint(pp.peak, pp.saddle);
					// check if intersection == saddle here, move saddle one up if so
					
					List<Point> path = new ArrayList<Point>();
					boolean breakNext = false;
					for (Point p : f.bt.trace(pp.peak)) {
						path.add(p);
						if (breakNext) {
							break;
						}
						if (p.equals(intersection)) {
							if (intersection.equals(pp.saddle)) {
								breakNext = true;
							} else {
								break;
							}
						}
					}
					assert path.size() % 2 == 1;
					for (int i = 0; i + 1 < path.size(); i += 2) {
						Point cur = path.get(i);
						Point saddle = path.get(i+1);
						Point next = path.get(i+2);
						mst.add(new Edge(cur.ix, next.ix, saddle.ix));
					}
				}
			}
		}

		void finalizeRemaining() {
			//Logging.log("finalizing remaining");
			for (Front f : fronts) {
				if (f.first() == null) {
					//Logging.log("'global' max: ignoring");
					continue;
				}

				PromPair pp = new PromPair(f.peak, f.first());
				if (isNotablyProminent(pp)) {
					PromFact pend = new PromFact();
					pend.p = new Point(pp.peak);
					pend.saddle = new PromFact.Saddle(pp.saddle, -1);
					emitFact(pend);
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
					f.flushPendingParents(pp, null, null, false, this);
				}
				
				//finalizeMST(f, saddle, other);
			}
		}		
		
		void finalizeSubsaddles(Front f, Front other, Point saddle, Point peak) {
			Map.Entry<Point, List<Point>> e = f.thresholds.traceUntil(saddle, peak, f.c);
			for (Point sub : e.getValue()) {
				if (this.isNotablyProminent(f.getProm(sub))) {
					PromFact ps = new PromFact();
					ps.p = new Point(sub);
					ps.elevSubsaddles.add(new PromFact.Saddle(saddle, -1 /* front f */));
					emitFact(ps);
				}
			}
		}
		
		void finalizeDomainSubsaddles(Front f, Front other, Point saddle, Point peak) {
			Map.Entry<Point, List<Point>> e = f.thresholds.traceUntil(saddle, peak, f.c);
			PromPair promThresh = null;
			for (Point sub : e.getValue()) {
				PromPair subpp = f.getProm(sub);
				if (this.isNotablyProminent(subpp) &&
						(promThresh == null || subpp.compareTo(promThresh) > 0)) {
					promThresh = subpp;

					PromFact pps = new PromFact();
					pps.p = new Point(sub);
					pps.promSubsaddles.add(new PromFact.Saddle(saddle, -1 /* front f */));
					emitFact(pps);
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
				mst.add(new Edge(cur.ix, next != null ? next.ix : PointIndex.NULL, s.ix));
			}
		}

		/*
		Path pathToUnknown(Front f) {
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
			return new Path(Lists.reverse(path), null);
		}
		*/
		
		public abstract void emitFact(PromFact pf);
		public abstract void emitPendingFront(Front f);
	}
	
	public static class Path {
		public List<Long> path;
		double thresholdFactor = -1;
		public double[] threshCoords = null;
		
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
			
		public List<double[]> _finalize() {
			List<double[]> cpath = new ArrayList<double[]>();
			for (long ix : path) {
				cpath.add(PointIndex.toLatLon(ix));
			}
			if (thresholdFactor >= 0) {
				// this is a stop-gap; actually threshold should be determined by following chase from saddle
				double[] last = cpath.get(0);
				double[] nextToLast = cpath.get(1);
				for (int i = 0; i < 2; i++) {
					last[i] = last[i] * thresholdFactor + nextToLast[i] * (1. - thresholdFactor);
				}
			}
			threshCoords = cpath.get(0);
			return cpath;
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
		
		public void mergeFromAsTree(Backtrace other, MeshPoint saddle, Comparator<Point> c) {
			removeInCommon(other, saddle);

			Map.Entry<Point, List<Point>> e = traceUntil(saddle, other.root, c);
			Point threshold = e.getKey();
			
			this.backtrace.remove(saddle);
			other.backtrace.remove(saddle);
			this.backtrace.putAll(other.backtrace);
			this.add(other.root, threshold);
		}
		
		public Map.Entry<Point, List<Point>> traceUntil(Point saddle, Point cutoff, Comparator<Point> c) {
			Point threshold = null;
			List<Point> belowThresh = new ArrayList<Point>();
			for (Point p : trace(saddle)) {
				if (p == saddle) {
					continue;
				}
				
				if (c.compare(p, cutoff) > 0 || p.equals(cutoff)) {
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
		
		/*
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
		*/
	}
	
	static class Front {
		// highest peak in the front
		MeshPoint peak;
		// list of saddles connecting to adjoining fronts
		MutablePriorityQueue<MeshPoint> queue; // the set of saddles delineating the cell for which 'peak' is the highest point
		// a path from 'key points' back to the highest peak
		Backtrace bt;
		// a heap-like structure storing the 'next highest' points you'd encounter on a journey from each saddle back towards the peak
		Backtrace thresholds;
		
		// mapping of notably prominent peaks to saddles within the front
		Map<MeshPoint, MeshPoint> promPoints;
		// set of peaks in front that are still searching for a minimally prominent threshold ("line parent")
		Set<Long> pendingPThresh;
		// set of peaks in front that are still searching for prominence parents (mapped to their saddle)
		Map<MeshPoint, MeshPoint> pendingParent; // could probably be a set, since saddles are stored in promPoints?
		Map<MeshPoint, MeshPoint> pendingParentThresh;
		
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
			pendingParentThresh = new HashMap<MeshPoint, MeshPoint>();
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
		public boolean mergeFrom(Front other, MeshPoint saddle) {
			if (other.queue.contains(saddle)) {
				throw new RuntimeException("saddle still in other: " + saddle);
			}
			
			boolean firstChanged = first().equals(saddle);
			remove(saddle);
			
			for (MeshPoint s : other.queue) {
				add(s, false);
			}

			promPoints.putAll(other.promPoints);
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
			for (Point p : thresholds.backtrace.keySet()) {
				btp.markPoint(p);
			}
			for (Point p : pendingParent.keySet()) {
				btp.markPoint(p);
			}
			btp.prune();
			
			Set<Point> allParentThreshes = new HashSet<Point>(pendingParentThresh.values());
			for (Iterator<MeshPoint> it = promPoints.keySet().iterator(); it.hasNext(); ) {
				Point p = it.next();
				if (!thresholds.backtrace.containsKey(p) &&
						!pendingParent.containsKey(p) &&
						!allParentThreshes.contains(p)) {
					it.remove();
				}
			}
		}
		
		public int size() {
			return queue.size();
		}
		
		public void flushPendingThresh(Point pthresh, Searcher s) {
			for (Iterator<Long> it = pendingPThresh.iterator(); it.hasNext(); ) {
				PromFact pt = new PromFact();
				pt.p = new Point(it.next(), 0);
				pt.pthresh = new Point(pthresh);
				s.emitFact(pt);

				it.remove();
			}
		}
		
		// we're in child front
		// cand is ascending thresholds up the parent
		
		public void flushPendingParents(PromPair cand, Front other, List<Point[]> newParents, boolean nosubsaddle, Searcher s) {
			for (Iterator<Entry<MeshPoint, MeshPoint>> it = this.pendingParent.entrySet().iterator(); it.hasNext(); ) {
				Entry<MeshPoint, MeshPoint> e = it.next();
				PromPair pend = new PromPair(e.getKey(), e.getValue());
				if (cand.compareTo(pend) > 0) {
					newParents.add(new Point[] {pend.peak, cand.peak});
					it.remove();
					this.pendingParentThresh.remove(pend.peak);
				}
			}
			if (!nosubsaddle) {
				// cand prom <= pend prom
				// cand thresh is NOT prom parent for pend
				
				// threshes is really like a step tree
				// 'i am blocked by peaks with this prominence, so they should get the subsaddle not me'
				
				// adds a prom subsaddle for CAND (saddle for pend
				for (Entry<MeshPoint, MeshPoint> e : this.pendingParent.entrySet()) {
					PromPair pend = new PromPair(e.getKey(), e.getValue());
					Point _thresh = this.pendingParentThresh.get(pend.peak);
					PromPair thresh;
					if (_thresh.equals(pend.peak)) {
						thresh = null;
					} else {
						// yikes
						thresh = this.getProm(_thresh);
						if (thresh == null) {
							assert other != null;
							thresh = other.getProm(_thresh);
						}
						assert thresh != null;
					}

					if (thresh == null || thresh.compareTo(cand) < 0) { // not lte
						PromFact pps = new PromFact();
						pps.p = new Point(cand.peak);
						pps.promSubsaddles.add(new PromFact.Saddle(pend.saddle, -1 /* trace *away* from pend.peak */));
						s.emitFact(pps);
						
						this.pendingParentThresh.put((MeshPoint)pend.peak, (MeshPoint)cand.peak);
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
			HashMap<Long, Long> thresholds;
			HashMap<Long, Long> backtrace;
			HashMap<Long, Long> promPoints;
			HashMap<Long, Long> pendingParent;
			HashMap<Long, Long> pendingParentThresh;
			List<Long> pendingPThresh;

			// for deserialization
			public AvroFront() {}
			
			public AvroFront(Front f) {
				Set<Point> _mesh = new HashSet<Point>();
				_mesh.add(f.peak);
				_mesh.addAll(f.queue);
				_mesh.addAll(f.thresholds.backtrace.keySet());
				_mesh.addAll(f.thresholds.backtrace.values());
				_mesh.addAll(f.bt.backtrace.keySet());
				_mesh.addAll(f.bt.backtrace.values());
				_mesh.addAll(f.promPoints.keySet());
				_mesh.addAll(f.promPoints.values());
				_mesh.addAll(f.pendingParent.keySet());
				_mesh.addAll(f.pendingParent.values());
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
				backtrace = encodePointMap(f.bt.backtrace);
				promPoints = encodePointMap(f.promPoints);
				
				pendingPThresh = new ArrayList<>(f.pendingPThresh);
				pendingParent = encodePointMap(f.pendingParent);
				pendingParentThresh = encodePointMap(f.pendingParentThresh);

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
				
				Front f = new Front(mesh.get(peakIx), up);

				for (long ix : queue) {
					MeshPoint p = mesh.get(ix);
					f.add(p, false);
				}

				decodePointMap(thresholds, mesh, f.thresholds.backtrace);
				decodePointMap(backtrace, mesh, f.bt.backtrace);
				decodePointMap(promPoints, mesh, f.promPoints);
				
				f.pendingPThresh.addAll(pendingPThresh);		
				decodePointMap(pendingParent, mesh, f.pendingParent);
				decodePointMap(pendingParentThresh, mesh, f.pendingParentThresh);
				
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
