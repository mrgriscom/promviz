package promviz;

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
import java.util.PriorityQueue;
import java.util.Set;

import promviz.Prominence.Backtrace.BacktracePruner;
import promviz.debug.Harness;
import promviz.dem.DEMFile;
import promviz.dem.DEMFile.Sample;
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
 */

public class Prominence {

	public static interface OnProm {
		void onprom(PromInfo pi);
	}
	
	public static void promSearch(List<DEMFile> DEMs, boolean up, double cutoff) {
		Map<Prefix, Set<DEMFile>> coverage = PagedElevGrid.partitionDEM(DEMs);
		
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
		double cutoff;
		OnProm onprom;
		Map<Prefix, Set<DEMFile>> coverage;
	}
	
	static class ChunkOutput {
		Prefix p;
	}
	
//	static class PromSearch extends WorkerPool<ChunkInput, ChunkOutput> {
	static class PromSearch extends WorkerPoolDebug<ChunkInput, ChunkOutput> {

		int numWorkers;
		Map<Prefix, Set<DEMFile>> coverage;
		boolean up;
		double cutoff;
		OnProm onprom;
		
		public PromSearch(int numWorkers, Map<Prefix, Set<DEMFile>> coverage, boolean up, double cutoff, OnProm onprom) {
			this.numWorkers = numWorkers;
			this.coverage = coverage;
			this.up = up;
			this.cutoff = cutoff;
			this.onprom = onprom;
		}

		public void baseCellSearch() {
			Set<Prefix> chunks = TopologyBuilder.enumerateChunks(coverage);
			Logging.log(chunks.size() + " network chunks");
			launch(numWorkers, Iterables.transform(chunks, new Function<Prefix, ChunkInput>() {
				public ChunkInput apply(Prefix p) {
					return makeInput(p, up, cutoff, onprom);
				}
			}));
		}
		
		public boolean needsCoalesce() {
			// whether we need to aggregate at the next highest level
			return false;
		}
		
		public void coalesce() {
			// aggregate at the next highest level

//			this.launch(numWorkers, Iterables.transform(chunks, new Function<Prefix, ChunkInput>() {
//				public ChunkInput apply(Prefix p) {
//					return makeInput(p, upChunks, downChunks);
//				}
//			}));

		}
		
		public ChunkOutput process(ChunkInput input) {
			return new ChunkProcessor(input).build();
		}

		public void postprocess(int i, ChunkOutput output) {
			Logging.log(i + " " + output.p);
			
			// write mst?
		}

		ChunkInput makeInput(Prefix p, boolean up, double cutoff, OnProm onprom) {
			ChunkInput ci = new ChunkInput();
			ci.p = p;
			ci.up = up;
			ci.cutoff = cutoff;
			ci.onprom = onprom;
			ci.coverage = (HashMap)((HashMap)coverage).clone();
			return ci;
		}
	}
	
	static class ChunkProcessor {
		ChunkInput input;
		Prefix prefix;
		boolean up;
		PagedElevGrid mesh;

		Map<Point, Front> fronts;
		Map<MeshPoint, Set<Front>> connectorsBySaddle;
		Map<Set<Front>, MeshPoint> connectorsByFrontPair;
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
			load();
			
			Logging.log("before " + fronts.size());
			while (pendingMerges.size() > 0) {
				Iterator<FrontMerge> it = pendingMerges.iterator();
				FrontMerge toMerge = it.next();
				it.remove();
				mergeFronts(toMerge.parent, toMerge.child);
			}
			Logging.log("after " + fronts.size());
			
			// dump mst
			// dump pending fronts

			ChunkOutput output = new ChunkOutput();
			output.p = prefix;
			return output;
		}
		
		static interface EdgeProcessor {
			void process(long summitIx, long saddleIx);
		}
		
		static class FrontMerge {
			Front parent;
			Front child;
			
			FrontMerge(Front child, Front parent) {
				this.parent = parent;
				this.child = child;
			}
			
			public boolean equals(Object o) {
				FrontMerge fm = (FrontMerge)o;
				return this.parent == fm.parent && this.child == fm.child;
			}
			
			public int hashCode() {
				return parent.hashCode() ^ child.hashCode();
			}
		}
		
		public void load() {
			fronts = new DefaultMap<Point, Front>() {
				public Front defaultValue(Point key) {
					return new Front(Point.cmpElev(up));
				}
			};
			connectorsBySaddle = new HashMap<MeshPoint, Set<Front>>();
			connectorsByFrontPair = new HashMap<Set<Front>, MeshPoint>();
			pendingMerges = new HashSet<FrontMerge>();
			
			List<Edge> edges = Lists.newArrayList(FileUtil.loadEdges(up, prefix, FileUtil.PHASE_RAW));
			
			final Set<Prefix> pages = new HashSet<Prefix>();
			processEdges(edges, new EdgeProcessor() {
				public void process(long summitIx, long saddleIx) {
					pages.add(PagedElevGrid.segmentPrefix(summitIx));
					pages.add(PagedElevGrid.segmentPrefix(saddleIx));
				}
			});
			mesh.bulkLoadPage(pages);

			processEdges(edges, new EdgeProcessor() {
				public void process(long summitIx, long saddleIx) {
					MeshPoint summit = mesh.get(summitIx);
					MeshPoint saddle = mesh.get(saddleIx);
					fronts.get(summit).add(summit, null);
					fronts.get(summit).add(saddle, summit);
				}
			});
			for (Front f : fronts.values()) {
				f.next(); // remove summit  TODO remove this hackiness
			}
			
			Map<MeshPoint, Set<Front>> saddlesToFronts = new DefaultMap<MeshPoint, Set<Front>>() {
				public Set<Front> defaultValue(MeshPoint key) {
					return new HashSet<Front>();
				}
			};
			for (Front f : fronts.values()) {
				for (MeshPoint saddle : f.set){
					saddlesToFronts.get(saddle).add(f);
				}
			}
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
			for (Entry<Set<Front>, List<MeshPoint>> e : frontPairsToSaddles.entrySet()) {
				Set<Front> fp = e.getKey();
				List<MeshPoint> saddles = e.getValue();
				MeshPoint highest = Collections.max(saddles, Point.cmpElev(up));
				for (MeshPoint saddle : saddles) {
					if (saddle == highest) {
						continue;
					}
					for (Front f : fp) {
						f.remove(saddle);
					}
				}
				connectorsBySaddle.put(highest, fp);
				connectorsByFrontPair.put(fp, highest);
			}
			for (Front f : fronts.values()) {
				Front parent = yonderFront(f);
				if (parent != null && f.c.compare(f.root(), parent.root()) < 0) {
					pendingMerges.add(new FrontMerge(f, parent));
				}
			}
			
			Logging.log("" + pendingMerges.size());
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
		
		Front yonderFront(Front f) {
			return otherFront(f, f.peekNext());
		}
		
		Front otherFront(Front f, MeshPoint saddle) {
			Set<Front> fs = connectorsBySaddle.get(saddle);
			if (fs == null) {
				return null;
			}
			for (Front ff : fs) {
				if (ff != f) {
					return ff;
				}
			}
			// should never reach here
			assert false;
			return null;
			
		}
		
		Set<Front> setPair(Front a, Front b) {
			Set<Front> s = new HashSet<Front>();
			s.add(a);
			s.add(b);
			return s;
		}
		
		void mergeFronts(Front parent, Front child) {
			MeshPoint saddle = child.next();
			
			PromInfo pi = new PromInfo(child.root(), saddle);
			pi.up = up;
			pi._finalizeDumb();
			
			if (pi.prominence() >= input.cutoff) {
				input.onprom.onprom(pi);
			}

			fronts.remove(child.root());

			Set<Front> pc = setPair(parent, child);
			connectorsBySaddle.remove(saddle);
			connectorsByFrontPair.remove(pc);

			for (MeshPoint s : child.set) {
				parent.add(s, child.root()); // TODO fix backtracing

				Front f = otherFront(child, s);
				if (f == null) {
					continue;
				}
				
				Set<Front> childX = setPair(child, f);
				Set<Front> parentX = setPair(parent, f);

				connectorsBySaddle.remove(s);
				connectorsByFrontPair.remove(childX);
				
				MeshPoint newS;
				MeshPoint competingS = connectorsByFrontPair.get(parentX);
				if (competingS != null && parent.c.compare(competingS, s) > 0) {
					newS = competingS;
				} else {
					newS = s;
				}
				connectorsBySaddle.put(newS, parentX);
				connectorsByFrontPair.put(parentX, newS);
				
				FrontMerge possMerge = new FrontMerge(f, child);
				if (pendingMerges.contains(possMerge)) {
					pendingMerges.remove(possMerge);
					pendingMerges.add(new FrontMerge(f, parent));
				}
			}

			boolean rependParent = saddle.equals(parent.peekNext());
			parent.remove(saddle);
			if (rependParent) {
				Front pparent = yonderFront(parent);
				if (pparent != null && parent.c.compare(parent.root(), pparent.root()) < 0) {
					pendingMerges.add(new FrontMerge(parent, pparent));
				}
			}

		}
		
		/*

queue of all adjacencies to check/process?

for cell, get highest saddle in front
if saddle not in other front -- pending, nothing can do atm
if other cell has higher peak, merge

		 */
	}
	
	public static class PromInfo {
		public boolean up;
		public MeshPoint p;
		public MeshPoint saddle;
		public boolean global_max;
		public boolean min_bound_only;
		public List<Long> path;
		public boolean forwardSaddle; // danger
		public double thresholdFactor = -1;
		
		public PromInfo(MeshPoint peak, MeshPoint saddle) {
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
			forwardSaddle = true;
		}

		public void finalizeBackward(Front front) {
			Point thresh = front.searchThreshold(this.p, this.saddle);			
			Path _ = new Path(front.bt.getAtoB(thresh, this.p), this.p);
			this.path = _.path;
			this.thresholdFactor = _.thresholdFactor;
			forwardSaddle = false;
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
				throw new RuntimeException("point not loaded");
			}
			return parent;
		}
		
		public boolean isLoaded(Point p) {
			return backtrace.containsKey(p) || p.equals(root);
		}
		
		public int size() {
			return backtrace.size();
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
					Iterator<Point> iterBT = backtrace.keySet().iterator();
					while (iterBT.hasNext()) {
						Point p = iterBT.next();
						if (!backtraceKeep.contains(p)) {
					        iterBT.remove();
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

		void load(MeshPoint p, TopologyNetwork tree, boolean isPeak) {
			MeshPoint next;
			while (!isLoaded(p)) {
				if (isPeak) {
					next = tree.get(p.getByTag(1, true));
				} else {
					next = tree.get(p.getByTag(0, false));					
				}
				this.add(p, next);
				p = next;
				isPeak = !isPeak;
			}
		}		
	}
	
	static class Front {
		PriorityQueue<MeshPoint> queue; // the search front, akin to an expanding contour
		Set<MeshPoint> set; // set of all points in 'queue'
		Backtrace bt;
//		int pruneThreshold = 1; // this could start out much larger (memory-dependent) to avoid
//		                        // unnecessary pruning in the early stages

		Map<MeshPoint, MeshPoint> forwardSaddles;
		Map<MeshPoint, MeshPoint> backwardSaddles;
		
		Comparator<Point> c;

		public Front(final Comparator<Point> c) {
			this.c = c;
			queue = new PriorityQueue<MeshPoint>(10, new ReverseComparator<Point>(c));
			set = new HashSet<MeshPoint>();
			bt = new Backtrace();
			
			forwardSaddles = new HashMap<MeshPoint, MeshPoint>();
			backwardSaddles = new HashMap<MeshPoint, MeshPoint>();
		}
		
		public boolean add(MeshPoint p, MeshPoint parent) {
			boolean newItem = set.add(p);
			if (newItem) {
				queue.add(p);
				bt.add(p, parent);
			}
			return newItem;
		}

		// remove element, return whether item existed
		// does not affect backtrace, etc.
		public boolean remove(MeshPoint p) {
			// we can't remove from the middle of the queue, so next() ignores items already removed
			// we do ensure that peek() always yields a valid item, though
			boolean removed = set.remove(p);
			ensureNextIsValid();
			return removed;
		}
		
		public MeshPoint next() {
			MeshPoint p = queue.poll();
			if (p != null) {
				set.remove(p);
				ensureNextIsValid();
			}
			return p;
		}
		
		void ensureNextIsValid() {
			while (true) {
				MeshPoint _n = peekNext();
				if (_n == null || set.contains(_n)) {
					break;
				}
				next();
			}
		}
		
		public MeshPoint peekNext() {
			return queue.peek();
		}

		public MeshPoint root() {
			return (MeshPoint)bt.root;
		}
		
//		public void prune(Collection<MeshPoint> pendingPeaks, Collection<MeshPoint> pendingSaddles) {
//			// when called, front must contain only saddles
//
//			// TODO: could this be made to work generationally (i.e., only deal with the
//			// portion of the front that has changed since the last prune)
//			
//			long startAt = System.currentTimeMillis();
//			if (seen.size() <= pruneThreshold) {
//				return;
//			}
//			
//			seen.retainAll(this.adjacent());
//			pruneThreshold = Math.max(pruneThreshold, 2 * seen.size());
//
//			BacktracePruner btp = bt.pruner();
//
//			// concession for 'old school' mode
//			if (pendingPeaks == null) {
//				pendingPeaks = new ArrayList<MeshPoint>();
//				pendingSaddles = new ArrayList<MeshPoint>();
//			}
//			
//			for (Point p : Iterables.concat(queue, pendingPeaks)) {
//				btp.markPoint(p);
//			}
//			Set<Point> bookkeeping = new HashSet<Point>();
//			Set<Point> significantSaddles = new HashSet<Point>(pendingSaddles);
//			for (Point p : Iterables.concat(queue, pendingSaddles)) {
//				bulkSearchThresholdStart(p, btp, bookkeeping, significantSaddles);
//			}
//			btp.prune();
//
//			Iterator<Point> iterFS = forwardSaddles.keySet().iterator();
//			while (iterFS.hasNext()) {
//				Point p = iterFS.next();
//				if (!significantSaddles.contains(p)) {
//			        iterFS.remove();
//			    }
//			}
//			Iterator<Point> iterBS = backwardSaddles.keySet().iterator();
//			while (iterBS.hasNext()) {
//				Point p = iterBS.next();
//				if (!significantSaddles.contains(p)) {
//			        iterBS.remove();
//			    }
//			}
//						
//			double runTime = (System.currentTimeMillis() - startAt) / 1000.;
//			Logging.log(String.format("prooned [%.2fs] %d %d %d %d", runTime, queue.size(), bt.size(), forwardSaddles.size(), backwardSaddles.size()));
//		}
		
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
//			System.err.println("infinite loop failsafe exceeded " + p);
//			return saddle;
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
		
		
	}

	
	
	// temporary
	static class TopologyNetwork implements IMesh {
		PagedElevGrid mesh;
		Iterable<Sample> samples;
		boolean up;
		
		Map<Long, MeshPoint> points;
		Set<MeshPoint> pendingSaddles;
		
		public TopologyNetwork(boolean up, Map<Prefix, Set<DEMFile>> coverage) {
			this.up = up;
			this.mesh = new PagedElevGrid(coverage, 1 << 29);
			this.samples = this.mesh.bulkLoadPage(coverage.keySet());
			
			points = new HashMap<Long, MeshPoint>();
			pendingSaddles = new HashSet<MeshPoint>();
		}


		void addPoint(long ix) {
			if (!points.containsKey(ix)) {
				points.put(ix, new MeshPoint(ix, mesh.get(ix).elev));
			}
		}
		
		@Override
		public MeshPoint get(long ix) {
			return points.get(ix);
		}
		
		Set<MeshPoint> adjacent(MeshPoint p) {
			return new HashSet<MeshPoint>(getPoint(p).adjacent(this));
		}
		
		MeshPoint getPoint(Point p) {
			return get(p.ix);
		}
	}

	

}
