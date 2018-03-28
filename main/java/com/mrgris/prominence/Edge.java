package com.mrgris.prominence;

import java.util.Objects;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import com.mrgris.prominence.util.Util;

@DefaultCoder(AvroCoder.class)
public class Edge {

	static final int TAG_NULL = -1;
	
	public long a;
	public long b;
	public long saddle;
	public int tagA;
	public int tagB;

	// for deserialization
	public Edge() {}
	
	public Edge(long a, long b, long saddle, int tagA, int tagB) {
		if (saddle == PointIndex.NULL || a == saddle || b == saddle ||
				(a == PointIndex.NULL && b == PointIndex.NULL) ||
				(a == b && tagA == tagB)) {
			throw new RuntimeException("invalid edge: " + String.format("%s %s %s", Util.print(a), Util.print(b), Util.print(saddle)));
		}
		
		this.a = a;
		this.b = b;
		this.saddle = saddle;
		this.tagA = tagA;
		this.tagB = tagB;
	}
	
	// needed?
	public void normalize() {
		if (this.a == PointIndex.NULL) {
			reverse();
		}
	}
	
	public Edge(long a, long b, long saddle) {
		this(a, b, saddle, TAG_NULL, TAG_NULL);
	}
	
	@DefaultCoder(AvroCoder.class)
	public static class HalfEdge {
		public long p;
		public long saddle;
		public int tag;
		
		// for deserialization
		public HalfEdge() {}

		public HalfEdge(long p, long saddle, int tag) {
			this.p = p;
			this.saddle = saddle;
			this.tag = tag;
		}
	}
	
	public HalfEdge[] split() {
		HalfEdge[] spl = new HalfEdge[2];
		spl[0] = (a != PointIndex.NULL ? new HalfEdge(a, saddle, tagA) : null);
		spl[1] = (b != PointIndex.NULL ? new HalfEdge(b, saddle, tagB) : null);
		return spl;
	}
	
	public void reverse() {
		long tmpIx = a;
		a = b;
		b = tmpIx;
		int tmpTag = tagA;
		tagA = tagB;
		tagB = tmpTag;
	}
	
	public boolean equals(Object o) {
		if (o instanceof Edge) {
			Edge e = (Edge)o;
			return this.a == e.a && this.b == e.b && this.saddle == e.saddle;
		} else {
			return false;
		}
	}
	
	public int hashCode() {
		return Objects.hash(a, b, saddle);
	}
	
	public String _fmtTag(int tag) {
		return (tag != TAG_NULL ? "(" + tag + ")" : "");
	}
	
	public String toString() {
		return String.format("%s <=%s %s %s=> %s", Util.print(a), _fmtTag(tagA), Util.print(saddle), _fmtTag(tagB), Util.print(b));
	}
}
