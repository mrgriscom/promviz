package promviz;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;

import promviz.util.Util;

public class Edge {

	static final int TAG_NULL = -1;
	
	public long a;
	public long b;
	public long saddle;
	public int tagA;
	public int tagB;
	
	public Edge(long a, long b, long saddle, int tagA, int tagB) {
		assert (a != b && a != saddle && b != saddle) : String.format("%s %s %s", Util.print(a), Util.print(b), Util.print(saddle));
		assert (saddle != PointIndex.NULL);
		
		this.a = a;
		this.b = b;
		this.saddle = saddle;
		this.tagA = tagA;
		this.tagB = tagB;
		
		if (this.a == PointIndex.NULL) {
			reverse();
		}
	}
	
	public Edge(long a, long b, long saddle) {
		this(a, b, saddle, TAG_NULL, TAG_NULL);
	}
	
	public boolean pending() {
		return b == PointIndex.NULL;
	}

	public void reverse() {
		long tmpIx = a;
		a = b;
		b = tmpIx;
		int tmpTag = tagA;
		tagA = tagB;
		tagB = tmpTag;
	}
	
	void write(DataOutputStream out) {
		try {
			out.writeLong(this.a);
			out.writeLong(this.b);
			out.writeLong(this.saddle);
			out.writeByte(this.tagA);
			out.writeByte(this.tagB);
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		}
	}
	
	static Edge read(DataInputStream in) throws IOException {
		return new Edge(in.readLong(), in.readLong(), in.readLong(), in.readByte(), in.readByte());
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
		return Long.valueOf(this.a).hashCode() ^ Long.valueOf(this.b).hashCode() ^ Long.valueOf(this.saddle).hashCode();
	}
	
	public String _fmtTag(int tag) {
		return (tag != TAG_NULL ? "(" + tag + ")" : "");
	}
	
	public String toString() {
		return String.format("%s <=%s %s %s=> %s", Util.print(a), _fmtTag(tagA), Util.print(saddle), _fmtTag(tagB), Util.print(b));
	}
}
