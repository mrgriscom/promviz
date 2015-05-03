package old.promviz;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import promviz.Prefix;
import promviz.util.DefaultMap;
import promviz.util.Logging;

public class PreprocessNetwork {
	
	
	
	static class Meta {
		long ix;
		
		public Meta() {}
		
		public Meta(long ix) {
			this.ix = ix;
		}
		
		void read(DataInputStream in) throws EOFException {
			try {
				this.ix = in.readLong();
				readData(in);
			} catch (EOFException eof) {
				throw eof;
			} catch (IOException ioe) {
				throw new RuntimeException();
			}
		}
		void readData(DataInputStream in) throws IOException {}

		void write(DataOutputStream out) {
			try {
				out.writeLong(this.ix);
				writeData(out);
			} catch (IOException ioe) {
				throw new RuntimeException();
			}
		}
		void writeData(DataOutputStream out) throws IOException {}

		int recSize() {
			return 8 + dataSize();
		}
		int dataSize() { return 0; }
		String getName() { return null; }
		
		String bulkPath(boolean up) { // bulk meta is always located in the 'mst' directory
			return EdgeIterator.dir(EdgeIterator.PHASE_MST, true) + "/" + getName() + "-" + (up ? "up" : "down");
		}
		String chunkPath(int phase, boolean up, Prefix p) {
			return prefixPath(up, getName(), p, phase);
		}
		Meta fuckingHell() { return null; }
		
		class MetaIterator implements Iterator<Meta> {
			DataInputStream in;
			int count = 0;
			Meta nextMeta;
			
			public MetaIterator(String path) {
				try {
					in = new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
					readNext();
				} catch (FileNotFoundException e) {
					//throw new RuntimeException();
					nextMeta = null;
				}
			}
			
			public MetaIterator(boolean up) {
				this(bulkPath(up));
			}

			public MetaIterator(int phase, boolean up, Prefix p) {
				this(chunkPath(phase, up, p));
			}
			
			void readNext() {
				try {
					nextMeta = fuckingHell();
					nextMeta.read(in);
				} catch (EOFException eof) {
					nextMeta = null;
					try {
						in.close();
					} catch (IOException e) { }
				}
				count += 1;
			}
			
			public boolean hasNext() {
				return nextMeta != null;
			}

			public Meta next() {
				Meta m = nextMeta;
				if (count % 1000000 == 0) {
					Logging.log(count + " read (" + getName() + ")");
				}
				readNext();
				return m;
			}

			public void remove() {
				throw new UnsupportedOperationException();			
			}
			
			public Iterable<Meta> toIter() {
				return new Iterable<Meta>() {
					public Iterator<Meta> iterator() {
						return MetaIterator.this;
					}
				};
			}
		}
		MetaIterator iterator(boolean up) {
			return new MetaIterator(up);
		}
		MetaIterator iterator(int phase, boolean up, Prefix p) {
			return new MetaIterator(phase, up, p);
		}
	}
	
	static class PromMeta extends Meta {
		float prom;
		private long otherIx;
		boolean forward;
		
		public PromMeta() {}
		
		public PromMeta(long ix, float prom, long otherIx, boolean forward) {
			super(ix);
			this.prom = prom;
			this.otherIx = otherIx;
			this.forward = forward;
		}
		
		void readData(DataInputStream in) throws IOException {
			this.prom = in.readFloat();
			this.otherIx = in.readLong();
			this.forward = in.readBoolean();
		}
		void writeData(DataOutputStream out) throws IOException {
			out.writeFloat(this.prom);	
			out.writeLong(this.otherIx);
			out.writeBoolean(this.forward);		
		}
		
		long getPeak(boolean isSaddle) {
			if (isSaddle) {
				return otherIx;
			} else {
				throw new IllegalArgumentException();
			}
		}
		
		long getSaddle(boolean isSaddle) {
			if (isSaddle) {
				throw new IllegalArgumentException();
			} else {
				return otherIx;
			}			
		}
		
		String getName() {
			return "prom";
		}
		
		int dataSize() {
			return 13;
		}		
		
		Meta fuckingHell() { return new PromMeta(); }
	}

	static class ThresholdMeta extends Meta {
				
		public ThresholdMeta() {}
		
		public ThresholdMeta(long ix) {
			super(ix);
		}
		
		String getName() {
			return "thresh";
		}
		
		int dataSize() {
			return 0;
		}		
		
		Meta fuckingHell() { return new ThresholdMeta(); }
	}

	
	public static void partitionMeta(int phase, boolean up, Set<Prefix> buckets, Meta spec) {
		Logging.log("--" + spec.getName() + "--");
		final long MAX_UNITS_AT_ONCE = Long.parseLong(DEMManager.props.getProperty("memory")) / (3 * spec.recSize());
		for (Iterable<Meta> chunk : chunker(spec.iterator(up), MAX_UNITS_AT_ONCE)) {
			partitionMetaChunk(phase, up, buckets, chunk, spec);
		}
	}
	
	static void partitionMetaChunk(int phase, boolean up, Set<Prefix> buckets, Iterable<Meta> metas, Meta spec) {
		final Map<Prefix, ByteArrayOutputStream> _f = new HashMap<Prefix, ByteArrayOutputStream>();
		Map<Prefix, DataOutputStream> f = new DefaultMap<Prefix, DataOutputStream>() {
			public DataOutputStream defaultValue(Prefix key) {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				_f.put(key, baos);
				return new DataOutputStream(baos);
			}
		};

		for (Meta m : metas) {
			Prefix bucket = matchPrefix(m.ix, buckets);
			m.write(f.get(bucket));
		}
		
		for (Map.Entry<Prefix, ByteArrayOutputStream> e : _f.entrySet()) {
			Prefix p = e.getKey();
			ByteArrayOutputStream out = e.getValue();

			try {
				FileOutputStream fout = new FileOutputStream(spec.chunkPath(phase, up, p), true);
				fout.write(out.toByteArray());
				fout.close();
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}
		}

		
	}
}
