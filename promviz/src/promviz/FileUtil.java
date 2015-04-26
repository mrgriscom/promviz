package promviz;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import old.promviz.DEMManager;
import promviz.util.Logging;
import promviz.util.SaneIterable;

public class FileUtil {

	public static final int PHASE_RAW = 0;
	public static final int PHASE_MST = 1;
	public static final int PHASE_RMST = 2;
	
	static String segmentPath(boolean up, Prefix p, int phase) {
		return prefixPath(up, null, p, phase);
	}
	
	static String fmtOffset(int k) {
		String s = String.format("%06x", k);
		return s.substring(s.length() - 6);
		//return (k < 0 ? "-" : "") + String.format("%06x", Math.abs(k));
	}
	
	// TBD is 'mode' still useful?
	static String prefixPath(boolean up, String mode, Prefix p, int phase) {
		int[] pp = PointIndex.split(p.prefix);
		return String.format("%s/%s%s-%d,%04d,%s,%s",
				dir(phase), mode != null ? mode : "", up ? "U" : "D",
				p.res, pp[0], fmtOffset(pp[1]), fmtOffset(pp[2]));		
	}

	public static String dir(int phase) {
		String _d = null;
		if (phase == PHASE_RAW) {
			_d = "dir_net";
		} else if (phase == PHASE_MST) {
			_d = "dir_mst";
		} else if (phase == PHASE_RMST) {
			_d = "dir_rmst";
		}
		String root = Main.props.getProperty("dir_root");
		String path = Main.props.getProperty(_d);
		return new File(root, path).getPath();
	}

	public static SaneIterable<Edge> loadEdges(boolean up, Prefix p, int phase) {
		return loadEdges(segmentPath(up, p, phase));
	}
	
	public static SaneIterable<Edge> loadEdges(String path) {
		final DataInputStream in;
		try {
			in = new DataInputStream(new BufferedInputStream(new FileInputStream(path)));
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}

		return new SaneIterable<Edge>() {
			public Edge genNext() {
				Edge next = Edge.read(in);
				if (next != null) {
					return next;
				} else {
					throw new NoSuchElementException();
				}
			}
		};
	}

	static public void ensureEmpty(int phase) {
		String path = dir(phase);
		File folder = new File(path);
		if (folder.listFiles().length != 0) {
			throw new RuntimeException(path + " not empty!");
		}
	}
	
}
