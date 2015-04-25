package promviz.util;

public abstract class WorkerPoolDebug<I, O> extends WorkerPool<I, O> {
	
	public void launch(int numWorkers, Iterable<I> tasks) {
		int i = 0;
		for (I task : tasks) {
			postprocess(i, process(task));
			i++;
		}
	}
	
}
