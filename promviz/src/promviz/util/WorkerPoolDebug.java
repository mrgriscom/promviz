package promviz.util;

public abstract class WorkerPoolDebug<I, O> extends WorkerPool<I, O> {
	
	public WorkerPoolDebug(int numWorkers) {
		super(numWorkers);
		threadPool.shutdown();
	}

	public void launch(Iterable<I> tasks) {
		for (I task : tasks) {
			postprocess(process(task));
		}
	}
	
}
