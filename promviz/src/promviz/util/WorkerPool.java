package promviz.util;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class WorkerPool <I, O> {

	BlockingQueue<Result> results;

	class Result {
		O result;
		Exception e;
	}
	
	public abstract O process(I input);
	public abstract void postprocess(int i, O output);
	
	class Task implements Runnable {
		I task;
		
		public Task(I task) {
			this.task = task;
		}
		
		@Override
		public void run() {
			Result r = new Result();
			try {
				r.result = process(task);
			} catch(Exception e) {
				r.e = e;
			}
			submit(r);
		}
		
		void submit(Result r) {
			try {
				if (!results.offer(r)) {
					Logging.log(String.format("worker %d blocked submitting result", Thread.currentThread().getId()));
					results.put(r);
				}
			} catch (InterruptedException ie) {
				die(ie);
			}
		}
	}
	
	public void launch(int numWorkers, Iterable<I> tasks) {
		results = new ArrayBlockingQueue<Result>(2 * numWorkers);
		ExecutorService threadPool = Executors.newFixedThreadPool(numWorkers);
		int numTasks = 0;
		int numProcessed = 0;
		
		// TODO randomize tasks?
		for (I task : tasks) {
			threadPool.submit(new Task(task));
			numTasks++;
		}
		threadPool.shutdown();
		
		while (numProcessed < numTasks) {
			Result result;
			try {
				result = results.take();
			} catch (InterruptedException ie) {
				throw new RuntimeException(ie);
			}
			
			if (result.e != null) {
				die(result.e);
			}
			
			postprocess(numProcessed, result.result);
			numProcessed++;
		}
	}
	
	void die(Exception e) {
		e.printStackTrace();
		System.exit(1);
	}
	
}
