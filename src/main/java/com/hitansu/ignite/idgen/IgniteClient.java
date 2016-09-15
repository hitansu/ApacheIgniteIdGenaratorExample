package com.hitansu.ignite.idgen;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;

public class IgniteClient {
	
    static Set<Long> set= new HashSet<Long>();
    CyclicBarrier waiter;
    int concurrency= 20;
    static long start= 0;
    
	public IgniteClient() {
		
		waiter= new CyclicBarrier(concurrency, new Runnable() {
			
			public void run() {
				System.out.println("Printing start time ... "+start);
				System.out.println("Total time taken:: "+(System.currentTimeMillis()-start));
			}
		});
	}

    
	public static void main(String[] args) {
		
		Ignition.setClientMode(true);
		Ignite ignite = Ignition.start();
		IgniteCluster cluster = ignite.cluster();
		
		//	client.generateSequnceId(100000, ignite);
		
		
		IgniteClient client= new IgniteClient();
		client.printServerNodes(cluster, ignite);
		start= System.currentTimeMillis();
		client.startIdGenTask(ignite);
		

		

	}
	
	private void startIdGenTask(Ignite ignite) {
		for(int i= 1;i<= concurrency;i++) {
			new Thread(new IdGenTask(waiter, ignite)).start();
		}
		
	}

	static class IdGenTask implements Runnable {

		CyclicBarrier barrier;
		int count= 100000;
		Ignite ignite;
		IgniteLogger log;
		public IdGenTask(CyclicBarrier barrier, Ignite ignite) {
			this.barrier= barrier;
			this.ignite= ignite;
			this.log= this.ignite.log();
		}
		
		public void run() {
			try {
				generateSequnceId(count, ignite);
				barrier.await();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		private void generateSequnceId(int count, Ignite ignite) {
			final IgniteAtomicSequence seq = ignite.atomicSequence("prefix", 0, true);
			for(int i= 1;i<= count;i++) {
				long curr= seq.get();
				long next= seq.incrementAndGet();
				if(!set.add(next)) throw new RuntimeException("Duplicate Id generated");
				log.debug("Api call value: "+next);
				System.out.println("Current value: "+curr+"| Api call value: "+next);
			}
		}
	}

	/*private void generateSequnceId(int count, Ignite ignite) {
		final IgniteAtomicSequence seq = ignite.atomicSequence("prefix", 0, true);
		for(int i= 1;i<= count;i++) {
			long curr= seq.get();
			long next= seq.incrementAndGet();
			if(!set.add(next)) throw new RuntimeException("Duplicate Id generated");
			System.out.println("Current value: "+curr+"| Api call value: "+next);
		}
	}*/

	private void printServerNodes(IgniteCluster cluster, Ignite ignite) {
		ClusterGroup remoteCluster = ignite.cluster().forRemotes();
		ignite.compute(remoteCluster);
		Collection<ClusterNode> remoteNodes = remoteCluster.nodes();
		for(ClusterNode node: remoteNodes) {
			System.out.println(node.toString()+ "is server: "+!node.isClient());
		}
	}

}
