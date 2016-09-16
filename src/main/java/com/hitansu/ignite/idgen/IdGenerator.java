package com.hitansu.ignite.idgen;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

public class IdGenerator {
	
    static Set<String> set= new HashSet<String>();
    CyclicBarrier waiter;
    int concurrency= 20;
    static long start= 0;
    IdPersistService persistService= null;
    final static int WRITE_INTERVAl= 1000;
    
	public IdGenerator() {
		
		waiter= new CyclicBarrier(concurrency, new Runnable() {
			
			public void run() {
				System.out.println("Printing start time ... "+start);
				System.out.println("Total time taken:: "+(System.currentTimeMillis()-start));
				System.out.println("Total "+set.size()+" id generated");
				persistService.closeAllConn();
			}
		});
	}

    static {
			try {
				Class.forName("oracle.jdbc.driver.OracleDriver");
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			}
	}
	
	public static void main(String[] args) {
		Ignition.setClientMode(true);
		Ignite ignite = Ignition.start();
		
		IdGenerator client= new IdGenerator();
		start= System.currentTimeMillis();
		client.persistService= new IdPersistService();
		client.startIdGenTask(ignite, client.persistService);
		

		

	}
	
	private void startIdGenTask(Ignite ignite, IdPersistService persistService) {
		for(int i= 1;i<= concurrency;i++) {
			new Thread(new IdGenTask(waiter, ignite, persistService)).start();
		}
		
	}

	static class IdGenTask implements Runnable {
		
		String[] prefixes= {"BUG-", "STORY-", "EPIC-", "SPRINT-", "TASK-", "ISSUE-", "US-", "TICKET-"};

		CyclicBarrier barrier;
		int count= 200000;
		Ignite ignite;
		IgniteLogger log;
		Random rand;
		IdPersistService persistService;
		IgniteCache<String, Long> cache;
		
		public IdGenTask(CyclicBarrier barrier, Ignite ignite, IdPersistService persistService) {
			this.barrier= barrier;
			this.ignite= ignite;
			this.log= this.ignite.log();
			rand= new Random();
			this.persistService= persistService;
			this.cache= getCacheInstance(ignite);
		}
		
		private IgniteCache<String, Long> getCacheInstance(Ignite ignite) {
			CacheConfiguration<String, Long> conf= new CacheConfiguration<String, Long>();
			conf.setName("id_cache");
			conf.setCacheMode(CacheMode.PARTITIONED);
			conf.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
			if(ignite.cache("id_cache")== null) {
				return ignite.createCache(conf);
			}
			return ignite.cache("id_cache");
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
			IgniteAtomicSequence seq;
			for(int i= 1;i<= count;i++) {
				String prefix_key= prefixes[rand.nextInt(prefixes.length)];
				try {
					long last_save_id= 0; //TODO: Here it will check the DB for last id if any
					seq = ignite.atomicSequence(prefix_key, last_save_id, false);
					if(seq== null) {
						seq = ignite.atomicSequence(prefix_key, 0, true);
					}
				} catch(IgniteException e) {
					throw e;
				}
				long curr= seq.get();
				if(curr%WRITE_INTERVAl== 0) {
					// write next next id to db
					Lock lock = cache.lock(prefix_key);
					try {
						if(lock.tryLock(2, TimeUnit.SECONDS)) {
							try {
								persistService.persistId(prefix_key, curr+WRITE_INTERVAl);
							} finally {
								lock.unlock();
							}
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				long next= seq.incrementAndGet();
				if(!set.add(prefix_key+next)) throw new RuntimeException("Duplicate Id generated");
				log.debug("Api call value: "+next);
				System.out.println("Current value: "+prefix_key+curr+"| Api call value: "+prefix_key+next);
			}
		}
	}
}
