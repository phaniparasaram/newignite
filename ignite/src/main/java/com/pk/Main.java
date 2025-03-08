package com.pk;

import com.pk.enums.FinderMode;
import com.pk.impl.IgniteNode;
import com.pk.interfaces.Node;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        // Create the configuration for Ignite
        // IgniteConfiguration cfg = new IgniteConfiguration();
        // cfg.setIncludeEventTypes(EventType.EVT_CACHE_OBJECT_PUT, EventType.EVT_CACHE_OBJECT_READ,
        // EventType.EVT_CACHE_OBJECT_REMOVED, EventType.EVT_NODE_JOINED, EventType.EVT_NODE_LEFT);
        // CacheConfiguration cacheCfg = new CacheConfiguration("myCache");

        // cacheCfg.setCacheMode(CacheMode.REPLICATED);
        // cacheCfg.setBackups(2);
        // cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        // cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        // cacheCfg.setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE);
        // cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        
        // // Set up a discovery SPI for Ignite (using default settings or custom settings)
        // TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
       
        // TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        // discoverySpi.setIpFinder(ipFinder);
        // cfg.setDiscoverySpi(discoverySpi);

        // // Start Ignite node
        // Ignite ignite = Ignition.start(cfg);

        // // Get the atomic sequence for leader election
        // IgniteAtomicSequence sequence = ignite.atomicSequence("leaderElectionSeq", 0, true);

        // // Perform the leader election
        // if (sequence.incrementAndGet() == 1) {
        //     System.out.println("This node is the leader!");
        // } else {
        //     System.out.println("This node is a follower.");
        // }
        Node node = new IgniteNode(FinderMode.Multicast, "sessionIdCache");
        node.start();
        
        // Keep the node running to simulate leader election process
        //Thread.sleep(10000);  // Simulating service running
        if(node.isLeader()){
            System.out.println("Inserting data");
            node.addData("SessionId", "r6dytub");
        } else{
            
            System.out.println("Reading Data \n SessionId: " + node.getData("SessionId"));
        }
    }
}
