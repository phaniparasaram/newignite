package com.pk.impl;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteEvents;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import com.pk.enums.FinderMode;
import com.pk.enums.NodeRole;
import com.pk.interfaces.Node;

public class IgniteNode implements Node{
    private IgniteConfiguration cfg ;
    private CacheConfiguration cacheCfg;
    private Ignite ignite;
    private NodeRole noderole;
    private String cacheName;
    IgniteCache<String, String> cache;
    public IgniteNode(FinderMode fm, String cacheName) {
        cfg = new IgniteConfiguration();
        cfg.setIncludeEventTypes(EventType.EVT_CACHE_OBJECT_PUT, EventType.EVT_CACHE_OBJECT_READ,
        EventType.EVT_CACHE_OBJECT_REMOVED, EventType.EVT_NODE_JOINED, EventType.EVT_NODE_LEFT);
        cacheCfg = new CacheConfiguration(cacheName);
        cacheCfg.setCacheMode(CacheMode.REPLICATED);
        cacheCfg.setBackups(2);
        cacheCfg.setRebalanceMode(CacheRebalanceMode.SYNC);
        cacheCfg.setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC);
        //cacheCfg.setPartitionLossPolicy(PartitionLossPolicy.READ_ONLY_SAFE);
        //cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);
        cfg.setCacheConfiguration(cacheCfg);
        this.cacheName = cacheName;
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();       
        TcpDiscoveryIpFinder ipFinder = fm == FinderMode.Multicast ? new TcpDiscoveryMulticastIpFinder() : new TcpDiscoveryVmIpFinder();

        discoverySpi.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(discoverySpi);
    }


    @Override
    public void start() {
        ignite = Ignition.start(cfg);
        competeForLeader();
        // Get the atomic sequence for leader election
        listenEvents();
    }

    private void competeForLeader(){
        IgniteAtomicSequence sequence = ignite.atomicSequence("leaderElectionSeq", 0, true);
        long x = sequence.incrementAndGet();
        // Perform the leader election
        if (x == 1) {
            noderole = NodeRole.Leader;
            System.out.println("This node is the leader!" + x);
            createCache();
            
        } else {
            noderole = NodeRole.Follower;
            getCache();
            System.out.println("This node is a follower." + x);
        }
    }

    @Override
    public boolean isLeader() {
        return noderole == NodeRole.Leader;
    }

    @Override
    public void stop() {
        ignite.services().cancelAll();
        cache.destroy();
    }

    private void createCache(){
        System.out.println("Creating cache");
        cache = ignite.getOrCreateCache(cacheName);    
        System.out.println("Created cache:" + cache);
    }

    @Override
    public void addData(String key, String data){
        cache.put(key, data);
        System.out.println(ignite.cluster().state());
    }

    private void getCache(){
        System.out.println("Getting cache");
        System.out.println(ignite.cluster().state());
        cache = ignite.getOrCreateCache(cacheName);  
        System.out.println("Got cache:" + cache);
    }

    @Override
    public String getData(String key){
        if (cache!=null) {
            System.out.println(cache);
            return cache.get(key);
        }
        return null;
    }

    private void listenEvents(){
        IgniteEvents events = ignite.events();
        // Local listener that listens to local events.
        IgnitePredicate<CacheEvent> localListener = evt -> {
            System.out.println("Received event [evt=" + evt.name() + ", key=" + evt.key() + ", oldVal=" + evt.oldValue()
            + ", newVal=" + evt.newValue());
            
            return true; // Continue listening.
        };

        // Subscribe to the cache events that are triggered on the local node.
        events.localListen(localListener, EventType.EVT_CACHE_OBJECT_PUT, EventType.EVT_CACHE_OBJECT_READ,
        EventType.EVT_CACHE_OBJECT_REMOVED);
        
        IgnitePredicate<DiscoveryEvent> localListener2 = evt -> {
            System.out.println("Received event [evt=" + evt.name() + ", key=" + ", oldVal=" + "Old"
                    + ", newVal=" + evt.message());
            competeForLeader();
            return true; // Continue listening.
        };

        // Subscribe to the cache events that are triggered on the local node.
        events.localListen(localListener2, EventType.EVT_NODE_JOINED, EventType.EVT_NODE_LEFT, EventType.EVT_NODE_FAILED, EventType.EVT_NODE_SEGMENTED, EventType.EVT_NODE_VALIDATION_FAILED);
    }

}
