package com.nokia;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteAtomicSequence;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        // Create the configuration for Ignite
        IgniteConfiguration cfg = new IgniteConfiguration();

        // Set up a discovery SPI for Ignite (using default settings or custom settings)
        TcpDiscoverySpi discoverySpi = new TcpDiscoverySpi();
       
        TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryMulticastIpFinder();
        discoverySpi.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(discoverySpi);

        // Start Ignite node
        Ignite ignite = Ignition.start(cfg);

        // Get the atomic sequence for leader election
        IgniteAtomicSequence sequence = ignite.atomicSequence("leaderElectionSeq", 0, true);

        // Perform the leader election
        if (sequence.incrementAndGet() == 1) {
            System.out.println("This node is the leader!");
        } else {
            System.out.println("This node is a follower.");
        }

        // Keep the node running to simulate leader election process
        Thread.sleep(10000);  // Simulating service running
    }
}
