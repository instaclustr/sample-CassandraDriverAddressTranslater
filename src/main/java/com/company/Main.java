package com.company;

import com.datastax.driver.core.*;
import com.datastax.driver.core.policies.AddressTranslater;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.HashMap;

public class Main {

    static class PrivateToPublicAddressTranslater implements AddressTranslater {
        final HashMap<InetSocketAddress, InetSocketAddress> privatePublicAddressMap = new HashMap<InetSocketAddress, InetSocketAddress>();

        {
            privatePublicAddressMap.put(new InetSocketAddress("10.224.0.134", 9042), new InetSocketAddress("50.16.170.131", 9042));
            privatePublicAddressMap.put(new InetSocketAddress("10.224.89.62", 9042), new InetSocketAddress("52.202.55.67", 9042));
            privatePublicAddressMap.put(new InetSocketAddress("10.224.133.143", 9042), new InetSocketAddress("52.202.11.232", 9042));
        }

        public InetSocketAddress translate(final InetSocketAddress inetSocketAddress) {
            return privatePublicAddressMap.get(inetSocketAddress);
        }
    }

    public static void main(String[] args) throws KeyStoreException, IOException, CertificateException, NoSuchAlgorithmException, KeyManagementException {

        final SSLContext sslContext;
        {
            final KeyStore trustStore = KeyStore.getInstance("JKS");
            final InputStream stream = Files.newInputStream(Paths.get("/home/chris/truststore.jks"));
            trustStore.load(stream, "truststore_password".toCharArray());
            final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagerFactory.init(trustStore);
            sslContext = SSLContext.getInstance("TLS");
            sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
        }

        final Cluster.Builder clusterBuilder = Cluster.builder()
                .addContactPoints(
                        "50.16.170.131", "52.202.55.67", "52.202.11.232"
                )
                .withAddressTranslater(new PrivateToPublicAddressTranslater())
                .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ALL))
                .withLoadBalancingPolicy((new TokenAwarePolicy(new DCAwareRoundRobinPolicy("AWS_VPC_US_EAST_1"))))
                .withSSL(new SSLOptions(sslContext, SSLOptions.DEFAULT_SSL_CIPHER_SUITES))
	        .withAuthProvider(new PlainTextAuthProvider("cassandra_username", "cassandra_password"));

        final Cluster cluster = clusterBuilder.build();
        final Metadata metadata = cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n", metadata.getClusterName());

        for (final Host host: metadata.getAllHosts()) {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n", host.getDatacenter(), host.getAddress(), host.getRack());
        }
        Session session;
        session = cluster.connect("system");
        ResultSet results = session.execute("SELECT * FROM schema_keyspaces");
        for (Row row : results) {
           System.out.format("%s\n", row.getString("keyspace_name"));
        }
        cluster.close();
    }
}
