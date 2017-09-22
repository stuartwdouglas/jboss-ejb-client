/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2017 Red Hat, Inc., and individual contributors
 * as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jboss.ejb.protocol.remote;

import static java.security.AccessController.doPrivileged;
import static org.jboss.ejb.client.EJBClientContext.FILTER_ATTR_EJB_MODULE;
import static org.jboss.ejb.client.EJBClientContext.FILTER_ATTR_EJB_MODULE_DISTINCT;
import static org.jboss.ejb.client.EJBClientContext.FILTER_ATTR_NODE;
import static org.jboss.ejb.client.EJBClientContext.getCurrent;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.net.ssl.SSLContext;

import org.jboss.ejb._private.Logs;
import org.jboss.ejb.client.EJBClientConnection;
import org.jboss.ejb.client.EJBClientContext;
import org.jboss.ejb.client.EJBModuleIdentifier;
import org.jboss.ejb.client.InvocationTrace;
import org.jboss.remoting3.ConnectionPeerIdentity;
import org.jboss.remoting3.Endpoint;
import org.wildfly.common.Assert;
import org.wildfly.common.net.CidrAddressTable;
import org.wildfly.common.net.Inet;
import org.wildfly.discovery.AllFilterSpec;
import org.wildfly.discovery.AttributeValue;
import org.wildfly.discovery.EqualsFilterSpec;
import org.wildfly.discovery.FilterSpec;
import org.wildfly.discovery.ServiceType;
import org.wildfly.discovery.ServiceURL;
import org.wildfly.discovery.spi.DiscoveryProvider;
import org.wildfly.discovery.spi.DiscoveryRequest;
import org.wildfly.discovery.spi.DiscoveryResult;
import org.wildfly.security.auth.client.AuthenticationConfiguration;
import org.wildfly.security.auth.client.AuthenticationContext;
import org.wildfly.security.auth.client.AuthenticationContextConfigurationClient;
import org.xnio.FailedIoFuture;
import org.xnio.IoFuture;
import org.xnio.OptionMap;

/**
 * Provides discovery service based on all known EJBClientChannel service registry entries.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
final class RemotingEJBDiscoveryProvider implements DiscoveryProvider, DiscoveredNodeRegistry {

    static final AuthenticationContextConfigurationClient AUTH_CONFIGURATION_CLIENT = doPrivileged(AuthenticationContextConfigurationClient.ACTION);

    private final ConcurrentHashMap<String, NodeInformation> nodes = new ConcurrentHashMap<>();

    private final Set<URI> failedDestinations = Collections.newSetFromMap(new ConcurrentHashMap<URI, Boolean>());

    private final ConcurrentHashMap<String, Set<String>> clusterNodes = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, URI> effectiveAuthURIs = new ConcurrentHashMap<>();



    public RemotingEJBDiscoveryProvider() {
        Endpoint.getCurrent(); //this will blow up if remoting is not present, preventing this from being registered
    }

    public NodeInformation getNodeInformation(final String nodeName) {
        return nodes.computeIfAbsent(nodeName, NodeInformation::new);
    }

    public List<NodeInformation> getAllNodeInformation() {
        return new ArrayList<>(nodes.values());
    }

    public void addNode(final String clusterName, final String nodeName, URI registeredBy) {
        effectiveAuthURIs.putIfAbsent(clusterName, registeredBy);
        clusterNodes.computeIfAbsent(clusterName, ignored -> Collections.newSetFromMap(new ConcurrentHashMap<>())).add(nodeName);
    }

    public void removeNode(final String clusterName, final String nodeName) {
        clusterNodes.getOrDefault(clusterName, Collections.emptySet()).remove(nodeName);
    }

    public void removeCluster(final String clusterName) {
        final Set<String> removed = clusterNodes.remove(clusterName);
        if (removed != null) removed.clear();
        effectiveAuthURIs.remove(clusterName);
    }

    public DiscoveryRequest discover(final ServiceType serviceType, final FilterSpec filterSpec, final DiscoveryResult result) {

        if (! serviceType.implies(ServiceType.of("ejb", "jboss"))) {
            // only respond to requests for JBoss EJB services
            result.complete();
            return DiscoveryRequest.NULL;
        }
        final EJBClientContext ejbClientContext = getCurrent();
        final RemoteEJBReceiver ejbReceiver = ejbClientContext.getAttachment(RemoteTransportProvider.ATTACHMENT_KEY);
        if (ejbReceiver == null) {
            InvocationTrace.logStatic("Not performing discovery as no ejbReceiver");
            // ???
            result.complete();
            return DiscoveryRequest.NULL;
        }

        final List<EJBClientConnection> configuredConnections = ejbClientContext.getConfiguredConnections();

        final DiscoveryAttempt discoveryAttempt = new DiscoveryAttempt(serviceType, filterSpec, result, ejbReceiver, AuthenticationContext.captureCurrent());

        boolean ok = false;
        boolean discoveryConnections = false;
        // first pass
        for (EJBClientConnection connection : configuredConnections) {
            if (! connection.isForDiscovery()) {
                continue;
            }
            discoveryConnections = true;
            final URI uri = connection.getDestination();
            if (failedDestinations.contains(uri)) {
                Logs.INVOCATION.tracef("EJB discovery provider: attempting to connect to configured connection %s, skipping because marked as failed", uri);
                continue;
            }
            ok = true;
            Logs.INVOCATION.tracef("EJB discovery provider: attempting to connect to configured connection %s", uri);
            discoveryAttempt.connectAndDiscover(uri, null);
        }
        // also establish cluster nodes if known
        for (Map.Entry<String, Set<String>> entry : clusterNodes.entrySet()) {
            final String clusterName = entry.getKey();
            final Set<String> nodeSet = entry.getValue();
            int maxConnections = ejbClientContext.getMaximumConnectedClusterNodes();
            nodeLoop: for (String nodeName : nodeSet) {
                if (maxConnections <= 0) break;
                final NodeInformation nodeInformation = nodes.get(nodeName);
                if (nodeInformation != null) {
                    final NodeInformation.ClusterNodeInformation clusterInfo = nodeInformation.getClustersByName().get(clusterName);
                    if (clusterInfo != null) {
                        final Map<String, CidrAddressTable<InetSocketAddress>> tables = clusterInfo.getAddressTablesByProtocol();
                        for (Map.Entry<String, CidrAddressTable<InetSocketAddress>> entry2 : tables.entrySet()) {
                            final String protocol = entry2.getKey();
                            final CidrAddressTable<InetSocketAddress> addressTable = entry2.getValue();
                            for (CidrAddressTable.Mapping<InetSocketAddress> mapping : addressTable) {
                                final InetSocketAddress destination = mapping.getValue();
                                final InetSocketAddress source = ejbReceiver.getSourceAddress(destination);
                                if (source == null ? mapping.getRange().getNetmaskBits() == 0 : source.equals(destination)) {
                                    try {
                                        final InetAddress destinationAddress = destination.getAddress();
                                        String hostName = Inet.getHostNameIfResolved(destinationAddress);
                                        if (hostName == null) {
                                            if (destinationAddress instanceof Inet6Address) {
                                                hostName = '[' + Inet.toOptimalString(destinationAddress) + ']';
                                            } else {
                                                hostName = Inet.toOptimalString(destinationAddress);
                                            }
                                        }
                                        final URI uri = new URI(protocol, null, hostName, destination.getPort(), null, null, null);
                                        if (! failedDestinations.contains(uri)) {
                                            maxConnections--;
                                            Logs.INVOCATION.tracef("EJB discovery provider: attempting to connect to cluster %s connection %s", clusterName, uri);
                                            discoveryAttempt.connectAndDiscover(uri, clusterName);
                                            ok = true;
                                            continue nodeLoop;
                                        }
                                    } catch (URISyntaxException e) {
                                        // ignore URI and try the next one
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        // special second pass - retry everything because all were marked failed
        if (discoveryConnections && ! ok) {
            Logs.INVOCATION.tracef("EJB discovery provider: all connections marked failed, retrying ...");
            for (EJBClientConnection connection : configuredConnections) {
                if (! connection.isForDiscovery()) {
                    continue;
                }
                URI destination = connection.getDestination();
                Logs.INVOCATION.tracef("EJB discovery provider: attempting to connect to connection %s", destination);
                discoveryAttempt.connectAndDiscover(destination, null);
            }
        }

        discoveryAttempt.countDown();
        return discoveryAttempt;
    }

    static EJBModuleIdentifier getIdentifierForAttribute(String attribute, AttributeValue value) {
        if (! value.isString()) {
            return null;
        }
        final String stringVal = value.toString();
        switch (attribute) {
            case FILTER_ATTR_EJB_MODULE: {
                final String[] segments = stringVal.split("/");
                final String app, module;
                if (segments.length == 2) {
                    app = segments[0];
                    module = segments[1];
                } else if (segments.length == 1) {
                    app = "";
                    module = segments[0];
                } else {
                    return null;
                }
                return new EJBModuleIdentifier(app, module, "");
            }
            case FILTER_ATTR_EJB_MODULE_DISTINCT: {
                final String[] segments = stringVal.split("/");
                final String app, module, distinct;
                if (segments.length == 3) {
                    app = segments[0];
                    module = segments[1];
                    distinct = segments[2];
                } else if (segments.length == 2) {
                    app = "";
                    module = segments[0];
                    distinct = segments[1];
                } else {
                    return null;
                }
                return new EJBModuleIdentifier(app, module, distinct);
            }
            default: {
                return null;
            }
        }
    }

    static final FilterSpec.Visitor<Void, EJBModuleIdentifier, RuntimeException> MI_EXTRACTOR = new FilterSpec.Visitor<Void, EJBModuleIdentifier, RuntimeException>() {
        public EJBModuleIdentifier handle(final EqualsFilterSpec filterSpec, final Void parameter) throws RuntimeException {
            return getIdentifierForAttribute(filterSpec.getAttribute(), filterSpec.getValue());
        }

        public EJBModuleIdentifier handle(final AllFilterSpec filterSpec, final Void parameter) throws RuntimeException {
            for (FilterSpec child : filterSpec) {
                final EJBModuleIdentifier match = child.accept(this);
                if (match != null) {
                    return match;
                }
            }
            return null;
        }
    };

    static final FilterSpec.Visitor<Void, String, RuntimeException> NODE_EXTRACTOR = new FilterSpec.Visitor<Void, String, RuntimeException>() {
        public String handle(final EqualsFilterSpec filterSpec, final Void parameter) throws RuntimeException {
            final AttributeValue value = filterSpec.getValue();
            return filterSpec.getAttribute().equals(FILTER_ATTR_NODE) && value.isString() ? value.toString() : null;
        }

        public String handle(final AllFilterSpec filterSpec, final Void parameter) throws RuntimeException {
            for (FilterSpec child : filterSpec) {
                final String match = child.accept(this);
                if (match != null) {
                    return match;
                }
            }
            return null;
        }
    };

    IoFuture<ConnectionPeerIdentity> getConnectedIdentityUsingClusterEffective(Endpoint endpoint, URI destination, String abstractType, String abstractTypeAuthority, AuthenticationContext context, String clusterName) {
        Assert.checkNotNullParam("destination", destination);
        Assert.checkNotNullParam("context", context);

        URI effectiveAuth = clusterName != null ? effectiveAuthURIs.get(clusterName) : null;
        if (effectiveAuth == null)  {
            effectiveAuth = destination;
        }

        final AuthenticationContextConfigurationClient client = AUTH_CONFIGURATION_CLIENT;
        final SSLContext sslContext;
        try {
            sslContext = client.getSSLContext(destination, context);
        } catch (GeneralSecurityException e) {
            return new FailedIoFuture<>(Logs.REMOTING.failedToConfigureSslContext(e));
        }
        final AuthenticationConfiguration authenticationConfiguration = client.getAuthenticationConfiguration(effectiveAuth, context, -1, abstractType, abstractTypeAuthority);
        return endpoint.getConnectedIdentity(destination, sslContext, clusterName != null ? clearOverrides(authenticationConfiguration) : authenticationConfiguration);
    }

    // TODO remove this hack once ELY-1399 is fully completed, and nothing else
    // (e.g. naming) registers an override
    private AuthenticationConfiguration clearOverrides(AuthenticationConfiguration config) {
        return config.useProtocol(null).useHost(null).usePort(-1);
    }

    final class DiscoveryAttempt implements DiscoveryRequest, DiscoveryResult {
        private final ServiceType serviceType;
        private final FilterSpec filterSpec;
        private final DiscoveryResult discoveryResult;
        private final RemoteEJBReceiver ejbReceiver;
        private final AuthenticationContext authenticationContext;

        private final Endpoint endpoint;
        private final AtomicInteger outstandingCount = new AtomicInteger(1); // this is '1' so that we don't finish until all connections are searched
        private volatile boolean phase2;
        private final List<Runnable> cancellers = Collections.synchronizedList(new ArrayList<>());
        private final IoFuture.HandlingNotifier<ConnectionPeerIdentity, URI> outerNotifier;
        private final IoFuture.HandlingNotifier<EJBClientChannel, URI> innerNotifier;
        private final InvocationTrace invocationTrace;

        DiscoveryAttempt(final ServiceType serviceType, final FilterSpec filterSpec, final DiscoveryResult discoveryResult, final RemoteEJBReceiver ejbReceiver, final AuthenticationContext authenticationContext) {
            this.serviceType = serviceType;
            this.filterSpec = filterSpec;
            this.discoveryResult = discoveryResult;
            this.ejbReceiver = ejbReceiver;
            this.invocationTrace = InvocationTrace.getCurrent();

            this.authenticationContext = authenticationContext;
            endpoint = Endpoint.getCurrent();
            outerNotifier = new IoFuture.HandlingNotifier<ConnectionPeerIdentity, URI>() {
                public void handleCancelled(final URI destination) {
                    invocationTrace.log("outerNotifier cancelled for " + destination);
                    countDown();
                }

                public void handleFailed(final IOException exception, final URI destination) {
                    invocationTrace.log("outerNotifier failed " + exception + " for " + destination);
                    DiscoveryAttempt.this.discoveryResult.reportProblem(exception);
                    failedDestinations.add(destination);
                    countDown();
                }

                public void handleDone(final ConnectionPeerIdentity data, final URI destination) {
                    invocationTrace.log("outerNotifier done for " + destination);
                    final IoFuture<EJBClientChannel> future = DiscoveryAttempt.this.ejbReceiver.serviceHandle.getClientService(data.getConnection(), OptionMap.EMPTY);
                    onCancel(future::cancel);
                    future.addNotifier(innerNotifier, destination);
                }
            };
            innerNotifier = new IoFuture.HandlingNotifier<EJBClientChannel, URI>() {
                public void handleCancelled(final URI destination) {
                    invocationTrace.log("innerNotifier cancelled for " + destination);
                    countDown();
                }

                public void handleFailed(final IOException exception, final URI destination) {
                    invocationTrace.log("innerNotifier failed " + exception + " for " + destination);
                    DiscoveryAttempt.this.discoveryResult.reportProblem(exception);
                    failedDestinations.add(destination);
                    countDown();
                }

                public void handleDone(final EJBClientChannel clientChannel, final URI destination) {
                    invocationTrace.log("innerNotifier done for " + destination);
                    failedDestinations.remove(destination);
                    countDown();
                }
            };
        }

        void connectAndDiscover(URI uri, String clusterEffective) {
            invocationTrace.log("connectAndDiscover " + uri);
            final String scheme = uri.getScheme();
            if (scheme == null || ! ejbReceiver.getRemoteTransportProvider().supportsProtocol(scheme) || ! endpoint.isValidUriScheme(scheme)) {
                invocationTrace.log("connectAndDiscover " + uri + " not valid, counting down");
                countDown();
                return;
            }
            outstandingCount.getAndIncrement();
            final IoFuture<ConnectionPeerIdentity> future = doPrivileged((PrivilegedAction<IoFuture<ConnectionPeerIdentity>>) () -> getConnectedIdentityUsingClusterEffective(endpoint, uri, "ejb", "jboss", authenticationContext, clusterEffective));
            onCancel(future::cancel);
            future.addNotifier(outerNotifier, uri);
        }

        void countDown() {
            invocationTrace.log("countDown " + outstandingCount.get());
            if (outstandingCount.decrementAndGet() == 0) {
                invocationTrace.log("countDown hit zero");
                final DiscoveryResult result = this.discoveryResult;
                final String node = filterSpec.accept(NODE_EXTRACTOR);
                final EJBModuleIdentifier module = filterSpec.accept(MI_EXTRACTOR);
                if (phase2) {
                    if (node != null) {
                        final NodeInformation information = nodes.get(node);
                        if (information != null) {
                            invocationTrace.log("phase2 discovered node " + information.getNodeName());
                            information.discover(serviceType, filterSpec, result);
                        }
                    } else for (NodeInformation information : nodes.values()) {
                        invocationTrace.log("phase2 discovered nodes " + information.getNodeName());
                        information.discover(serviceType, filterSpec, result);
                    }
                    invocationTrace.log("phase2 complete");
                    result.complete();
                } else {
                    boolean ok = false;
                    // optimize for simple module identifier and node name queries
                    if (node != null) {
                        final NodeInformation information = nodes.get(node);
                        if (information != null) {
                            invocationTrace.log("phase1 discovered node " + information.getNodeName());
                            if (information.discover(serviceType, filterSpec, result)) {
                                ok = true;
                            }
                        }
                    } else for (NodeInformation information : nodes.values()) {
                        invocationTrace.log("phase1 discovered nodes " + information.getNodeName());
                        if (information.discover(serviceType, filterSpec, result)) {
                            ok = true;
                        }
                    }
                    if (ok) {
                        invocationTrace.log("phase1 all done");
                        result.complete();
                    } else {
                        invocationTrace.log("phase1 failed, try phase2");
                        // everything failed.  We have to reconnect everything.
                        Set<URI> everything = new HashSet<>();
                        Map<URI, String> effectiveAuthMappings = new HashMap<>();
                        for (EJBClientConnection connection : ejbReceiver.getReceiverContext().getClientContext().getConfiguredConnections()) {
                            if (connection.isForDiscovery()) {
                                everything.add(connection.getDestination());
                            }
                        }
                        outer: for (NodeInformation information : nodes.values()) {
                            for (NodeInformation.ClusterNodeInformation cni : information.getClustersByName().values()) {
                                final Map<String, CidrAddressTable<InetSocketAddress>> atm = cni.getAddressTablesByProtocol();
                                for (Map.Entry<String, CidrAddressTable<InetSocketAddress>> entry2 : atm.entrySet()) {
                                    final String protocol = entry2.getKey();
                                    final CidrAddressTable<InetSocketAddress> addressTable = entry2.getValue();
                                    for (CidrAddressTable.Mapping<InetSocketAddress> mapping : addressTable) {
                                        final InetSocketAddress destination = mapping.getValue();
                                        final InetSocketAddress source = ejbReceiver.getSourceAddress(destination);
                                        if (source == null ? mapping.getRange().getNetmaskBits() == 0 : source.equals(destination)) {
                                            try {
                                                final InetAddress destinationAddress = destination.getAddress();
                                                String hostName = Inet.getHostNameIfResolved(destinationAddress);
                                                if (hostName == null) {
                                                    if (destinationAddress instanceof Inet6Address) {
                                                        hostName = '[' + Inet.toOptimalString(destinationAddress) + ']';
                                                    } else {
                                                        hostName = Inet.toOptimalString(destinationAddress);
                                                    }
                                                }
                                                URI location = new URI(protocol, null, hostName, destination.getPort(), null, null, null);
                                                String cluster = effectiveAuthMappings.get(location);
                                                if (cluster != null) {
                                                    effectiveAuthMappings.put(location, cluster);
                                                }

                                                everything.add(location);
                                                continue outer;
                                            } catch (URISyntaxException e) {
                                                // ignore URI and try the next one
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        // now connect them ALL
                        phase2 = true;
                        outstandingCount.incrementAndGet();
                        for (URI uri : everything) {
                            connectAndDiscover(uri, effectiveAuthMappings.get(uri));
                        }
                        countDown();
                    }
                }
            }
        }

        // discovery result methods

        public void complete() {
            countDown();
        }

        public void reportProblem(final Throwable description) {
            discoveryResult.reportProblem(description);
        }

        public void addMatch(final ServiceURL serviceURL) {
            discoveryResult.addMatch(serviceURL);
        }

        // discovery request methods

        public void cancel() {
            final List<Runnable> cancellers = this.cancellers;
            synchronized (cancellers) {
                for (Runnable canceller : cancellers) {
                    canceller.run();
                }
            }
        }

        void onCancel(final Runnable action) {
            final List<Runnable> cancellers = this.cancellers;
            synchronized (cancellers) {
                cancellers.add(action);
            }
        }
    }
}
