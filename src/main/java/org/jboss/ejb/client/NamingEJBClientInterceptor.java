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

package org.jboss.ejb.client;

import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import org.jboss.ejb.client.annotation.ClientInterceptorPriority;
import org.wildfly.naming.client.NamingProvider;

/**
 * EJB client interceptor to discover a target location based on naming context information in the EJB proxy.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 */
@ClientInterceptorPriority(NamingEJBClientInterceptor.PRIORITY)
public final class NamingEJBClientInterceptor implements EJBClientInterceptor {
    /**
     * This interceptor's priority.
     */
    public static final int PRIORITY = ClientInterceptorPriority.JBOSS_AFTER + 50;

    private static final AttachmentKey<Set<URI>> ATTEMPTED_KEY = new AttachmentKey<>();

    public NamingEJBClientInterceptor() {
    }

    public void handleInvocation(final EJBClientInvocationContext context) throws Exception {
        setDestination(context, context.getProxyAttachment(EJBRootContext.NAMING_PROVIDER_ATTACHMENT_KEY));
        context.sendRequest();
    }

    public Object handleInvocationResult(final EJBClientInvocationContext context) throws Exception {
        return context.getResult();
    }

    public SessionID handleSessionCreation(final EJBSessionCreationInvocationContext context) throws Exception {
        setDestination(context, context.getAttachment(EJBRootContext.NAMING_PROVIDER_ATTACHMENT_KEY));
        return context.proceed();
    }

    private static void setDestination(final AbstractInvocationContext context, final NamingProvider namingProvider) {

        if (namingProvider != null) {
            final URI destination = context.getDestination();
            if (destination == null) {
                final EJBLocator<?> locator = context.getLocator();
                if (locator.getAffinity() == Affinity.NONE || locator.getAffinity() instanceof ClusterAffinity) {
                    final Affinity weakAffinity = context.getWeakAffinity();
                    if (weakAffinity == Affinity.NONE || weakAffinity instanceof ClusterAffinity) {


                        Set<URI> set = context.getAttachment(ATTEMPTED_KEY);
                        if (set == null) {
                            final Set<URI> appearing = context.putAttachmentIfAbsent(ATTEMPTED_KEY, set = new HashSet<>());
                            if (appearing != null) {
                                set = appearing;
                            }
                        }
                        final List<NamingProvider.Location> locations = namingProvider.getLocations();
                        if (locations.size() == 1) {
                            URI uri = locations.get(0).getUri();
                            if(set.add(uri)) {
                                context.setDestination(uri);
                            }
                        } else if (locations.size() > 0) {
                            int offset = ThreadLocalRandom.current().nextInt(locations.size());
                            for(int i = 0; i < locations.size(); ++i) {
                                int pos = (i + offset) % locations.size();
                                URI location = locations.get(pos).getUri();
                                if(set.add(location)) {
                                    context.setDestination(location);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
