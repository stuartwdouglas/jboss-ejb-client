package org.jboss.ejb.client;

import java.util.Properties;

/**
 * An interface that allows for the default client context to be extended by wrapping the default config based selector.
 *
 * In environments where the default selector is replaced this has no effect.
 *
 * @author Stuart Douglas
 */
public interface DefaultContextSelectorProvider  {

    ContextSelector<EJBClientContext> wrapDefaultContextSelector(ContextSelector<EJBClientContext> selector, Properties properties);

}
