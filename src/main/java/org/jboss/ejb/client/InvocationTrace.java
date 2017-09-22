package org.jboss.ejb.client;

import org.jboss.ejb._private.Logs;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;

public class InvocationTrace {

    public static final AttachmentKey<InvocationTrace> ATTACHMENT_KEY = new AttachmentKey<>();

    private static final ThreadLocal<InvocationTrace> CURRENT = new ThreadLocal<>();

    private final List<String> messages = new CopyOnWriteArrayList<>();

    private final EJBLocator<?> locator;
    private final EJBMethodLocator methodLocator;

    private int printCount;

    public InvocationTrace(EJBLocator<?> locator, EJBMethodLocator methodLocator) {
        this.locator = locator;
        this.methodLocator = methodLocator;
    }

    public static InvocationTrace getCurrent() {
        return CURRENT.get();
    }

    public void log(String message) {
        messages.add(Thread.currentThread().getName() + ": " + System.currentTimeMillis() + ": " + message);
    }

    public void print() {
        StringBuilder sb = new StringBuilder("============================================= pc:"+(printCount++)+"\nAudit log for invocation " + locator + " " + methodLocator);

        for (int i = 0; i < messages.size(); ++i) {
            sb.append("\nEVENT:" + i + ":" + messages.get(i));
        }
        sb.append("\n+++++++++++++++++++++++++++++++++++++++++++");
        Logs.MAIN.error(sb.toString());
    }

    public static void logStatic(String message) {
        InvocationTrace current = CURRENT.get();
        if (current == null) {
            Logs.MAIN.error("invocation trace is null", new IllegalStateException());
        }
        current.log(message);
    }

    public void run(Runnable runnable) {
        InvocationTrace old = CURRENT.get();
        try {
            CURRENT.set(this);
            runnable.run();
        } finally {
            CURRENT.set(old);
        }
    }

    public <T> T run(Callable<T> runnable) {
        InvocationTrace old = CURRENT.get();
        try {
            CURRENT.set(this);
            return runnable.call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            CURRENT.set(old);
        }
    }
}
