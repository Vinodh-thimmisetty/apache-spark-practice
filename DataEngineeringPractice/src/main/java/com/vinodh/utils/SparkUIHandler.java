package com.vinodh.utils;


import com.vinodh.apps.SparkApp;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.time.Duration;

@Slf4j
public class SparkUIHandler implements InvocationHandler {

    private final SparkApp sparkApp;
    private final Method execute;


    @SneakyThrows
    public SparkUIHandler(SparkApp sparkApp) {
        this.sparkApp = sparkApp;
        this.execute = sparkApp.getClass().getDeclaredMethod("execute");
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable {
        if (StringUtils.equalsIgnoreCase(execute.getName(), method.getName())) {
            log.info("Proxying for {}", execute.getDeclaringClass().getCanonicalName());
            Object result = execute.invoke(sparkApp, args);
            if (execute.isAnnotationPresent(SparkUITimeout.class)) {
                SparkUITimeout timeoutAnnotation = execute.getAnnotation(SparkUITimeout.class);
                if (timeoutAnnotation.isEnabled()) {
                    long timeoutInSeconds = timeoutAnnotation.timeout();
                    Thread mainThread = Thread.currentThread();
                    if (timeoutInSeconds < 1) {
                        log.warn("Blocking {} Thread indefinitely ", mainThread.getName());
                        mainThread.join();
                        log.warn("Spark App is killed manually");
                    } else {
                        log.warn("Blocking {} Thread for {} seconds ", mainThread.getName(), timeoutInSeconds);
                        mainThread.join(Duration.ofSeconds(timeoutInSeconds).toMillis());
                        log.warn("Wait time Over...Closing Spark App");
                    }
                }
            }
            return result;
        }
        return method.invoke(sparkApp, args);
    }
}