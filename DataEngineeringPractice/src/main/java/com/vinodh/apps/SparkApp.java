package com.vinodh.apps;

import com.vinodh.utils.SparkUIHandler;
import lombok.SneakyThrows;

import java.lang.reflect.Proxy;

public interface SparkApp {

    void execute();

    @SneakyThrows
    static SparkApp setup(SparkApp app) {
        SparkUIHandler appHandler = new SparkUIHandler(app);
        return (SparkApp) Proxy.newProxyInstance(SparkApp.class.getClassLoader(), new Class<?>[]{SparkApp.class}, appHandler);
    }

}
