package com.vinodh;


import com.vinodh.apps.HelloVinodh;
import com.vinodh.apps.HelloWorld;
import com.vinodh.apps.SparkApp;


public class SparkDemos {

    public static void main(String[] args) {
        SparkApp.setup(new HelloWorld()).execute();
        SparkApp.setup(new HelloVinodh()).execute();
    }


}
