package com.openvraas.services.healthchecker;

import com.openvraas.services.cdi.WeldContext;

public class Starter {

    public static void main(String[] args) {

        WeldContext.INSTANCE.getBean(HealthChecker.class);

    }

}
