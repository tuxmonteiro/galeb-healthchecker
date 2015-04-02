package io.galeb.services.healthchecker;

import io.galeb.services.cdi.WeldContext;


public class Starter {

    public static void main(String[] args) {

        WeldContext.INSTANCE.getBean(HealthChecker.class);

    }

}
