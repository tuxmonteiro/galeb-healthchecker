package io.galeb.services.healthchecker;

import io.galeb.core.starter.AbstractStarter;


public class Starter extends AbstractStarter {

    private Starter() {
        // main class
    }

    public static void main(String[] args) {

        loadService(HealthChecker.class);

    }

}
