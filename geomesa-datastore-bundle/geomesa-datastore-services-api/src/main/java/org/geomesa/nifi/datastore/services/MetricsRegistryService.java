package org.geomesa.nifi.datastore.services;

import org.apache.nifi.controller.ControllerService;

import java.io.Closeable;

public interface MetricsRegistryService extends ControllerService {

    /**
     * Register a component with this service, which will ensure any metrics registries managed by this service
     * are added to the global metrics registry
     *
     * @return reference to deregister
     */
    Closeable register();
}
