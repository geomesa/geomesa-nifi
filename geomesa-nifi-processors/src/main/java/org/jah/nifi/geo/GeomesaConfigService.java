package org.jah.nifi.geo;

import org.apache.nifi.controller.ControllerService;
import org.geotools.data.DataStore;


public interface GeomesaConfigService extends ControllerService{
    DataStore getDataStore();
}
