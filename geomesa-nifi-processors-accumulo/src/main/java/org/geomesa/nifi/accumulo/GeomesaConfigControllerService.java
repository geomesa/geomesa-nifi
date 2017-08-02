/***********************************************************************
 * Copyright (c) 2015-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.geomesa.nifi.accumulo;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.geotools.data.DataStore;
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams;

import java.io.IOException;
import java.util.*;

@CapabilityDescription("Defines credentials for GeoMesa processors.")
@SeeAlso(classNames = { "org.geomesa.nifi.geo.PutGeoMesa" })
@Tags({ "geo", "geomesa","accumulo" })
public class GeomesaConfigControllerService
        extends AbstractControllerService
        implements GeomesaConfigService {

    public static final PropertyDescriptor ZOOKEEPER_PROP = new PropertyDescriptor.Builder()
            .name(AccumuloDataStoreParams.zookeepersParam().getName())
            .description("Zookeeper host(:port) pairs, comma separated")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INSTANCE_NAME_PROP = new PropertyDescriptor.Builder()
            .name(AccumuloDataStoreParams.instanceIdParam().getName())
            .description("Accumulo instance name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USER_PROP = new PropertyDescriptor.Builder()
            .name(AccumuloDataStoreParams.userParam().getName())
            .description("Accumulo user name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD_PROP = new PropertyDescriptor.Builder()
            .name(AccumuloDataStoreParams.passwordParam().getName())
            .description("Accumulo password")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor TABLE_NAME_PROP = new PropertyDescriptor.Builder()
            .name(AccumuloDataStoreParams.tableNameParam().getName())
            .description("GeoMesa catalog table name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ZOOKEEPER_PROP);
        props.add(INSTANCE_NAME_PROP);
        props.add(USER_PROP);
        props.add(PASSWORD_PROP);
        props.add(TABLE_NAME_PROP);
        properties = Collections.unmodifiableList(props);
    }

    public DataStore dataStore;

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        final Map<Object, Object> paramMap = new HashMap<>();
        paramMap.put(AccumuloDataStoreParams.zookeepersParam().getName(), context.getProperty(ZOOKEEPER_PROP).getValue());
        paramMap.put(AccumuloDataStoreParams.instanceIdParam().getName(), context.getProperty(INSTANCE_NAME_PROP).getValue());
        paramMap.put(AccumuloDataStoreParams.userParam().getName(), context.getProperty(USER_PROP).getValue());
        paramMap.put(AccumuloDataStoreParams.passwordParam().getName(), context.getProperty(PASSWORD_PROP).getValue());
        paramMap.put(AccumuloDataStoreParams.tableNameParam().getName(), context.getProperty(TABLE_NAME_PROP).getValue());

        try {
            dataStore = org.geotools.data.DataStoreFinder.getDataStore(Collections.unmodifiableMap(paramMap));
        } catch (IOException e) {
            getLogger().error("Error getting GeoMesa datastore", e);
        }

    }

    @OnDisabled
    public void cleanup() {
        if (dataStore != null){
            dataStore.dispose();
            dataStore = null;
        }
    }

    @Override
    public String toString() {
        return "GeomesaConfigControllerService[id=" + getIdentifier() + "]";
    }

    @Override
    public DataStore getDataStore() {
        return dataStore;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

}