/***********************************************************************
 * Copyright (c) 2015-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.geomesa.nifi.datastore.impl;

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
import org.geomesa.nifi.datastore.services.DataStoreConfigService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@CapabilityDescription("Defines credentials for GeoMesa Accumulo processors")
@SeeAlso(classNames = { "org.geomesa.nifi.processors.accumulo.PutGeoMesaAccumulo" })
@Tags({ "geo", "geomesa","accumulo" })
public class AccumuloDataStoreConfigControllerService
      extends AbstractControllerService implements DataStoreConfigService {

    public static final PropertyDescriptor ZOOKEEPER_PROP = new PropertyDescriptor.Builder()
            .name("accumulo.zookeepers")
            .description("Zookeeper host(:port) pairs, comma separated")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INSTANCE_ID_PROP = new PropertyDescriptor.Builder()
            .name("accumulo.instance.id")
            .description("Accumulo instance ID")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor USER_PROP = new PropertyDescriptor.Builder()
            .name("accumulo.user")
            .description("Accumulo user name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD_PROP = new PropertyDescriptor.Builder()
            .name("accumulo.password")
            .description("Accumulo password")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor CATALOG_PROP = new PropertyDescriptor.Builder()
            .name("accumulo.catalog")
            .description("GeoMesa catalog table name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ZOOKEEPER_PROP);
        props.add(INSTANCE_ID_PROP);
        props.add(USER_PROP);
        props.add(PASSWORD_PROP);
        props.add(CATALOG_PROP);
        properties = Collections.unmodifiableList(props);
    }

    private Map<String, Serializable> params;

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        final Map<String, Serializable> map = new HashMap<>();
        for (PropertyDescriptor prop : properties) {
            map.put(prop.getName(), context.getProperty(prop).getValue());
        }
        params = Collections.unmodifiableMap(map);
    }

    @OnDisabled
    public void cleanup() {
    }

    @Override
    public String toString() {
        return "AccumuloDataStoreConfigService[id=" + getIdentifier() + "]";
    }

    @Override
    public Map<String, Serializable> getDataStoreParameters() {
        return params;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
}
