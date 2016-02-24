/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.nifi;
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

import java.io.IOException;
import java.util.*;

import static org.geotools.data.DataStoreFinder.getDataStore;

/**
 * Implementation of GeomesaConfigProviderService interface
 *
 * @see GeomesaConfigProviderService
 */
@CapabilityDescription("Defines credentials for Amazon Web Services processors.")
@SeeAlso(classNames = {
        "org.locationtech.geomesa.nifi.GeoMesaIngest",
        "org.locationtech.geomesa.nifi.AvroToGeomesa"})
@Tags({ "aws", "credentials","provider" })
public class GeomesaConfigControllerService extends AbstractControllerService  {

    public static final PropertyDescriptor Zookeepers = new PropertyDescriptor.Builder()
            .name("Zookeepers")
            .description("Zookeepers host(:port) pairs, comma separated")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor InstanceName = new PropertyDescriptor.Builder()
            .name("Instance")
            .description("Accumulo instance name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor User = new PropertyDescriptor.Builder()
            .name("User")
            .description("Accumulo user name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor Password = new PropertyDescriptor.Builder()
            .name("Password")
            .description("Accumulo password")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true)
            .build();

    public static final PropertyDescriptor Catalog = new PropertyDescriptor.Builder()
            .name("Catalog")
            .description("GeoMesa catalog table name")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();


    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(Zookeepers);
        props.add(InstanceName);
        props.add(User);
        props.add(Password);
        props.add(Catalog);

        properties = Collections.unmodifiableList(props);
    }


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    public DataStore dataStore;

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {


        final Map paramMap = new HashMap<>();
        paramMap.put("zookeepers", context.getProperty(Zookeepers).getValue());
        paramMap.put("instanceId", context.getProperty(InstanceName).getValue());
        paramMap.put("tableName", context.getProperty(Catalog).getValue());
        paramMap.put("user", context.getProperty(User).getValue());
        paramMap.put("password", context.getProperty(Password).getValue());


        try {
            dataStore = getDataStore(paramMap);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @OnDisabled
    public void cleanup() {
        if(dataStore!=null){
            dataStore = null;
        }
    }

    @Override
    public String toString() {
        return "GeomesaConfigControllerService[id=" + getIdentifier() + "]";
    }

}