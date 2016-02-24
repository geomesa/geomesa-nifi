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
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

// refactor these out?

//user
//password
//catalog


/**
 * Implementation of GeomesaConfigProviderService interface
 *
 * @see GeomesaConfigProviderService
 */
@CapabilityDescription("Defines credentials for Amazon Web Services processors.")
@Tags({ "aws", "credentials","provider" })
public class GeomesaConfigControllerService extends AbstractControllerService implements GeomesaConfigProviderService {

    /**
     * AWS Role Arn used for cross account access
     *
     * @see <a href="http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html#genref-arns">AWS ARN</a>
     */
    public static final PropertyDescriptor ASSUME_ROLE_ARN = new PropertyDescriptor.Builder().name("Assume Role ARN")
            .expressionLanguageSupported(false).required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false).description("The AWS Role ARN for cross account access. This is used in conjunction with role name and session timeout").build();

    /**
     * The role name while creating aws role
     */
    public static final PropertyDescriptor ASSUME_ROLE_NAME = new PropertyDescriptor.Builder().name("Assume Role Session Name")
            .expressionLanguageSupported(false).required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(false).description("The aws role name for cross account access. This is used in conjunction with role arn and session time out").build();

    /**
     * Max session time for role based credentials. The range is between 900 and 3600 seconds.
     */
    public static final PropertyDescriptor MAX_SESSION_TIME = new PropertyDescriptor.Builder()
            .name("Session Time")
            .description("Session time for role based session (between 900 and 3600 seconds). This is used in conjunction with role arn and name")
            .defaultValue("3600")
            .required(false)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .sensitive(false)
            .build();

    private static final List<PropertyDescriptor> properties;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ASSUME_ROLE_ARN);
        props.add(ASSUME_ROLE_NAME);
        props.add(MAX_SESSION_TIME);

        properties = Collections.unmodifiableList(props);
    }

    private volatile String credentialsProvider;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public String getCredentialsProvider() throws ProcessException {
        return credentialsProvider;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {

        final boolean assumeRoleArnIsSet = validationContext.getProperty(ASSUME_ROLE_ARN).isSet();
        final boolean assumeRoleNameIsSet = validationContext.getProperty(ASSUME_ROLE_NAME).isSet();
        final Integer maxSessionTime = validationContext.getProperty(MAX_SESSION_TIME).asInteger();


        final Collection<ValidationResult> validationFailureResults = new ArrayList<>();



        // Both role and arn name are req if present
        if (assumeRoleArnIsSet ^ assumeRoleNameIsSet ) {
            validationFailureResults.add(new ValidationResult.Builder().input("Assume Role Arn and Name")
                    .valid(false).explanation("Assume role requires both arn and name to be set").build());
        }

        // Session time only b/w 900 to 3600 sec (see sts session class)
        if ( maxSessionTime < 900 || maxSessionTime > 3600 )
            validationFailureResults.add(new ValidationResult.Builder().valid(false).input(maxSessionTime + "")
                    .subject(MAX_SESSION_TIME.getDisplayName() +
                            " can have value only between 900 and 3600 seconds").build());

        return validationFailureResults;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {

        final String assumeRoleArn = context.getProperty(ASSUME_ROLE_ARN).getValue();
        final Integer maxSessionTime = context.getProperty(MAX_SESSION_TIME).asInteger();
        final String assumeRoleName = context.getProperty(ASSUME_ROLE_NAME).getValue();



    }

    @Override
    public String toString() {
        return "GeomesaConfigControllerService[id=" + getIdentifier() + "]";
    }
}