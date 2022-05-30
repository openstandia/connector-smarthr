/*
 *  Copyright Nomura Research Institute, Ltd.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package jp.openstandia.connector.smarthr;

import jp.openstandia.connector.smarthr.rest.SmartHRRESTClient;
import okhttp3.*;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.AlreadyExistsException;
import org.identityconnectors.framework.common.exceptions.ConnectorException;
import org.identityconnectors.framework.common.exceptions.ConnectorIOException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.common.objects.filter.FilterTranslator;
import org.identityconnectors.framework.spi.Configuration;
import org.identityconnectors.framework.spi.ConnectorClass;
import org.identityconnectors.framework.spi.InstanceNameAware;
import org.identityconnectors.framework.spi.PoolableConnector;
import org.identityconnectors.framework.spi.operations.*;

import java.io.IOException;
import java.net.*;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static jp.openstandia.connector.smarthr.SmartHRConnectionGroupHandler.CONNECTION_GROUP_OBJECT_CLASS;
import static jp.openstandia.connector.smarthr.SmartHRConnectionHandler.CONNECTION_OBJECT_CLASS;
import static jp.openstandia.connector.smarthr.SmartHRUserGroupHandler.USER_GROUP_OBJECT_CLASS;
import static jp.openstandia.connector.smarthr.SmartHRUserHandler.USER_OBJECT_CLASS;

@ConnectorClass(configurationClass = SmartHRConfiguration.class, displayNameKey = "NRI OpenStandia SmartHR Connector")
public class SmartHRConnector implements PoolableConnector, CreateOp, UpdateDeltaOp, DeleteOp, SchemaOp, TestOp, SearchOp<SmartHRFilter>, InstanceNameAware {

    private static final Log LOG = Log.getLog(SmartHRConnector.class);

    protected SmartHRConfiguration configuration;
    protected SmartHRClient client;

    private SmartHRSchema cachedSchema;
    private String instanceName;

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public void init(Configuration configuration) {
        this.configuration = (SmartHRConfiguration) configuration;

        try {
            authenticateResource();
        } catch (RuntimeException e) {
            throw processRuntimeException(e);
        }

        LOG.ok("Connector {0} successfully initialized", getClass().getName());
    }

    protected void authenticateResource() {
        OkHttpClient.Builder okHttpBuilder = new OkHttpClient.Builder();
        okHttpBuilder.connectTimeout(20, TimeUnit.SECONDS);
        okHttpBuilder.readTimeout(15, TimeUnit.SECONDS);
        okHttpBuilder.writeTimeout(15, TimeUnit.SECONDS);
        okHttpBuilder.addInterceptor(getInterceptor());

        // Setup cookie manager for multiple smarthr nodes with sticky session cookie
        CookieManager cookieManager = new CookieManager();
        cookieManager.setCookiePolicy(CookiePolicy.ACCEPT_ORIGINAL_SERVER);
        okHttpBuilder.cookieJar(new JavaNetCookieJar(cookieManager));

        // Setup http proxy aware httpClient
        if (StringUtil.isNotEmpty(configuration.getHttpProxyHost())) {
            okHttpBuilder.proxy(new Proxy(Proxy.Type.HTTP,
                    new InetSocketAddress(configuration.getHttpProxyHost(), configuration.getHttpProxyPort())));

            if (StringUtil.isNotEmpty(configuration.getHttpProxyUser()) && configuration.getHttpProxyPassword() != null) {
                configuration.getHttpProxyPassword().access(c -> {
                    okHttpBuilder.proxyAuthenticator((Route route, Response response) -> {
                        String credential = Credentials.basic(configuration.getHttpProxyUser(), String.valueOf(c));
                        return response.request().newBuilder()
                                .header("Proxy-Authorization", credential)
                                .build();
                    });
                });
            }
        }

        OkHttpClient httpClient = okHttpBuilder.build();

        client = new SmartHRRESTClient(instanceName, configuration, httpClient);

        // Verify we can access the smarthr server
        client.auth();
    }

    private Interceptor getInterceptor() {
        return new Interceptor() {
            @Override
            public Response intercept(Chain chain) throws IOException {
                Request newRequest = chain.request().newBuilder()
                        .addHeader("Accept", "application/json")
                        .build();
                return chain.proceed(newRequest);
            }
        };
    }

    @Override
    public Schema schema() {
        try {
            List<SmartHRClient.SmartHRSchemaRepresentation> smarthrSchema = this.client.schema();
            cachedSchema = new SmartHRSchema(configuration, client, smarthrSchema);
            return cachedSchema.schema;

        } catch (RuntimeException e) {
            throw processRuntimeException(e);
        }
    }

    private SmartHRSchema geSchema() {
        // Load schema map if it's not loaded yet
        if (cachedSchema == null) {
            schema();
        }
        return cachedSchema;
    }

    protected SmartHRObjectHandler createSmartHRObjectHandler(ObjectClass objectClass) {
        if (objectClass == null) {
            throw new InvalidAttributeValueException("ObjectClass value not provided");
        }

        if (objectClass.equals(USER_OBJECT_CLASS)) {
            return new SmartHRUserHandler(configuration, client, geSchema());

        } else if (objectClass.equals(USER_GROUP_OBJECT_CLASS)) {
            return new SmartHRUserGroupHandler(configuration, client, geSchema());

        } else if (objectClass.equals(CONNECTION_OBJECT_CLASS)) {
            return new SmartHRConnectionHandler(configuration, client, geSchema());

        } else if (objectClass.equals(CONNECTION_GROUP_OBJECT_CLASS)) {
            return new SmartHRConnectionGroupHandler(configuration, client, geSchema());

        } else {
            throw new InvalidAttributeValueException("Unsupported object class " + objectClass);
        }
    }

    @Override
    public Uid create(ObjectClass objectClass, Set<Attribute> createAttributes, OperationOptions options) {
        if (createAttributes == null || createAttributes.isEmpty()) {
            throw new InvalidAttributeValueException("Attributes not provided or empty");
        }

        try {
            return createSmartHRObjectHandler(objectClass).create(createAttributes);

        } catch (RuntimeException e) {
            throw processRuntimeException(e);
        }
    }

    @Override
    public Set<AttributeDelta> updateDelta(ObjectClass objectClass, Uid uid, Set<AttributeDelta> modifications, OperationOptions options) {
        if (uid == null) {
            throw new InvalidAttributeValueException("uid not provided");
        }

        try {
            return createSmartHRObjectHandler(objectClass).updateDelta(uid, modifications, options);

        } catch (RuntimeException e) {
            throw processRuntimeException(e);
        }
    }

    @Override
    public void delete(ObjectClass objectClass, Uid uid, OperationOptions options) {
        if (uid == null) {
            throw new InvalidAttributeValueException("uid not provided");
        }

        try {
            createSmartHRObjectHandler(objectClass).delete(uid, options);

        } catch (RuntimeException e) {
            throw processRuntimeException(e);
        }
    }

    @Override
    public FilterTranslator<SmartHRFilter> createFilterTranslator(ObjectClass objectClass, OperationOptions options) {
        return new SmartHRFilterTranslator(objectClass, options);
    }

    @Override
    public void executeQuery(ObjectClass objectClass, SmartHRFilter filter, ResultsHandler resultsHandler, OperationOptions options) {
        createSmartHRObjectHandler(objectClass).query(filter, resultsHandler, options);
    }

    @Override
    public void test() {
        try {
            dispose();
            authenticateResource();
        } catch (RuntimeException e) {
            throw processRuntimeException(e);
        }
    }

    @Override
    public void dispose() {
        client.close();
        this.client = null;
    }

    @Override
    public void checkAlive() {
        // Do nothing
    }

    @Override
    public void setInstanceName(String instanceName) {
        this.instanceName = instanceName;
    }

    protected ConnectorException processRuntimeException(RuntimeException e) {
        if (e instanceof ConnectorException) {
            // Write error log because IDM might not write full stack trace
            // It's hard to debug the error
            if (e instanceof AlreadyExistsException) {
                LOG.warn(e, "Detect smarthr connector error");
            } else {
                LOG.error(e, "Detect smarthr connector error");
            }
            return (ConnectorException) e;
        }
        return new ConnectorIOException(e);
    }
}
