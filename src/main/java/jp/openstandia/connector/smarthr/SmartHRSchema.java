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

import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptionInfoBuilder;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SchemaBuilder;
import org.identityconnectors.framework.spi.operations.SearchOp;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Schema for SmartHR objects.
 *
 * @author Hiroyuki Wada
 */
public class SmartHRSchema {

    private final SmartHRConfiguration configuration;
    private final SmartHRClient client;

    public final Schema schema;

    private Map<String, SmartHRObjectHandler> schemaHandlerMap;

    public SmartHRSchema(SmartHRConfiguration configuration, SmartHRClient client,
                         List<SmartHRClient.CrewCustomField> smarthrSchema) {
        this.configuration = configuration;
        this.client = client;
        this.schemaHandlerMap = new HashMap<>();

        SchemaBuilder schemaBuilder = new SchemaBuilder(SmartHRConnector.class);

        buildSchema(schemaBuilder, SmartHRCrewHandler.createSchema(smarthrSchema).build(),
                (schema) -> new SmartHRCrewHandler(configuration, client, schema));

        buildSchema(schemaBuilder, SmartHRDepartmentHandler.createSchema().build(),
                (schema) -> new SmartHRDepartmentHandler(configuration, client, schema));

        buildSchema(schemaBuilder, SmartHREmploymentTypeHandler.createSchema().build(),
                (schema) -> new SmartHREmploymentTypeHandler(configuration, client, schema));

        buildSchema(schemaBuilder, SmartHRJobTitleHandler.createSchema().build(),
                (schema) -> new SmartHRJobTitleHandler(configuration, client, schema));

        buildSchema(schemaBuilder, SmartHRCompanyHandler.createSchema().build(),
                (schema) -> new SmartHRCompanyHandler(configuration, client, schema));

        buildSchema(schemaBuilder, SmartHRBizEstablishmentHandler.createSchema().build(),
                (schema) -> new SmartHRBizEstablishmentHandler(configuration, client, schema));

        // Define operation options
        schemaBuilder.defineOperationOption(OperationOptionInfoBuilder.buildAttributesToGet(), SearchOp.class);
        schemaBuilder.defineOperationOption(OperationOptionInfoBuilder.buildReturnDefaultAttributes(), SearchOp.class);
        schemaBuilder.defineOperationOption(OperationOptionInfoBuilder.buildPageSize(), SearchOp.class);
        schemaBuilder.defineOperationOption(OperationOptionInfoBuilder.buildPagedResultsOffset(), SearchOp.class);

        this.schema = schemaBuilder.build();

    }

    private void buildSchema(SchemaBuilder builder, SchemaDefinition schemaDefinition, Function<SchemaDefinition, SmartHRObjectHandler> callback) {
        builder.defineObjectClass(schemaDefinition.getObjectClassInfo());
        SmartHRObjectHandler handler = callback.apply(schemaDefinition);
        this.schemaHandlerMap.put(schemaDefinition.getType(), handler);
    }

    public SmartHRObjectHandler getSchemaHandler(ObjectClass objectClass) {
        return schemaHandlerMap.get(objectClass.getObjectClassValue());
    }
}