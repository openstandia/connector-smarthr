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

        SchemaBuilder schemaBuilder = new SchemaBuilder(SmartHRConnector.class);

        SchemaDefinition crewSchema = SmartHRCrewHandler.createSchema(smarthrSchema).build();
        schemaBuilder.defineObjectClass(crewSchema.getObjectClassInfo());

        SchemaDefinition deptSchema = SmartHRDepartmentHandler.createSchema().build();
        schemaBuilder.defineObjectClass(deptSchema.getObjectClassInfo());

        SchemaDefinition empTypeSchema = SmartHREmploymentTypeHandler.createSchema().build();
        schemaBuilder.defineObjectClass(empTypeSchema.getObjectClassInfo());

        SchemaDefinition jobTitleSchema = SmartHRJobTitleHandler.createSchema().build();
        schemaBuilder.defineObjectClass(jobTitleSchema.getObjectClassInfo());

        schemaBuilder.defineOperationOption(OperationOptionInfoBuilder.buildAttributesToGet(), SearchOp.class);
        schemaBuilder.defineOperationOption(OperationOptionInfoBuilder.buildReturnDefaultAttributes(), SearchOp.class);
        schemaBuilder.defineOperationOption(OperationOptionInfoBuilder.buildPageSize(), SearchOp.class);
        schemaBuilder.defineOperationOption(OperationOptionInfoBuilder.buildPagedResultsOffset(), SearchOp.class);

        this.schema = schemaBuilder.build();

        this.schemaHandlerMap = new HashMap<>();
        this.schemaHandlerMap.put(crewSchema.getType(), new SmartHRCrewHandler(configuration, client, crewSchema));
        this.schemaHandlerMap.put(deptSchema.getType(), new SmartHRDepartmentHandler(configuration, client, deptSchema));
        this.schemaHandlerMap.put(empTypeSchema.getType(), new SmartHREmploymentTypeHandler(configuration, client, empTypeSchema));
        this.schemaHandlerMap.put(jobTitleSchema.getType(), new SmartHRJobTitleHandler(configuration, client, jobTitleSchema));
    }

    public SmartHRObjectHandler getSchemaHandler(ObjectClass objectClass) {
        return schemaHandlerMap.get(objectClass.getObjectClassValue());
    }
}