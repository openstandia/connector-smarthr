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

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.*;

import java.util.Set;

import static jp.openstandia.connector.smarthr.SchemaDefinition.SchemaOption.*;

public class SmartHREmploymentTypeHandler implements SmartHRObjectHandler {

    public static final ObjectClass EMPLOYMENT_TYPE_OBJECT_CLASS = new ObjectClass("employment_type");

    private static final Log LOGGER = Log.getLog(SmartHREmploymentTypeHandler.class);

    private final SmartHRConfiguration configuration;
    private final SmartHRClient client;
    private final SchemaDefinition schema;

    public SmartHREmploymentTypeHandler(SmartHRConfiguration configuration, SmartHRClient client, SchemaDefinition schema) {
        this.configuration = configuration;
        this.client = client;
        this.schema = schema;
    }

    public static SchemaDefinition.Builder createSchema() {
        SchemaDefinition.Builder sb = SchemaDefinition.newBuilder(EMPLOYMENT_TYPE_OBJECT_CLASS);

        // __UID__
        // The id for the employment_type. Must be unique within the SmartHR tenant and unchangeable.
        // Also, it's UUID (case-insensitive).
        // We can't use "id" for the schema because of conflict in midpoint.
        sb.addUid("employment_type_id",
                SchemaDefinition.Types.UUID,
                SmartHRClient.EmploymentType.class,
                SmartHRClient.EmploymentType.class,
                null,
                (source) -> source.id,
                "id",
                REQUIRED, NOT_CREATABLE, NOT_UPDATABLE
        );

        // __NAME__
        sb.addName("name",
                SchemaDefinition.Types.STRING,
                SmartHRClient.EmploymentType.class,
                SmartHRClient.EmploymentType.class,
                (source, dest) -> dest.name = source,
                (source) -> source.name,
                null,
                REQUIRED
        );

        // Metadata (readonly)
        sb.add("created_at",
                SchemaDefinition.Types.DATETIME_STRING,
                SmartHRClient.EmploymentType.class,
                SmartHRClient.EmploymentType.class,
                null,
                (source) -> source.created_at,
                null
        );
        sb.add("updated_at",
                SchemaDefinition.Types.DATETIME_STRING,
                SmartHRClient.EmploymentType.class,
                SmartHRClient.EmploymentType.class,
                null,
                (source) -> source.updated_at,
                null
        );

        LOGGER.ok("The constructed employment_type schema");

        return sb;
    }


    @Override
    public SchemaDefinition getSchema() {
        return schema;
    }

    @Override
    public Uid create(Set<Attribute> attributes) {
        SmartHRClient.EmploymentType dest = new SmartHRClient.EmploymentType();

        schema.apply(attributes, dest);

        Uid newUid = client.createEmploymentType(dest);

        return newUid;
    }

    @Override
    public Set<AttributeDelta> updateDelta(Uid uid, Set<AttributeDelta> modifications, OperationOptions options) {
        // To apply diff for multiple values, we need to fetch the current object
        SmartHRClient.EmploymentType current = client.getEmploymentType(uid, options, null);

        if (current == null) {
            throw new UnknownUidException(String.format("Not found employment_type. id: %s", uid.getUidValue()));
        }

        SmartHRClient.EmploymentType dest = new SmartHRClient.EmploymentType();

        schema.applyDelta(modifications, dest);

        client.updateEmploymentType(uid, dest);

        return null;
    }

    @Override
    public void delete(Uid uid, OperationOptions options) {
        client.deleteEmploymentType(uid, options);
    }

    @Override
    public int getByUid(Uid uid, ResultsHandler resultsHandler, OperationOptions options,
                        Set<String> returnAttributesSet, Set<String> fetchFieldsSet,
                        boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        SmartHRClient.EmploymentType dept = client.getEmploymentType(uid, options, fetchFieldsSet);

        if (dept != null) {
            resultsHandler.handle(toConnectorObject(schema, dept, returnAttributesSet, allowPartialAttributeValues));
            return 1;
        }
        return 0;
    }

    @Override
    public int getByName(Name name, ResultsHandler resultsHandler, OperationOptions options,
                         Set<String> returnAttributesSet, Set<String> fetchFieldsSet,
                         boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        SmartHRClient.EmploymentType dept = client.getEmploymentType(name, options, fetchFieldsSet);

        if (dept != null) {
            resultsHandler.handle(toConnectorObject(schema, dept, returnAttributesSet, allowPartialAttributeValues));
            return 1;
        }
        return 0;
    }

    @Override
    public int getAll(ResultsHandler resultsHandler, OperationOptions options,
                      Set<String> returnAttributesSet, Set<String> fetchFieldsSet,
                      boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        return client.getEmploymentTypes((crew) -> resultsHandler.handle(toConnectorObject(schema, crew, returnAttributesSet, allowPartialAttributeValues)),
                options, fetchFieldsSet, pageSize, pageOffset);
    }
}
