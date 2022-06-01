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

import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.*;

import java.util.Set;

import static jp.openstandia.connector.smarthr.SchemaDefinition.SchemaOption.*;

public class SmartHRDepartmentHandler implements SmartHRObjectHandler {

    public static final ObjectClass DEPARTMENT_OBJECT_CLASS = new ObjectClass("department");

    private static final Log LOGGER = Log.getLog(SmartHRDepartmentHandler.class);

    private final SmartHRConfiguration configuration;
    private final SmartHRClient client;
    private final SchemaDefinition schema;

    public SmartHRDepartmentHandler(SmartHRConfiguration configuration, SmartHRClient client, SchemaDefinition schema) {
        this.configuration = configuration;
        this.client = client;
        this.schema = schema;
    }

    public static SchemaDefinition.Builder createSchema() {
        SchemaDefinition.Builder sb = SchemaDefinition.newBuilder(DEPARTMENT_OBJECT_CLASS);

        // __UID__
        // The id for the department. Must be unique within the SmartHR tenant and unchangeable.
        // Also, it's UUID (case-insensitive).
        // We can't use "id" for the schema because of conflict in midpoint.
        sb.addUid("department_id",
                SchemaDefinition.Types.UUID,
                SmartHRClient.Department.class,
                SmartHRClient.Department.class,
                null,
                (source) -> source.id,
                REQUIRED, NOT_CREATABLE, NOT_UPDATABLE
        );

        // code (__NAME__)
        // The code for the department. Must be unique within the SmartHR tenant and changeable.
        // This is NOT required attribute in the tenant. If IDM doesn't provide, use __UID__ as __NAME__.
        // Also, it's case-sensitive.
        sb.addName("code",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Department.class,
                SmartHRClient.Department.class,
                (source, dest) -> dest.code = source,
                (source) -> StringUtil.isEmpty(source.code) ? source.id : source.code
        );

        sb.add("name",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Department.class,
                SmartHRClient.Department.class,
                (source, dest) -> dest.name = source,
                (source) -> source.name,
                REQUIRED
        );

        sb.add("parent_id",
                SchemaDefinition.Types.UUID,
                SmartHRClient.Department.class,
                SmartHRClient.Department.class,
                (source, dest) -> dest.parent_id = source,
                (source) -> source.parent_id
        );

        LOGGER.ok("The constructed department schema");

        return sb;
    }


    @Override
    public SchemaDefinition getSchema() {
        return schema;
    }

    @Override
    public Uid create(Set<Attribute> attributes) {
        SmartHRClient.Department dest = new SmartHRClient.Department();

        schema.apply(attributes, dest);

        Uid newUid = client.createDepartment(dest);

        return newUid;
    }

    @Override
    public Set<AttributeDelta> updateDelta(Uid uid, Set<AttributeDelta> modifications, OperationOptions options) {
        // To apply diff for multiple values, we need to fetch the current object
        SmartHRClient.Department current = client.getDepartment(uid, options, null);

        if (current == null) {
            throw new UnknownUidException(String.format("Not found crew. id: %s", uid.getUidValue()));
        }

        SmartHRClient.Department dest = new SmartHRClient.Department();

        schema.applyDelta(modifications, dest);

        client.updateDepartment(uid, dest);

        return null;
    }

    @Override
    public void delete(Uid uid, OperationOptions options) {
        client.deleteDepartment(uid, options);
    }

    @Override
    public int getByUid(Uid uid, ResultsHandler resultsHandler, OperationOptions options, Set<String> attributesToGet,
                        boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        SmartHRClient.Department dept = client.getDepartment(uid, options, attributesToGet);

        if (dept != null) {
            resultsHandler.handle(toConnectorObject(schema, dept, attributesToGet, allowPartialAttributeValues));
            return 1;
        }
        return 0;
    }

    @Override
    public int getByName(Name name, ResultsHandler resultsHandler, OperationOptions options, Set<String> attributesToGet,
                         boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        SmartHRClient.Department dept = client.getDepartment(name, options, attributesToGet);

        if (dept != null) {
            resultsHandler.handle(toConnectorObject(schema, dept, attributesToGet, allowPartialAttributeValues));
            return 1;
        }
        return 0;
    }

    @Override
    public int getAll(ResultsHandler resultsHandler, OperationOptions options, Set<String> attributesToGet,
                      boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        return client.getDepartments((crew) -> resultsHandler.handle(toConnectorObject(schema, crew, attributesToGet, allowPartialAttributeValues)),
                options, attributesToGet, pageSize, pageOffset);
    }
}
