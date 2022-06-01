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
import java.util.stream.Collectors;

import static jp.openstandia.connector.smarthr.SchemaDefinition.SchemaOption.*;

public class SmartHRDepartmentHandler implements SmartHRObjectHandler {

    public static final ObjectClass CONNECTION_GROUP_OBJECT_CLASS = new ObjectClass("department");

    private static final Log LOGGER = Log.getLog(SmartHRDepartmentHandler.class);

    // Unique, auto-generated and unchangeable within the smarthr server
    private static final String ATTR_IDENTIFIER = "identifier";
    // Unique and changeable within the smarthr server
    // This is composed by 'parentIdentifier' and 'name' to make it unique
    // The format is <parentIdentifier>/<name>
    private static final String ATTR_NAME_WITH_PARENT_IDENTIFIER = "name-with-parentIdentifier";
    // ORGANIZATIONAL or BALANCING
    private static final String ATTR_TYPE = "type";

    // Attributes
    private static final String ATTR_ENABLE_SESSION_AFFINITY = "enable-session-affinity";
    private static final String ATTR_MAX_CONNECTIONS = "max-connections";
    private static final String ATTR_MAX_CONNECTIONS_PER_USER = "max-connections-per-user";

    private final SmartHRConfiguration configuration;
    private final SmartHRClient client;
    private final SchemaDefinition schema;

    public SmartHRDepartmentHandler(SmartHRConfiguration configuration, SmartHRClient client, SchemaDefinition schema) {
        this.configuration = configuration;
        this.client = client;
        this.schema = schema;
    }

    public static SchemaDefinition.Builder createSchema() {
        SchemaDefinition.Builder sb = SchemaDefinition.newBuilder(CONNECTION_GROUP_OBJECT_CLASS);

        // __UID__
        // The id for the crew. Must be unique within the SmartHR tenant and unchangeable.
        // Also, it's UUID (case-insensitive).
        // We can't use "id" for the schema because of conflict in midpoint.
        sb.addUid("departmentId",
                SchemaDefinition.Types.UUID,
                SmartHRClient.Department.class,
                SmartHRClient.Department.class,
                null,
                (source) -> source.id,
                REQUIRED, NOT_CREATABLE, NOT_UPDATABLE
        );

        // emp_code (__NAME__)
        // The emp_code for the user. Must be unique within the SmartHR tenant and changeable.
        // This is NOT required attribute in the tenant. If IDM doesn't provide, use __UID__ as __NAME__.
        // Also, it's case-sensitive.
        sb.addName("code",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Department.class,
                SmartHRClient.Department.class,
                (source, dest) -> dest.code = source,
                (source) -> StringUtil.isEmpty(source.code) ? source.id : source.code
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
        SmartHRClient.Crew dest = new SmartHRClient.Crew();

        schema.apply(attributes, dest);

        Uid newUid = client.createCrew(dest);

        return newUid;
    }

    @Override
    public Set<AttributeDelta> updateDelta(Uid uid, Set<AttributeDelta> modifications, OperationOptions options) {
        // To apply diff for multiple values, we need to fetch the current object
        SmartHRClient.Crew current = client.getCrew(uid, options, null);

        if (current == null) {
            throw new UnknownUidException(String.format("Not found crew. id: %s", uid.getUidValue()));
        }

        SmartHRClient.Crew dest = new SmartHRClient.Crew();
        dest.department_ids = current.departments.stream().map(d -> d.id).collect(Collectors.toList());

        schema.applyDelta(modifications, dest);

        client.updateCrew(uid, dest);

        return null;
    }

    @Override
    public void delete(Uid uid, OperationOptions options) {
        client.deleteCrew(uid, options);
    }

    @Override
    public void getByUid(Uid uid, ResultsHandler resultsHandler, OperationOptions options, Set<String> attributesToGet,
                         boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        SmartHRClient.Crew user = client.getCrew(uid, options, attributesToGet);

        if (user != null) {
            resultsHandler.handle(toConnectorObject(schema, user, attributesToGet, allowPartialAttributeValues));
        }
    }

    @Override
    public void getByName(Name name, ResultsHandler resultsHandler, OperationOptions options, Set<String> attributesToGet,
                          boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        SmartHRClient.Crew crew = client.getCrew(name, options, attributesToGet);

        if (crew != null) {
            resultsHandler.handle(toConnectorObject(schema, crew, attributesToGet, allowPartialAttributeValues));
        }
    }

    @Override
    public void getAll(ResultsHandler resultsHandler, OperationOptions options, Set<String> attributesToGet,
                       boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        client.getCrews((crew) -> resultsHandler.handle(toConnectorObject(schema, crew, attributesToGet, allowPartialAttributeValues)),
                options, attributesToGet, pageSize, pageOffset);
    }
}
