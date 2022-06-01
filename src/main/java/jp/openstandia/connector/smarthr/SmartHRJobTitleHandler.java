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

public class SmartHRJobTitleHandler implements SmartHRObjectHandler {

    public static final ObjectClass JOB_TITLE_OBJECT_CLASS = new ObjectClass("job_title");

    private static final Log LOGGER = Log.getLog(SmartHRJobTitleHandler.class);

    private final SmartHRConfiguration configuration;
    private final SmartHRClient client;
    private final SchemaDefinition schema;

    public SmartHRJobTitleHandler(SmartHRConfiguration configuration, SmartHRClient client, SchemaDefinition schema) {
        this.configuration = configuration;
        this.client = client;
        this.schema = schema;
    }

    public static SchemaDefinition.Builder createSchema() {
        SchemaDefinition.Builder sb = SchemaDefinition.newBuilder(JOB_TITLE_OBJECT_CLASS);

        // __UID__
        // The id for the employment_type. Must be unique within the SmartHR tenant and unchangeable.
        // Also, it's UUID (case-insensitive).
        // We can't use "id" for the schema because of conflict in midpoint.
        sb.addUid("job_title_id",
                SchemaDefinition.Types.UUID,
                SmartHRClient.JobTitle.class,
                SmartHRClient.JobTitle.class,
                null,
                (source) -> source.id,
                REQUIRED, NOT_CREATABLE, NOT_UPDATABLE
        );

        // __NAME__
        sb.addName("name",
                SchemaDefinition.Types.STRING,
                SmartHRClient.JobTitle.class,
                SmartHRClient.JobTitle.class,
                (source, dest) -> dest.name = source,
                (source) -> source.name,
                REQUIRED
        );

        sb.add("rank",
                SchemaDefinition.Types.INTEGER,
                SmartHRClient.JobTitle.class,
                SmartHRClient.JobTitle.class,
                (source, dest) -> dest.rank = source,
                (source) -> source.rank
        );

        // Metadata (readonly)
        sb.add("created_at",
                SchemaDefinition.Types.DATETIME_STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                null,
                (source) -> source.created_at
        );
        sb.add("updated_at",
                SchemaDefinition.Types.DATETIME_STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                null,
                (source) -> source.updated_at
        );

        LOGGER.ok("The constructed job_title schema");

        return sb;
    }

    @Override
    public SchemaDefinition getSchema() {
        return schema;
    }

    @Override
    public Uid create(Set<Attribute> attributes) {
        SmartHRClient.JobTitle dest = new SmartHRClient.JobTitle();

        schema.apply(attributes, dest);

        Uid newUid = client.createJobTitle(dest);

        return newUid;
    }

    @Override
    public Set<AttributeDelta> updateDelta(Uid uid, Set<AttributeDelta> modifications, OperationOptions options) {
        // To apply diff for multiple values, we need to fetch the current object
        SmartHRClient.JobTitle current = client.getJobTitle(uid, options, null);

        if (current == null) {
            throw new UnknownUidException(String.format("Not found job_title. id: %s", uid.getUidValue()));
        }

        SmartHRClient.JobTitle dest = new SmartHRClient.JobTitle();

        schema.applyDelta(modifications, dest);

        client.updateJobTitle(uid, dest);

        return null;
    }

    @Override
    public void delete(Uid uid, OperationOptions options) {
        client.deleteJobTitle(uid, options);
    }

    @Override
    public int getByUid(Uid uid, ResultsHandler resultsHandler, OperationOptions options, Set<String> attributesToGet,
                        boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        SmartHRClient.JobTitle dept = client.getJobTitle(uid, options, attributesToGet);

        if (dept != null) {
            resultsHandler.handle(toConnectorObject(schema, dept, attributesToGet, allowPartialAttributeValues));
            return 1;
        }
        return 0;
    }

    @Override
    public int getByName(Name name, ResultsHandler resultsHandler, OperationOptions options, Set<String> attributesToGet,
                         boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        SmartHRClient.JobTitle dept = client.getJobTitle(name, options, attributesToGet);

        if (dept != null) {
            resultsHandler.handle(toConnectorObject(schema, dept, attributesToGet, allowPartialAttributeValues));
            return 1;
        }
        return 0;
    }

    @Override
    public int getAll(ResultsHandler resultsHandler, OperationOptions options, Set<String> attributesToGet,
                      boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        return client.getJobTitles((crew) -> resultsHandler.handle(toConnectorObject(schema, crew, attributesToGet, allowPartialAttributeValues)),
                options, attributesToGet, pageSize, pageOffset);
    }
}
