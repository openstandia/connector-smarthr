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
import org.identityconnectors.framework.common.objects.*;

import java.util.Set;

import static jp.openstandia.connector.smarthr.SchemaDefinition.SchemaOption.*;

public class SmartHRCompanyHandler implements SmartHRObjectHandler {

    public static final ObjectClass COMPANY_OBJECT_CLASS = new ObjectClass("company");

    private static final Log LOGGER = Log.getLog(SmartHRCompanyHandler.class);

    private final SmartHRConfiguration configuration;
    private final SmartHRClient client;
    private final SchemaDefinition schema;

    public SmartHRCompanyHandler(SmartHRConfiguration configuration, SmartHRClient client, SchemaDefinition schema) {
        this.configuration = configuration;
        this.client = client;
        this.schema = schema;
    }

    public static SchemaDefinition.Builder createSchema() {
        SchemaDefinition.Builder sb = SchemaDefinition.newBuilder(COMPANY_OBJECT_CLASS);

        // __UID__
        // The id for the company. Must be unique within the SmartHR tenant and unchangeable.
        // Also, it's UUID (case-insensitive).
        // We can't use "id" for the schema because of conflict in midpoint.
        sb.addUid("company_id",
                SchemaDefinition.Types.UUID,
                SmartHRClient.Company.class,
                SmartHRClient.Company.class,
                null,
                (source) -> source.id,
                "id",
                REQUIRED, NOT_CREATABLE, NOT_UPDATABLE
        );

        // __NAME__
        sb.addName("name",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Company.class,
                SmartHRClient.Company.class,
                (source, dest) -> dest.name = source,
                (source) -> source.name,
                null,
                REQUIRED, NOT_CREATABLE, NOT_UPDATABLE
        );

        // 基本情報
        sb.add("tel_number",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Company.class,
                SmartHRClient.Company.class,
                (source, dest) -> dest.tel_number = source,
                (source) -> source.tel_number,
                null,
                REQUIRED, NOT_CREATABLE, NOT_UPDATABLE
        );

        // Metadata (readonly)
        sb.add("created_at",
                SchemaDefinition.Types.DATETIME_STRING,
                SmartHRClient.Company.class,
                SmartHRClient.Company.class,
                null,
                (source) -> source.created_at,
                null,
                NOT_CREATABLE, NOT_UPDATABLE
        );
        sb.add("updated_at",
                SchemaDefinition.Types.DATETIME_STRING,
                SmartHRClient.Company.class,
                SmartHRClient.Company.class,
                null,
                (source) -> source.updated_at,
                null,
                NOT_CREATABLE, NOT_UPDATABLE
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
        throw new UnsupportedOperationException("SmartHR API doesn't support creating company");
    }

    @Override
    public Set<AttributeDelta> updateDelta(Uid uid, Set<AttributeDelta> modifications, OperationOptions options) {
        throw new UnsupportedOperationException("SmartHR API doesn't support updating company");
    }

    @Override
    public void delete(Uid uid, OperationOptions options) {
        throw new UnsupportedOperationException("SmartHR API doesn't support deleting company");
    }

    @Override
    public int getByUid(Uid uid, ResultsHandler resultsHandler, OperationOptions options,
                        Set<String> returnAttributesSet, Set<String> fetchFieldsSet,
                        boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        SmartHRClient.Company dept = client.getCompany(uid, options, fetchFieldsSet);

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
        SmartHRClient.Company dept = client.getCompany(name, options, fetchFieldsSet);

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
        return client.getCompanies((company) -> resultsHandler.handle(toConnectorObject(schema, company, returnAttributesSet, allowPartialAttributeValues)),
                options, fetchFieldsSet, pageSize, pageOffset);
    }
}
