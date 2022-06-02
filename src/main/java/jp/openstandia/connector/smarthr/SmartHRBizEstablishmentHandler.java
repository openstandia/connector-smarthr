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

public class SmartHRBizEstablishmentHandler implements SmartHRObjectHandler {

    public static final ObjectClass BIZ_ESTABLISHMENT_OBJECT_CLASS = new ObjectClass("biz_establishment");

    private static final Log LOGGER = Log.getLog(SmartHRBizEstablishmentHandler.class);

    private final SmartHRConfiguration configuration;
    private final SmartHRClient client;
    private final SchemaDefinition schema;

    public SmartHRBizEstablishmentHandler(SmartHRConfiguration configuration, SmartHRClient client, SchemaDefinition schema) {
        this.configuration = configuration;
        this.client = client;
        this.schema = schema;
    }

    public static SchemaDefinition.Builder createSchema() {
        SchemaDefinition.Builder sb = SchemaDefinition.newBuilder(BIZ_ESTABLISHMENT_OBJECT_CLASS);

        // __UID__
        // The id for the biz_establishment. Must be unique within the SmartHR tenant and unchangeable.
        // Also, it's UUID (case-insensitive).
        // We can't use "id" for the schema because of conflict in midpoint.
        sb.addUid("biz_establishment_id",
                SchemaDefinition.Types.UUID,
                SmartHRClient.BizEstablishment.class,
                SmartHRClient.BizEstablishment.class,
                null,
                (source) -> source.id,
                "id",
                REQUIRED, NOT_CREATABLE, NOT_UPDATABLE
        );

        // __NAME__
        sb.addName("name",
                SchemaDefinition.Types.STRING,
                SmartHRClient.BizEstablishment.class,
                SmartHRClient.BizEstablishment.class,
                (source, dest) -> dest.name = source,
                (source) -> source.name,
                null,
                REQUIRED, NOT_CREATABLE, NOT_UPDATABLE
        );

        // 社会保険
        sb.add("soc_ins_name",
                SchemaDefinition.Types.STRING,
                SmartHRClient.BizEstablishment.class,
                SmartHRClient.BizEstablishment.class,
                (source, dest) -> dest.soc_ins_name = source,
                (source) -> source.soc_ins_name,
                null,
                REQUIRED, NOT_CREATABLE, NOT_UPDATABLE, NOT_RETURN_BY_DEFAULT
        );

        sb.add("soc_ins_tel_number",
                SchemaDefinition.Types.STRING,
                SmartHRClient.BizEstablishment.class,
                SmartHRClient.BizEstablishment.class,
                (source, dest) -> dest.soc_ins_tel_number = source,
                (source) -> source.soc_ins_tel_number,
                null,
                REQUIRED, NOT_CREATABLE, NOT_UPDATABLE, NOT_RETURN_BY_DEFAULT
        );

        sb.add("lab_ins_name",
                SchemaDefinition.Types.STRING,
                SmartHRClient.BizEstablishment.class,
                SmartHRClient.BizEstablishment.class,
                (source, dest) -> dest.lab_ins_name = source,
                (source) -> source.lab_ins_name,
                null,
                REQUIRED, NOT_CREATABLE, NOT_UPDATABLE, NOT_RETURN_BY_DEFAULT
        );

        // 労働保険
        sb.add("lab_ins_tel_number",
                SchemaDefinition.Types.STRING,
                SmartHRClient.BizEstablishment.class,
                SmartHRClient.BizEstablishment.class,
                (source, dest) -> dest.lab_ins_tel_number = source,
                (source) -> source.lab_ins_tel_number,
                null,
                REQUIRED, NOT_CREATABLE, NOT_UPDATABLE, NOT_RETURN_BY_DEFAULT
        );

        // Metadata (readonly)
        sb.add("created_at",
                SchemaDefinition.Types.DATETIME_STRING,
                SmartHRClient.BizEstablishment.class,
                SmartHRClient.BizEstablishment.class,
                null,
                (source) -> source.created_at,
                null,
                NOT_CREATABLE, NOT_UPDATABLE
        );
        sb.add("updated_at",
                SchemaDefinition.Types.DATETIME_STRING,
                SmartHRClient.BizEstablishment.class,
                SmartHRClient.BizEstablishment.class,
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
        throw new UnsupportedOperationException("SmartHR API doesn't support creating biz_establishment");
    }

    @Override
    public Set<AttributeDelta> updateDelta(Uid uid, Set<AttributeDelta> modifications, OperationOptions options) {
        throw new UnsupportedOperationException("SmartHR API doesn't support creating biz_establishment");
    }

    @Override
    public void delete(Uid uid, OperationOptions options) {
        throw new UnsupportedOperationException("SmartHR API doesn't support creating biz_establishment");
    }

    @Override
    public int getByUid(Uid uid, ResultsHandler resultsHandler, OperationOptions options,
                        Set<String> returnAttributesSet, Set<String> fetchFieldsSet,
                        boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        SmartHRClient.BizEstablishment dept = client.getBizEstablishment(uid, options, fetchFieldsSet);

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
        SmartHRClient.BizEstablishment dept = client.getBizEstablishment(name, options, fetchFieldsSet);

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
        return client.getBizEstablishments((crew) -> resultsHandler.handle(toConnectorObject(schema, crew, returnAttributesSet, allowPartialAttributeValues)),
                options, fetchFieldsSet, pageSize, pageOffset);
    }
}
