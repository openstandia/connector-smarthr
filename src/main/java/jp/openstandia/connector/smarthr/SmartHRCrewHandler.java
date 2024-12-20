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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.*;

import java.math.BigDecimal;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

import static jp.openstandia.connector.smarthr.SchemaDefinition.SchemaOption.*;
import static jp.openstandia.connector.smarthr.SmartHRUtils.toZoneDateTime;

public class SmartHRCrewHandler implements SmartHRObjectHandler {

    public static final ObjectClass CREW_OBJECT_CLASS = new ObjectClass("crew");

    private static final Log LOGGER = Log.getLog(SmartHRCrewHandler.class);

    private final SmartHRConfiguration configuration;
    private final SmartHRClient client;
    private final SchemaDefinition schema;

    public SmartHRCrewHandler(SmartHRConfiguration configuration, SmartHRClient client,
                              SchemaDefinition schema) {
        this.configuration = configuration;
        this.client = client;
        this.schema = schema;
    }

    private static final ObjectMapper mapper = new ObjectMapper();

    public static SchemaDefinition.Builder createSchema(List<SmartHRClient.CrewCustomField> schema) {
        SchemaDefinition.Builder sb = SchemaDefinition.newBuilder(CREW_OBJECT_CLASS);

        // __UID__
        // The id for the crew. Must be unique within the SmartHR tenant and unchangeable.
        // Also, it's UUID (case-insensitive).
        // We can't use "id" for the schema because of conflict in midpoint.
        sb.addUid("crew_id",
                SchemaDefinition.Types.UUID,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                null,
                (source) -> source.id,
                "id",
                REQUIRED, NOT_CREATABLE, NOT_UPDATABLE
        );

        // emp_code (__NAME__)
        // The emp_code for the user. Must be unique within the SmartHR tenant and changeable.
        // This is NOT required attribute in the tenant. If IDM doesn't provide, use __UID__ as __NAME__.
        // Also, it's case-sensitive.
        sb.addName("emp_code",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.emp_code = source,
                (source) -> StringUtil.isEmpty(source.emp_code) ? source.id : source.emp_code,
                null
        );

        // 基本情報
        sb.add("last_name",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.last_name = source,
                (source) -> source.last_name,
                null
        );
        sb.add("first_name",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.first_name = source,
                (source) -> source.first_name,
                null
        );
        sb.add("last_name_yomi",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.last_name_yomi = source,
                (source) -> source.last_name_yomi,
                null
        );
        sb.add("first_name_yomi",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.first_name_yomi = source,
                (source) -> source.first_name_yomi,
                null
        );

        sb.add("business_last_name",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.business_last_name = source,
                (source) -> source.business_last_name,
                null
        );
        sb.add("business_first_name",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.business_first_name = source,
                (source) -> source.business_first_name,
                null
        );
        sb.add("business_last_name_yomi",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.business_last_name_yomi = source,
                (source) -> source.business_last_name_yomi,
                null
        );
        sb.add("business_first_name_yomi",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.business_first_name_yomi = source,
                (source) -> source.business_first_name_yomi,
                null
        );

        sb.add("birth_at",
                SchemaDefinition.Types.DATE_STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.birth_at = source,
                (source) -> source.birth_at,
                null
        );
        sb.add("gender",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.gender = source,
                (source) -> source.gender,
                null
        );
        sb.add("email",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.email = source,
                (source) -> source.email,
                null
        );

        // 入退社情報
        sb.add("emp_status",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.emp_status = source,
                (source) -> source.emp_status,
                null,
                REQUIRED
        );
        sb.add("entered_at",
                SchemaDefinition.Types.DATE_STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.entered_at = source,
                (source) -> source.entered_at,
                null
        );
        sb.add("resigned_at",
                SchemaDefinition.Types.DATE_STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.resigned_at = source,
                (source) -> source.resigned_at,
                null
        );

        // 業務情報
        // emp_code is __NAME__
//        sb.add("emp_code",
//                SchemaBuilder.Types.STRING,
//                SmartHRClient.SmartHRCrewRepresentation.class,
//                SmartHRClient.SmartHRCrewRepresentation.class,
//                (source, dest) -> dest.emp_code = source,
//                (source) -> source.emp_code
//        );
        sb.add("biz_establishment_id",
                SchemaDefinition.Types.UUID,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.biz_establishment_id = source,
                (source) -> source.biz_establishment_id,
                null
        );
        sb.add("employment_type.id",
                SchemaDefinition.Types.UUID,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.employment_type_id = source,
                (source) -> source.employment_type != null ? source.employment_type.id : null,
                "employment_type"
        );
        // readonly
        sb.add("employment_type.name",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                null,
                (source) -> source.employment_type != null ? source.employment_type.name : null,
                "employment_type",
                NOT_CREATABLE, NOT_UPDATABLE
        );
        // readonly
        sb.add("employment_type.preset_type",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                null,
                (source) -> source.employment_type != null ? source.employment_type.preset_type : null,
                "employment_type",
                NOT_CREATABLE, NOT_UPDATABLE
        );
        sb.add("position",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.position = source,
                (source) -> source.position,
                null
        );
        // readonly
        sb.add("raw_positions",
                SchemaDefinition.Types.JSON,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                null,
                (source) -> {
                    if (source.positions == null) {
                        return null;
                    }
                    try {
                        return mapper.writeValueAsString(source.positions);
                    } catch (JsonProcessingException ignore) {
                        return null;
                    }
                },
                "positions",
                NOT_CREATABLE, NOT_UPDATABLE, NOT_RETURN_BY_DEFAULT
        );
        sb.add("occupation",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.occupation = source,
                (source) -> source.occupation,
                null
        );

        // 部署情報
        // readonly
        sb.add("department",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                null,
                (source) -> source.department,
                null,
                NOT_CREATABLE, NOT_UPDATABLE
        );
        // Association
        sb.addAsMultiple("departments",
                SchemaDefinition.Types.UUID,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.department_ids = source,
                (add, dest) -> dest.department_ids.addAll(add),
                (remove, dest) -> dest.department_ids.removeAll(remove),
                (source) -> source.departments != null ? source.departments.stream()
                        .filter(Objects::nonNull)
                        .map(d -> d.id).collect(Collectors.toList()) : null,
                null
        );
        // readonly
        sb.add("raw_departments",
                SchemaDefinition.Types.JSON,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                null,
                (source) -> {
                    if (source.departments == null) {
                        return null;
                    }
                    try {
                        return mapper.writeValueAsString(source.departments);
                    } catch (JsonProcessingException ignore) {
                        return null;
                    }
                },
                "departments",
                NOT_CREATABLE, NOT_UPDATABLE, NOT_RETURN_BY_DEFAULT
        );

        // 現住所と連絡先
        sb.add("tel_number",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.tel_number = source,
                (source) -> source.tel_number,
                null,
                NOT_RETURN_BY_DEFAULT
        );

        // 雇用契約情報
        sb.add("contract_type",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.contract_type = source,
                (source) -> source.contract_type,
                null
        );
        sb.add("contract_start_on",
                SchemaDefinition.Types.DATE_STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.contract_start_on = source,
                (source) -> source.contract_start_on,
                null
        );
        sb.add("contract_end_on",
                SchemaDefinition.Types.DATE_STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.contract_end_on = source,
                (source) -> source.contract_end_on,
                null
        );
        sb.add("contract_renewal_type",
                SchemaDefinition.Types.STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                (source, dest) -> dest.contract_renewal_type = source,
                (source) -> source.contract_renewal_type,
                null
        );

        // Metadata (readonly)
        sb.add("created_at",
                SchemaDefinition.Types.DATETIME_STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                null,
                (source) -> source.created_at,
                null,
                NOT_CREATABLE, NOT_UPDATABLE
        );
        sb.add("updated_at",
                SchemaDefinition.Types.DATETIME_STRING,
                SmartHRClient.Crew.class,
                SmartHRClient.Crew.class,
                null,
                (source) -> source.updated_at,
                null,
                NOT_CREATABLE, NOT_UPDATABLE
        );

        // Custom Fields
        for (SmartHRClient.CrewCustomField field : schema) {
            final String name = "custom." + field.id;

            switch (field.type) {
                case "string":
                case "text":
                case "enum":
                    sb.add(name,
                            SchemaDefinition.Types.STRING,
                            SmartHRClient.Crew.class,
                            SmartHRClient.Crew.class,
                            (source, dest) -> {
                                SmartHRClient.CustomField value = new SmartHRClient.CustomField();
                                value.template_id = field.id;
                                value.value = source;
                                dest.custom_fields.add(value);
                            },
                            (source) -> {
                                Optional<SmartHRClient.CustomField> value = source.custom_fields.stream()
                                        .filter(f -> f.template.id.equals(field.id))
                                        .findFirst();
                                if (value.isPresent()) {
                                    return value.get().value;
                                }
                                return null;
                            },
                            "custom_fields"
                    );
                    break;
                case "decimal":
                    sb.add(name,
                            SchemaDefinition.Types.BIG_DECIMAL,
                            SmartHRClient.Crew.class,
                            SmartHRClient.Crew.class,
                            (source, dest) -> {
                                SmartHRClient.CustomField value = new SmartHRClient.CustomField();
                                value.template_id = field.id;
                                value.value = source.toPlainString();
                                dest.custom_fields.add(value);
                            },
                            (source) -> {
                                Optional<SmartHRClient.CustomField> value = source.custom_fields.stream()
                                        .filter(f -> f.template.id.equals(field.id))
                                        .findFirst();
                                if (value.isPresent()) {
                                    return new BigDecimal(value.get().value);
                                }
                                return null;
                            },
                            "custom_fields"
                    );
                    break;
                case "date":
                    sb.add(name,
                            SchemaDefinition.Types.DATE,
                            SmartHRClient.Crew.class,
                            SmartHRClient.Crew.class,
                            (source, dest) -> {
                                SmartHRClient.CustomField value = new SmartHRClient.CustomField();
                                value.template_id = field.id;
                                value.value = source.format(DateTimeFormatter.ISO_LOCAL_DATE);
                                dest.custom_fields.add(value);
                            },
                            (source) -> {
                                Optional<SmartHRClient.CustomField> value = source.custom_fields.stream()
                                        .filter(f -> f.template.id.equals(field.id))
                                        .findFirst();
                                if (value.isPresent()) {
                                    return toZoneDateTime(value.get().value);
                                }
                                return null;
                            },
                            "custom_fields"
                    );
                    break;
                case "file":
                default:
                    LOGGER.info("Not supported crew custom field type: {0}", field.type);
                    continue;
            }
        }

        LOGGER.ok("The constructed crew schema");

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
        if (current.departments == null) {
            dest.department_ids = new ArrayList<>();
        } else {
            dest.department_ids = current.departments.stream().map(d -> d.id).collect(Collectors.toList());
        }

        schema.applyDelta(modifications, dest);

        client.updateCrew(uid, dest);

        return null;
    }

    @Override
    public void delete(Uid uid, OperationOptions options) {
        client.deleteCrew(uid, options);
    }

    @Override
    public int getByUid(Uid uid, ResultsHandler resultsHandler, OperationOptions options,
                        Set<String> returnAttributesSet, Set<String> fetchFieldsSet,
                        boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        SmartHRClient.Crew crew = client.getCrew(uid, options, fetchFieldsSet);

        if (crew != null) {
            resultsHandler.handle(toConnectorObject(schema, crew, returnAttributesSet, allowPartialAttributeValues));
            return 1;
        }
        return 0;
    }

    @Override
    public int getByName(Name name, ResultsHandler resultsHandler, OperationOptions options,
                         Set<String> returnAttributesSet, Set<String> fetchFieldsSet,
                         boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        SmartHRClient.Crew user = client.getCrew(name, options, fetchFieldsSet);

        if (user != null) {
            resultsHandler.handle(toConnectorObject(schema, user, returnAttributesSet, allowPartialAttributeValues));
            return 1;
        }
        return 0;
    }

    @Override
    public int getAll(ResultsHandler resultsHandler, OperationOptions options,
                      Set<String> returnAttributesSet, Set<String> fetchFieldsSet,
                      boolean allowPartialAttributeValues, int pageSize, int pageOffset) {
        return client.getCrews((crew) -> resultsHandler.handle(toConnectorObject(schema, crew, returnAttributesSet, allowPartialAttributeValues)),
                options, fetchFieldsSet, pageSize, pageOffset);
    }
}
