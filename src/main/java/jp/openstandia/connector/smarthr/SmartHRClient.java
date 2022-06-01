package jp.openstandia.connector.smarthr;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.identityconnectors.framework.common.exceptions.AlreadyExistsException;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.Uid;

import java.util.List;
import java.util.Set;


public interface SmartHRClient {
    void test();

    List<CrewCustomField> schema();

    default String getCustomSchemaGroupEndpointURL(SmartHRConfiguration configuration) {
        String url = configuration.getSmartHREndpointURL();
        return String.format("%sv1/crew_custom_field_template_groups", url);
    }

    default String getCustomSchemaFieldEndpointURL(SmartHRConfiguration configuration) {
        String url = configuration.getSmartHREndpointURL();
        return String.format("%sv1/crew_custom_field_templates", url);
    }

    default String getCrewEndpointURL(SmartHRConfiguration configuration) {
        String url = configuration.getSmartHREndpointURL();
        return String.format("%sv1/crews", url);
    }

    default String getCrewEndpointURL(SmartHRConfiguration configuration, Uid uid) {
        String url = configuration.getSmartHREndpointURL();
        return String.format("%sv1/crews/%s", url, uid.getUidValue());
    }

    void close();

    // Crew

    Uid createCrew(Crew newCrew) throws AlreadyExistsException;

    Crew getCrew(Uid uid, OperationOptions options, Set<String> attributesToGet);

    Crew getCrew(Name name, OperationOptions options, Set<String> attributesToGet);

    void updateCrew(Uid uid, Crew update);

    void deleteCrew(Uid uid, OperationOptions options);

    void getCrews(SmartHRQueryHandler<Crew> handler, OperationOptions options, Set<String> attributesToGet, int pageSize, int pageOffset);

    // Department

    Uid createDepartment(Crew newCrew) throws AlreadyExistsException;

    Crew getDepartment(Uid uid, OperationOptions options, Set<String> attributesToGet);

    void updateDepartment(Uid uid, Crew update);

    void deleteDepartment(Uid uid, OperationOptions options);

    void getDepartments(SmartHRQueryHandler<Crew> handler, OperationOptions options, Set<String> attributesToGet, int pageSize, int pageOffset);

    // JSON Representation

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    class Crew {
        public String id;
        public String emp_code;

        public String emp_status;
        public String birth_at;
        public List<String> department_ids;
        public List<Department> departments;
        public String last_name;
        public String first_name;
        public String last_name_yomi;
        public String first_name_yomi;
        public String business_last_name;
        public String business_first_name;
        public String business_last_name_yomi;
        public String business_first_name_yomi;
        public String gender;
        public String email;
        public String department;
        public String tel_number;
        public String contract_type;
        public String contract_start_on;
        public String contract_end_on;
        public String contract_renewal_type;
        public String created_at;
        public String updated_at;
        public String entered_at;
        public String resigned_at;
        public String biz_establishment_id;
        public String employment_type_id;
        public EmploymentType employment_type;
        public String position;
        public String occupation;
        public List<CustomField> custom_fields;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    class EmploymentType {
        public String id;
        public String name;
        public String preset_type;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    class Department {
        public String id;
        public String name;
        public String code;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    class CustomField {
        public String template_id;
        public String value;
        public Template template;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    class Template {
        public String id;
        public String name;
        public String type;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    class SmartHRSchemaRepresentation {
        public String name;
        public List<CrewCustomField> fields;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    class CrewCustomField {
        public String id;
        public String name;
        public String type;
        public String group_id;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    class SmartHRErrorRepresentation {
        public int code;
        public String type;
        public String message;
        public List<ErrorDetail> errors;

        public boolean isAlreadyExists() {
            return code == 1;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    class ErrorDetail {
        public String message;
        public String resource;
        public String field;
    }

    class PatchOperation {
        final public String op;
        final public String path;
        final public String value;

        public PatchOperation(String op, String path, String value) {
            this.op = op;
            this.path = path;
            this.value = value;
        }
    }
}