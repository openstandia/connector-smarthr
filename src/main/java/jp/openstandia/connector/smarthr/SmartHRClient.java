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
        String url = configuration.getEndpointURL();
        return String.format("%sapi/v1/crew_custom_field_template_groups", url);
    }

    default String getCustomSchemaFieldEndpointURL(SmartHRConfiguration configuration) {
        String url = configuration.getEndpointURL();
        return String.format("%sapi/v1/crew_custom_field_templates", url);
    }

    default String getCrewEndpointURL(SmartHRConfiguration configuration) {
        String url = configuration.getEndpointURL();
        return String.format("%sapi/v1/crews", url);
    }

    default String getCrewEndpointURL(SmartHRConfiguration configuration, Uid uid) {
        String url = configuration.getEndpointURL();
        return String.format("%sapi/v1/crews/%s", url, uid.getUidValue());
    }

    default String getDeptEndpointURL(SmartHRConfiguration configuration) {
        String url = configuration.getEndpointURL();
        return String.format("%sapi/v1/departments", url);
    }

    default String getDeptEndpointURL(SmartHRConfiguration configuration, Uid uid) {
        String url = configuration.getEndpointURL();
        return String.format("%sapi/v1/departments/%s", url, uid.getUidValue());
    }

    default String getEmpTypeEndpointURL(SmartHRConfiguration configuration) {
        String url = configuration.getEndpointURL();
        return String.format("%sapi/v1/employment_types", url);
    }

    default String getEmpTypeEndpointURL(SmartHRConfiguration configuration, Uid uid) {
        String url = configuration.getEndpointURL();
        return String.format("%sapi/v1/employment_types/%s", url, uid.getUidValue());
    }

    default String getEmpTypeEndpointURL(SmartHRConfiguration configuration, Name name) {
        String url = configuration.getEndpointURL();
        return String.format("%sapi/v1/employment_types/%s", url, name.getNameValue());
    }

    default String getJobTitleEndpointURL(SmartHRConfiguration configuration) {
        String url = configuration.getEndpointURL();
        return String.format("%sapi/v1/job_titles", url);
    }

    default String getJobTitleEndpointURL(SmartHRConfiguration configuration, Uid uid) {
        String url = configuration.getEndpointURL();
        return String.format("%sapi/v1/job_titles/%s", url, uid.getUidValue());
    }

    default String getJobTitleEndpointURL(SmartHRConfiguration configuration, Name name) {
        String url = configuration.getEndpointURL();
        return String.format("%sapi/v1/job_titles/%s", url, name.getNameValue());
    }

    void close();

    // Crew

    Uid createCrew(Crew newCrew) throws AlreadyExistsException;

    Crew getCrew(Uid uid, OperationOptions options, Set<String> attributesToGet);

    Crew getCrew(Name name, OperationOptions options, Set<String> attributesToGet);

    void updateCrew(Uid uid, Crew update);

    void deleteCrew(Uid uid, OperationOptions options);

    int getCrews(SmartHRQueryHandler<Crew> handler, OperationOptions options, Set<String> attributesToGet, int pageSize, int pageOffset);

    // Department

    Uid createDepartment(Department newCrew) throws AlreadyExistsException;

    Department getDepartment(Uid uid, OperationOptions options, Set<String> attributesToGet);

    Department getDepartment(Name name, OperationOptions options, Set<String> attributesToGet);

    void updateDepartment(Uid uid, Department update);

    void deleteDepartment(Uid uid, OperationOptions options);

    int getDepartments(SmartHRQueryHandler<Department> handler, OperationOptions options, Set<String> attributesToGet, int pageSize, int pageOffset);

    // EmploymentType

    Uid createEmploymentType(EmploymentType newCrew) throws AlreadyExistsException;

    EmploymentType getEmploymentType(Uid uid, OperationOptions options, Set<String> attributesToGet);

    EmploymentType getEmploymentType(Name name, OperationOptions options, Set<String> attributesToGet);

    void updateEmploymentType(Uid uid, EmploymentType update);

    void deleteEmploymentType(Uid uid, OperationOptions options);

    int getEmploymentTypes(SmartHRQueryHandler<EmploymentType> handler, OperationOptions options, Set<String> attributesToGet, int pageSize, int pageOffset);

    // JobTitle

    Uid createJobTitle(JobTitle newCrew) throws AlreadyExistsException;

    JobTitle getJobTitle(Uid uid, OperationOptions options, Set<String> attributesToGet);

    JobTitle getJobTitle(Name name, OperationOptions options, Set<String> attributesToGet);

    void updateJobTitle(Uid uid, JobTitle update);

    void deleteJobTitle(Uid uid, OperationOptions options);

    int getJobTitles(SmartHRQueryHandler<JobTitle> handler, OperationOptions options, Set<String> attributesToGet, int pageSize, int pageOffset);

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

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    class EmploymentType {
        public String id;
        public String name;
        public String preset_type;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    class Department {
        public String id;
        public String name;
        public String code;
        public String parent_id;
        public Department parent;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    class JobTitle {
        public String id;
        public String name;
        public Integer rank;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
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
    class CrewCustomField {
        public String id;
        public String name;
        public String type;
        public String group_id;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    class ErrorResponse {
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
}