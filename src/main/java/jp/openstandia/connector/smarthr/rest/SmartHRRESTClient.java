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
package jp.openstandia.connector.smarthr.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jp.openstandia.connector.smarthr.SmartHRClient;
import jp.openstandia.connector.smarthr.SmartHRConfiguration;
import jp.openstandia.connector.smarthr.SmartHRQueryHandler;
import okhttp3.*;
import org.identityconnectors.common.StringUtil;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.*;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.Uid;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static jp.openstandia.connector.smarthr.SmartHRCrewHandler.CREW_OBJECT_CLASS;
import static jp.openstandia.connector.smarthr.SmartHRDepartmentHandler.DEPARTMENT_OBJECT_CLASS;
import static jp.openstandia.connector.smarthr.SmartHREmploymentTypeHandler.EMPLOYMENT_TYPE_OBJECT_CLASS;
import static jp.openstandia.connector.smarthr.SmartHRJobTitleHandler.JOB_TITLE_OBJECT_CLASS;

public class SmartHRRESTClient implements SmartHRClient {

    private static final Log LOG = Log.getLog(SmartHRRESTClient.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String instanceName;
    private final SmartHRConfiguration configuration;
    private final OkHttpClient httpClient;

    public SmartHRRESTClient(String instanceName, SmartHRConfiguration configuration, OkHttpClient httpClient) {
        this.instanceName = instanceName;
        this.configuration = configuration;
        this.httpClient = httpClient;
    }

    @Override
    public void test() {
        try (Response response = get(getCustomSchemaFieldEndpointURL(configuration))) {
            if (response.code() != 200) {
                // Something wrong..
                String body = response.body().string();
                throw new ConnectionFailedException(String.format("Unexpected authentication response. statusCode: %s, body: %s",
                        response.code(),
                        body));
            }

            LOG.info("[{0}] SmartHR connector's connection test is OK", instanceName);

        } catch (IOException e) {
            throw new ConnectionFailedException("Cannot connect to SmartHR REST API", e);
        }
    }

    @Override
    public List<CrewCustomField> schema() {
        try (Response response = get(getCustomSchemaFieldEndpointURL(configuration))) {
            if (response.code() == 404) {
                // Don't throw
                return null;
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get SmartHR custom schema fields. statusCode: %d", response.code()));
            }

            if (response.code() == 404) {
                // Don't throw
                return null;
            }

            // Success
            List<CrewCustomField> fields = MAPPER.readValue(response.body().byteStream(),
                    new TypeReference<List<CrewCustomField>>() {
                    });

            return fields;

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call SmartHR get schema API", e);
        }
    }

    @Override
    public void close() {
    }

    // Crew

    @Override
    public Uid createCrew(Crew newCrew) throws AlreadyExistsException {
        try (Response response = post(getCrewEndpointURL(configuration), newCrew)) {
            if (response.code() == 400) {
                ErrorResponse error = MAPPER.readValue(response.body().byteStream(), ErrorResponse.class);
                if (error.isAlreadyExists()) {
                    throw new AlreadyExistsException(String.format("Crew '%s' already exists.", newCrew.emp_code));
                }
                throw new InvalidAttributeValueException(String.format("Bad request when creating a crew. emp_code: %s", newCrew.emp_code));
            }

            if (response.code() != 201) {
                throw new ConnectorIOException(String.format("Failed to create SmartHR crew: %s, statusCode: %d", newCrew.emp_code, response.code()));
            }

            Crew created = MAPPER.readValue(response.body().byteStream(), Crew.class);

            // Created
            if (created.emp_code != null) {
                return new Uid(created.id, new Name(created.emp_code));
            }
            // Use "id" as __NAME__
            return new Uid(created.id, new Name(created.id));

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call SmartHR create crew API", e);
        }
    }

    @Override
    public Crew getCrew(Uid uid, OperationOptions options, Set<String> attributesToGet) {
        try (Response response = get(getCrewEndpointURL(configuration, uid))) {
            if (response.code() == 404) {
                // Don't throw
                return null;
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get SmartHR crew: %s, statusCode: %d", uid.getUidValue(), response.code()));
            }

            Crew found = MAPPER.readValue(response.body().byteStream(), Crew.class);

            return found;

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call SmartHR get crew API", e);
        }
    }

    @Override
    public Crew getCrew(Name name, OperationOptions options, Set<String> attributesToGet) {
        Map<String, String> params = new HashMap<>();
        params.put("emp_code", name.getNameValue());

        try (Response response = get(getCrewEndpointURL(configuration), params, 1, 1)) {
            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get SmartHR crew by emp_code. statusCode: %d", response.code()));
            }

            // Success
            List<Crew> crews = MAPPER.readValue(response.body().byteStream(),
                    new TypeReference<List<Crew>>() {
                    });
            if (crews.size() == 0) {
                return null;
            }

            return crews.get(0);

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call SmartHR list crews API", e);
        }
    }

    @Override
    public void updateCrew(Uid uid, Crew update) {
        callPatch(CREW_OBJECT_CLASS, getCrewEndpointURL(configuration, uid), uid, update);
    }

    @Override
    public void deleteCrew(Uid uid, OperationOptions options) {
        callDelete(CREW_OBJECT_CLASS, getCrewEndpointURL(configuration, uid), uid);
    }

    @Override
    public int getCrews(SmartHRQueryHandler<Crew> handler, OperationOptions options, Set<String> attributesToGet, int queryPageSize, int pageOffset) {
        // Start from 1 in SmartHR
        int page = 1;
        if (pageOffset > 0) {
            page = (int) Math.ceil(pageOffset / queryPageSize) + 1;
        }

        // TODO Support sort by other attributes
        Map<String, String> params = new HashMap<>();
        params.put("sort", "emp_code");

        int totalCount;

        // If no requested pageOffset, fetch all pages
        while (true) {
            try (Response response = get(getCrewEndpointURL(configuration), params, page, queryPageSize)) {
                if (response.code() != 200) {
                    throw new ConnectorIOException(String.format("Failed to get SmartHR crews. statusCode: %d", response.code()));
                }

                // Success
                totalCount = getTotalCount(response);

                List<Crew> crews = MAPPER.readValue(response.body().byteStream(),
                        new TypeReference<List<Crew>>() {
                        });
                if (crews.size() == 0) {
                    break;
                }

                crews.stream().forEach(crew -> handler.handle(crew));

                if (pageOffset > 0) {
                    // If requested pageOffset, don't process paging
                    break;
                }

                page = getPage(response);
                queryPageSize = getPerPage(response);

                if ((page * queryPageSize) < totalCount) {
                    page++;
                    continue;
                }

                break;

            } catch (IOException e) {
                throw new ConnectorIOException("Failed to call SmartHR get crews API", e);
            }
        }

        return totalCount;
    }

    // Department

    @Override
    public Uid createDepartment(Department newDept) throws AlreadyExistsException {
        try (Response response = post(getDeptEndpointURL(configuration), newDept)) {
            if (response.code() == 400) {
                ErrorResponse error = MAPPER.readValue(response.body().byteStream(), ErrorResponse.class);
                if (error.isAlreadyExists()) {
                    throw new AlreadyExistsException(String.format("Department '%s' already exists.", newDept.code));
                }
                throw new InvalidAttributeValueException(String.format("Bad request when creating a department. emp_code: %s", newDept.code));
            }

            if (response.code() != 201) {
                throw new ConnectorIOException(String.format("Failed to create SmartHR department: %s, statusCode: %d", newDept.code, response.code()));
            }

            Department created = MAPPER.readValue(response.body().byteStream(), Department.class);

            // Created
            if (created.code != null) {
                return new Uid(created.id, new Name(created.code));
            }
            // Use "id" as __NAME__
            return new Uid(created.id, new Name(created.id));

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call SmartHR create department API", e);
        }
    }

    @Override
    public Department getDepartment(Uid uid, OperationOptions options, Set<String> attributesToGet) {
        try (Response response = get(getDeptEndpointURL(configuration, uid))) {
            if (response.code() == 404) {
                // Don't throw
                return null;
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get SmartHR department: %s, statusCode: %d", uid.getUidValue(), response.code()));
            }

            Department found = MAPPER.readValue(response.body().byteStream(), Department.class);

            return found;

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call SmartHR get department API", e);
        }
    }

    @Override
    public Department getDepartment(Name name, OperationOptions options, Set<String> attributesToGet) {
        Map<String, String> params = new HashMap<>();
        params.put("code", name.getNameValue());

        try (Response response = get(getDeptEndpointURL(configuration), params, 1, 1)) {
            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get SmartHR department by code. statusCode: %d", response.code()));
            }

            // Success
            List<Department> dept = MAPPER.readValue(response.body().byteStream(),
                    new TypeReference<List<Department>>() {
                    });
            if (dept.size() == 0) {
                return null;
            }

            return dept.get(0);

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call SmartHR list depts API", e);
        }
    }

    @Override
    public void updateDepartment(Uid uid, Department update) {
        callPatch(DEPARTMENT_OBJECT_CLASS, getDeptEndpointURL(configuration, uid), uid, update);
    }

    @Override
    public void deleteDepartment(Uid uid, OperationOptions options) {
        callDelete(DEPARTMENT_OBJECT_CLASS, getDeptEndpointURL(configuration, uid), uid);
    }

    @Override
    public int getDepartments(SmartHRQueryHandler<Department> handler, OperationOptions options, Set<String> attributesToGet, int queryPageSize, int pageOffset) {
        // Start from 1 in SmartHR
        int page = 1;
        if (pageOffset > 0) {
            page = (int) Math.ceil(pageOffset / queryPageSize) + 1;
        }

        // TODO Support sort by other attributes
        Map<String, String> params = new HashMap<>();
        params.put("sort", "code");

        int totalCount;

        // If no requested pageOffset, fetch all pages
        while (true) {
            try (Response response = get(getDeptEndpointURL(configuration), params, page, queryPageSize)) {
                if (response.code() != 200) {
                    throw new ConnectorIOException(String.format("Failed to get SmartHR departments. statusCode: %d", response.code()));
                }

                // Success
                totalCount = getTotalCount(response);

                List<Department> departments = MAPPER.readValue(response.body().byteStream(),
                        new TypeReference<List<Department>>() {
                        });
                if (departments.size() == 0) {
                    break;
                }

                departments.stream().forEach(dept -> handler.handle(dept));

                if (pageOffset > 0) {
                    // If requested pageOffset, don't process paging
                    break;
                }

                page = getPage(response);
                queryPageSize = getPerPage(response);

                if ((page * queryPageSize) < totalCount) {
                    page++;
                    continue;
                }

                break;

            } catch (IOException e) {
                throw new ConnectorIOException("Failed to call SmartHR get departments API", e);
            }
        }

        return totalCount;
    }

    // EmploymentType

    @Override
    public Uid createEmploymentType(EmploymentType newEmpType) throws AlreadyExistsException {
        try (Response response = post(getEmpTypeEndpointURL(configuration), newEmpType)) {
            if (response.code() == 400) {
                ErrorResponse error = MAPPER.readValue(response.body().byteStream(), ErrorResponse.class);
                if (error.isAlreadyExists()) {
                    throw new AlreadyExistsException(String.format("Department '%s' already exists.", newEmpType.name));
                }
                throw new InvalidAttributeValueException(String.format("Bad request when creating an employment_type. emp_code: %s", newEmpType.name));
            }

            if (response.code() != 201) {
                throw new ConnectorIOException(String.format("Failed to create SmartHR employment_type: %s, statusCode: %d", newEmpType.name, response.code()));
            }

            EmploymentType created = MAPPER.readValue(response.body().byteStream(), EmploymentType.class);

            // Created
            if (created.name != null) {
                return new Uid(created.id, new Name(created.name));
            }
            // Use "id" as __NAME__
            return new Uid(created.id, new Name(created.id));

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call SmartHR create employment_type API", e);
        }
    }

    @Override
    public EmploymentType getEmploymentType(Uid uid, OperationOptions options, Set<String> attributesToGet) {
        try (Response response = get(getEmpTypeEndpointURL(configuration, uid))) {
            if (response.code() == 404) {
                // Don't throw
                return null;
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get SmartHR employment_type: %s, statusCode: %d", uid.getUidValue(), response.code()));
            }

            EmploymentType found = MAPPER.readValue(response.body().byteStream(), EmploymentType.class);

            return found;

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call SmartHR get employment_type API", e);
        }
    }

    @Override
    public EmploymentType getEmploymentType(Name name, OperationOptions options, Set<String> attributesToGet) {
        try (Response response = get(getEmpTypeEndpointURL(configuration, name))) {
            if (response.code() == 404) {
                // Don't throw
                return null;
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get SmartHR employment_type: %s, statusCode: %d", name.getNameValue(), response.code()));
            }

            EmploymentType found = MAPPER.readValue(response.body().byteStream(), EmploymentType.class);

            return found;

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call SmartHR get employment_type API", e);
        }
    }

    @Override
    public void updateEmploymentType(Uid uid, EmploymentType update) {
        callPatch(EMPLOYMENT_TYPE_OBJECT_CLASS, getEmpTypeEndpointURL(configuration, uid), uid, update);
    }

    @Override
    public void deleteEmploymentType(Uid uid, OperationOptions options) {
        callDelete(EMPLOYMENT_TYPE_OBJECT_CLASS, getEmpTypeEndpointURL(configuration, uid), uid);
    }

    @Override
    public int getEmploymentTypes(SmartHRQueryHandler<EmploymentType> handler, OperationOptions options, Set<String> attributesToGet, int pageSize, int pageOffset) {
        // Start from 1 in SmartHR
        int page = 1;
        if (pageOffset > 0) {
            page = (int) Math.ceil(pageOffset / pageSize) + 1;
        }

        int totalCount;

        // If no requested pageOffset, fetch all pages
        while (true) {
            try (Response response = get(getEmpTypeEndpointURL(configuration), page, pageSize)) {
                if (response.code() != 200) {
                    throw new ConnectorIOException(String.format("Failed to get SmartHR employment_types. statusCode: %d", response.code()));
                }

                // Success
                totalCount = getTotalCount(response);

                List<EmploymentType> departments = MAPPER.readValue(response.body().byteStream(),
                        new TypeReference<List<EmploymentType>>() {
                        });
                if (departments.size() == 0) {
                    break;
                }

                departments.stream().forEach(dept -> handler.handle(dept));

                if (pageOffset > 0) {
                    // If requested pageOffset, don't process paging
                    break;
                }

                page = getPage(response);
                pageSize = getPerPage(response);

                if ((page * pageSize) < totalCount) {
                    page++;
                    continue;
                }

                break;

            } catch (IOException e) {
                throw new ConnectorIOException("Failed to call SmartHR get employment_types API", e);
            }
        }

        return totalCount;
    }

    // JobTitle

    @Override
    public Uid createJobTitle(JobTitle newJobTitle) throws AlreadyExistsException {
        try (Response response = post(getJobTitleEndpointURL(configuration), newJobTitle)) {
            if (response.code() == 400) {
                ErrorResponse error = MAPPER.readValue(response.body().byteStream(), ErrorResponse.class);
                if (error.isAlreadyExists()) {
                    throw new AlreadyExistsException(String.format("Department '%s' already exists.", newJobTitle.name));
                }
                throw new InvalidAttributeValueException(String.format("Bad request when creating an job_title. emp_code: %s", newJobTitle.name));
            }

            if (response.code() != 201) {
                throw new ConnectorIOException(String.format("Failed to create SmartHR job_title: %s, statusCode: %d", newJobTitle.name, response.code()));
            }

            JobTitle created = MAPPER.readValue(response.body().byteStream(), JobTitle.class);

            // Created
            if (created.name != null) {
                return new Uid(created.id, new Name(created.name));
            }
            // Use "id" as __NAME__
            return new Uid(created.id, new Name(created.id));

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call SmartHR create job_title API", e);
        }
    }

    @Override
    public JobTitle getJobTitle(Uid uid, OperationOptions options, Set<String> attributesToGet) {
        try (Response response = get(getJobTitleEndpointURL(configuration, uid))) {
            if (response.code() == 404) {
                // Don't throw
                return null;
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get SmartHR job_title: %s, statusCode: %d", uid.getUidValue(), response.code()));
            }

            JobTitle found = MAPPER.readValue(response.body().byteStream(), JobTitle.class);

            return found;

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call SmartHR get job_title API", e);
        }
    }

    @Override
    public JobTitle getJobTitle(Name name, OperationOptions options, Set<String> attributesToGet) {
        try (Response response = get(getJobTitleEndpointURL(configuration, name))) {
            if (response.code() == 404) {
                // Don't throw
                return null;
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get SmartHR job_title: %s, statusCode: %d", name.getNameValue(), response.code()));
            }

            JobTitle found = MAPPER.readValue(response.body().byteStream(), JobTitle.class);

            return found;

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call SmartHR get job_title API", e);
        }
    }

    @Override
    public void updateJobTitle(Uid uid, JobTitle update) {
        callPatch(JOB_TITLE_OBJECT_CLASS, getJobTitleEndpointURL(configuration, uid), uid, update);
    }

    @Override
    public void deleteJobTitle(Uid uid, OperationOptions options) {
        callDelete(JOB_TITLE_OBJECT_CLASS, getJobTitleEndpointURL(configuration, uid), uid);
    }

    @Override
    public int getJobTitles(SmartHRQueryHandler<JobTitle> handler, OperationOptions options, Set<String> attributesToGet, int pageSize, int pageOffset) {
        // Start from 1 in SmartHR
        int page = 1;
        if (pageOffset > 0) {
            page = (int) Math.ceil(pageOffset / pageSize) + 1;
        }

        int totalCount;

        // If no requested pageOffset, fetch all pages
        while (true) {
            try (Response response = get(getJobTitleEndpointURL(configuration), page, pageSize)) {
                if (response.code() != 200) {
                    throw new ConnectorIOException(String.format("Failed to get SmartHR job_titles. statusCode: %d", response.code()));
                }

                // Success
                totalCount = getTotalCount(response);

                List<JobTitle> departments = MAPPER.readValue(response.body().byteStream(),
                        new TypeReference<List<JobTitle>>() {
                        });
                if (departments.size() == 0) {
                    break;
                }

                departments.stream().forEach(dept -> handler.handle(dept));

                if (pageOffset > 0) {
                    // If requested pageOffset, don't process paging
                    break;
                }

                page = getPage(response);
                pageSize = getPerPage(response);

                if ((page * pageSize) < totalCount) {
                    page++;
                    continue;
                }

                break;

            } catch (IOException e) {
                throw new ConnectorIOException("Failed to call SmartHR get job_titles API", e);
            }
        }

        return totalCount;
    }

    // Utilities

    protected void callPatch(ObjectClass objectClass, String url, Uid uid, Object target) {
        try (Response response = patch(url, target)) {
            if (response.code() == 400) {
                throw new InvalidAttributeValueException(String.format("Bad request when updating %s: %s, response: %s",
                        objectClass.getObjectClassValue(), uid.getUidValue(), toBody(response)));
            }

            if (response.code() == 404) {
                throw new UnknownUidException(uid, objectClass);
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to patch SmartHR %s: %s, statusCode: %d, response: %s",
                        objectClass.getObjectClassValue(), uid.getUidValue(), response.code(), toBody(response)));
            }

            // Success

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Failed to patch SmartHR %s: %s",
                    objectClass.getObjectClassValue(), uid.getUidValue()), e);
        }
    }

    protected void callUpdate(ObjectClass objectClass, String url, Uid uid, Object target) {
        try (Response response = put(url, target)) {
            if (response.code() == 400) {
                throw new InvalidAttributeValueException(String.format("Bad request when updating %s: %s, response: %s",
                        objectClass.getObjectClassValue(), uid.getUidValue(), toBody(response)));
            }

            if (response.code() == 404) {
                throw new UnknownUidException(uid, objectClass);
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to update SmartHR %s: %s, statusCode: %d, response: %s",
                        objectClass.getObjectClassValue(), uid.getUidValue(), response.code(), toBody(response)));
            }

            // Success

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Failed to update SmartHR %s: %s",
                    objectClass.getObjectClassValue(), uid.getUidValue()), e);
        }
    }

    private String toBody(Response response) {
        ResponseBody resBody = response.body();
        if (resBody == null) {
            return null;
        }
        try {
            return resBody.string();
        } catch (IOException e) {
            LOG.error(e, "Unexpected smarthr API response");
            return "<failed_to_parse_response>";
        }
    }

    /**
     * Generic delete method.
     *
     * @param objectClass
     * @param url
     * @param uid
     */
    protected void callDelete(ObjectClass objectClass, String url, Uid uid) {
        try (Response response = delete(url)) {
            if (response.code() == 404) {
                throw new UnknownUidException(uid, objectClass);
            }

            if (response.code() != 204) {
                throw new ConnectorIOException(String.format("Failed to delete smarthr %s: %s, statusCode: %d, response: %s",
                        objectClass.getObjectClassValue(), uid.getUidValue(), response.code(), toBody(response)));
            }

            // Success

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Failed to delete smarthr %s: %s",
                    objectClass.getObjectClassValue(), uid.getUidValue()), e);
        }
    }

    private RequestBody createJsonRequestBody(Object body) {
        String bodyString;
        try {
            bodyString = MAPPER.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            throw new ConnectorIOException("Failed to write request json body", e);
        }

        return RequestBody.create(bodyString, MediaType.parse("application/json; charset=UTF-8"));
    }

    private void throwExceptionIfUnauthorized(Response response) throws ConnectorIOException {
        if (response.code() == 401) {
            throw new ConnectionFailedException("Cannot authenticate to the SmartHR REST API: " + response.message());
        }
    }

    private void throwExceptionIfServerError(Response response) throws ConnectorIOException {
        if (response.code() >= 500 && response.code() <= 599) {
            try {
                String body = response.body().string();
                throw new ConnectorIOException("SmartHR server error: " + body);
            } catch (IOException e) {
                throw new ConnectorIOException("SmartHR server error", e);
            }
        }
    }

    private Response get(String url) throws IOException {
        final Request request = new Request.Builder()
                .url(url)
                .get()
                .build();

        final Response response = httpClient.newCall(request).execute();

        throwExceptionIfUnauthorized(response);
        throwExceptionIfServerError(response);

        return response;
    }

    private Response get(String url, Map<String, String> params, int page, int queryPageSize) throws IOException {
        HttpUrl.Builder httpBuilder = HttpUrl.parse(url).newBuilder();
        httpBuilder.addQueryParameter("page", String.valueOf(page));
        httpBuilder.addQueryParameter("per_page", String.valueOf(queryPageSize));
        params.entrySet().stream().forEach(entry -> httpBuilder.addQueryParameter(entry.getKey(), entry.getValue()));

        final Request request = new Request.Builder()
                .url(httpBuilder.build())
                .get()
                .build();

        final Response response = httpClient.newCall(request).execute();

        throwExceptionIfUnauthorized(response);
        throwExceptionIfServerError(response);

        return response;
    }

    private Response get(String url, int page, int queryPageSize) throws IOException {
        HttpUrl.Builder httpBuilder = HttpUrl.parse(url).newBuilder();
        httpBuilder.addQueryParameter("page", String.valueOf(page));
        httpBuilder.addQueryParameter("per_page", String.valueOf(queryPageSize));

        final Request request = new Request.Builder()
                .url(httpBuilder.build())
                .get()
                .build();

        final Response response = httpClient.newCall(request).execute();

        throwExceptionIfUnauthorized(response);
        throwExceptionIfServerError(response);

        return response;
    }

    private int getPage(Response response) {
        String value = response.header("x-page");
        if (StringUtil.isNotEmpty(value)) {
            return Integer.parseInt(value);
        }
        return -1;
    }

    private int getPerPage(Response response) {
        String value = response.header("x-per-page");
        if (StringUtil.isNotEmpty(value)) {
            return Integer.parseInt(value);
        }
        return -1;
    }

    private int getTotalCount(Response response) {
        String value = response.header("x-total-count");
        if (StringUtil.isNotEmpty(value)) {
            return Integer.parseInt(value);
        }
        return -1;
    }

    private Response post(String url, Object body) throws IOException {
        RequestBody requestBody = createJsonRequestBody(body);

        for (int i = 0; i < 2; i++) {
            final Request request = new Request.Builder()
                    .url(url)
                    .post(requestBody)
                    .build();

            final Response response = httpClient.newCall(request).execute();

            throwExceptionIfUnauthorized(response);
            throwExceptionIfServerError(response);

            return response;
        }

        throw new ConnectorIOException("Failed to call post API");
    }

    private Response put(String url, Object body) throws IOException {
        RequestBody requestBody = createJsonRequestBody(body);

        for (int i = 0; i < 2; i++) {
            final Request request = new Request.Builder()
                    .url(url)
                    .put(requestBody)
                    .build();

            final Response response = httpClient.newCall(request).execute();

            throwExceptionIfUnauthorized(response);
            throwExceptionIfServerError(response);

            return response;
        }

        throw new ConnectorIOException("Failed to call post API");
    }

    private Response patch(String url, Object body) throws IOException {
        RequestBody requestBody = createJsonRequestBody(body);

        for (int i = 0; i < 2; i++) {
            final Request request = new Request.Builder()
                    .url(url)
                    .patch(requestBody)
                    .build();

            final Response response = httpClient.newCall(request).execute();

            throwExceptionIfUnauthorized(response);
            throwExceptionIfServerError(response);

            return response;
        }

        throw new ConnectorIOException("Failed to call patch API");
    }

    private Response delete(String url) throws IOException {
        for (int i = 0; i < 2; i++) {
            final Request request = new Request.Builder()
                    .url(url)
                    .delete()
                    .build();

            final Response response = httpClient.newCall(request).execute();

            throwExceptionIfUnauthorized(response);
            throwExceptionIfServerError(response);

            return response;
        }

        throw new ConnectorIOException("Failed to call delete API");
    }
}
