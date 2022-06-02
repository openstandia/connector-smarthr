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
package jp.openstandia.connector.smarthr.testutil;

import jp.openstandia.connector.smarthr.SmartHRClient;
import jp.openstandia.connector.smarthr.SmartHRQueryHandler;
import org.identityconnectors.framework.common.exceptions.AlreadyExistsException;
import org.identityconnectors.framework.common.objects.Name;
import org.identityconnectors.framework.common.objects.OperationOptions;
import org.identityconnectors.framework.common.objects.Uid;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class MockClient implements SmartHRClient {

    private static final MockClient INSTANCE = new MockClient();

    public boolean closed = false;

    public void init() {
        closed = false;
    }

    private MockClient() {
    }

    public static MockClient instance() {
        return INSTANCE;
    }

    @Override
    public void test() {

    }

    @Override
    public List<CrewCustomField> schema() {
        return Collections.emptyList();
    }

    @Override
    public void close() {

    }

    @Override
    public Uid createCrew(Crew newCrew) throws AlreadyExistsException {
        return null;
    }

    @Override
    public Crew getCrew(Uid uid, OperationOptions options, Set<String> attributesToGet) {
        return null;
    }

    @Override
    public Crew getCrew(Name name, OperationOptions options, Set<String> attributesToGet) {
        return null;
    }

    @Override
    public void updateCrew(Uid uid, Crew update) {

    }

    @Override
    public void deleteCrew(Uid uid, OperationOptions options) {

    }

    @Override
    public int getCrews(SmartHRQueryHandler<Crew> handler, OperationOptions options, Set<String> attributesToGet, int pageSize, int pageOffset) {

        return pageSize;
    }

    @Override
    public Uid createDepartment(Department newCrew) throws AlreadyExistsException {
        return null;
    }

    @Override
    public Department getDepartment(Uid uid, OperationOptions options, Set<String> attributesToGet) {
        return null;
    }

    @Override
    public Department getDepartment(Name name, OperationOptions options, Set<String> attributesToGet) {
        return null;
    }

    @Override
    public void updateDepartment(Uid uid, Department update) {

    }

    @Override
    public void deleteDepartment(Uid uid, OperationOptions options) {

    }

    @Override
    public int getDepartments(SmartHRQueryHandler<Department> handler, OperationOptions options, Set<String> attributesToGet, int pageSize, int pageOffset) {

        return pageSize;
    }

    @Override
    public Uid createEmploymentType(EmploymentType newCrew) throws AlreadyExistsException {
        return null;
    }

    @Override
    public EmploymentType getEmploymentType(Uid uid, OperationOptions options, Set<String> attributesToGet) {
        return null;
    }

    @Override
    public EmploymentType getEmploymentType(Name name, OperationOptions options, Set<String> attributesToGet) {
        return null;
    }

    @Override
    public void updateEmploymentType(Uid uid, EmploymentType update) {

    }

    @Override
    public void deleteEmploymentType(Uid uid, OperationOptions options) {

    }

    @Override
    public int getEmploymentTypes(SmartHRQueryHandler<EmploymentType> handler, OperationOptions options, Set<String> attributesToGet, int pageSize, int pageOffset) {
        return 0;
    }

    @Override
    public Uid createJobTitle(JobTitle newCrew) throws AlreadyExistsException {
        return null;
    }

    @Override
    public JobTitle getJobTitle(Uid uid, OperationOptions options, Set<String> attributesToGet) {
        return null;
    }

    @Override
    public JobTitle getJobTitle(Name name, OperationOptions options, Set<String> attributesToGet) {
        return null;
    }

    @Override
    public void updateJobTitle(Uid uid, JobTitle update) {

    }

    @Override
    public void deleteJobTitle(Uid uid, OperationOptions options) {

    }

    @Override
    public int getJobTitles(SmartHRQueryHandler<JobTitle> handler, OperationOptions options, Set<String> attributesToGet, int pageSize, int pageOffset) {
        return 0;
    }

    @Override
    public Company getCompany(Uid uid, OperationOptions options, Set<String> fetchFieldsSet) {
        return null;
    }

    @Override
    public Company getCompany(Name name, OperationOptions options, Set<String> fetchFieldsSet) {
        return null;
    }

    @Override
    public int getCompanies(SmartHRQueryHandler<Company> handler, OperationOptions options, Set<String> fetchFieldsSet, int pageSize, int pageOffset) {
        return 0;
    }

    @Override
    public BizEstablishment getBizEstablishment(Uid uid, OperationOptions options, Set<String> fetchFieldsSet) {
        return null;
    }

    @Override
    public BizEstablishment getBizEstablishment(Name name, OperationOptions options, Set<String> fetchFieldsSet) {
        return null;
    }

    @Override
    public int getBizEstablishments(SmartHRQueryHandler<BizEstablishment> handler, OperationOptions options, Set<String> fetchFieldsSet, int pageSize, int pageOffset) {
        return 0;
    }
}
