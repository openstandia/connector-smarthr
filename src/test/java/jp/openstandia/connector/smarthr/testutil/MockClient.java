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
import jp.openstandia.connector.smarthr.SmartHRSchema;
import org.identityconnectors.framework.common.exceptions.AlreadyExistsException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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
    public boolean auth() {
        return false;
    }

    @Override
    public String getAuthToken() {
        return null;
    }

    @Override
    public List<SmartHRSchemaRepresentation> schema() {
        return new ArrayList<>();
    }

    @Override
    public void close() {

    }

    @Override
    public Uid createUser(SmartHRSchema schema, Set<Attribute> createAttributes) throws AlreadyExistsException {
        return null;
    }

    @Override
    public void updateUser(SmartHRSchema schema, Uid uid, Set<AttributeDelta> modifications, OperationOptions options) throws UnknownUidException {

    }

    @Override
    public void deleteUser(SmartHRSchema schema, Uid uid, OperationOptions options) throws UnknownUidException {

    }

    @Override
    public void getUsers(SmartHRSchema schema, SmartHRQueryHandler<SmartHRUserRepresentation> handler, OperationOptions options, Set<String> attributesToGet, int queryPageSize) {

    }

    @Override
    public SmartHRUserRepresentation getUser(SmartHRSchema schema, Uid uid, OperationOptions options, Set<String> attributesToGet) {
        return null;
    }

    @Override
    public void getUserGroupsForUser(String username, SmartHRQueryHandler<String> handler) {

    }

    @Override
    public void assignUserGroupsToUser(Uid username, List<String> addGroups, List<String> removeGroups) {

    }

    @Override
    public Uid createUserGroup(SmartHRSchema schema, Set<Attribute> createAttributes) throws AlreadyExistsException {
        return null;
    }

    @Override
    public void updateUserGroup(SmartHRSchema schema, Uid uid, Set<AttributeDelta> modifications, OperationOptions options) throws UnknownUidException {

    }

    @Override
    public void deleteUserGroup(SmartHRSchema schema, Uid uid, OperationOptions options) throws UnknownUidException {

    }

    @Override
    public void getUserGroups(SmartHRSchema schema, SmartHRQueryHandler<SmartHRUserGroupRepresentation> handler, OperationOptions options, Set<String> attributesToGet, int queryPageSize) {

    }

    @Override
    public SmartHRUserGroupRepresentation getUserGroup(SmartHRSchema schema, Uid uid, OperationOptions options, Set<String> attributesToGet) {
        return null;
    }

    @Override
    public void getUsersForUserGroup(String groupName, SmartHRQueryHandler<String> handler) {

    }

    @Override
    public void assignUsersToUserGroup(Uid groupName, List<String> addUsers, List<String> removeUsers) {

    }

    @Override
    public void getUserGroupsForUserGroup(String groupName, SmartHRQueryHandler<String> handler) {

    }

    @Override
    public void assignUserGroupsToUserGroup(Uid groupName, List<String> addGroups, List<String> removeGroups) {

    }

    @Override
    public void assignUserPermissionsToUser(Uid userUid, List<String> addPermissions, List<String> removePermissions) {

    }

    @Override
    public void assignSystemPermissionsToUser(Uid userUid, List<String> addPermissions, List<String> removePermissions) {

    }

    @Override
    public void assignSystemPermissionsToUserGroup(Uid groupUid, List<String> addPermissions, List<String> removePermissions) {

    }

    @Override
    public SmartHRPermissionRepresentation getPermissionsForUser(String username) {
        return null;
    }

    @Override
    public SmartHRPermissionRepresentation getPermissionsForUserGroup(String groupName) {
        return null;
    }

    @Override
    public Uid createConnection(SmartHRSchema schema, Set<Attribute> attributes) {
        return null;
    }

    @Override
    public void updateConnection(SmartHRSchema schema, Uid uid, Set<AttributeDelta> modifications, OperationOptions options) {

    }

    @Override
    public void deleteConnection(SmartHRSchema schema, Uid uid, OperationOptions options) {

    }

    @Override
    public void getConnections(SmartHRSchema schema, SmartHRQueryHandler<SmartHRConnectionRepresentation> handler, OperationOptions options, Set<String> attributesToGet, int queryPageSize) {

    }

    @Override
    public SmartHRConnectionRepresentation getConnection(SmartHRSchema schema, Uid uid, OperationOptions options, Set<String> attributesToGet) {
        return null;
    }

    @Override
    public SmartHRConnectionRepresentation getConnection(SmartHRSchema schema, Name name, OperationOptions options, Set<String> attributesToGet) {
        return null;
    }

    @Override
    public Map<String, String> getParameters(String identifier) {
        return null;
    }

    @Override
    public void assignConnectionsToUser(Uid userUid, List<String> addConnections, List<String> removeConnections) {

    }

    @Override
    public void assignConnectionsToUserGroup(Uid userGroupUid, List<String> addConnections, List<String> removeConnections) {

    }

    @Override
    public Uid createConnectionGroup(SmartHRSchema schema, Set<Attribute> attributes) {
        return null;
    }

    @Override
    public void updateConnectionGroup(SmartHRSchema schema, Uid uid, Set<AttributeDelta> modifications, OperationOptions options) {

    }

    @Override
    public void deleteConnectionGroup(SmartHRSchema schema, Uid uid, OperationOptions options) {

    }

    @Override
    public void getConnectionGroups(SmartHRSchema schema, SmartHRQueryHandler<SmartHRConnectionGroupRepresentation> handler, OperationOptions options, Set<String> attributesToGet, int queryPageSize) {

    }

    @Override
    public SmartHRConnectionGroupRepresentation getConnectionGroup(SmartHRSchema schema, Uid uid, OperationOptions options, Set<String> attributesToGet) {
        return null;
    }

    @Override
    public SmartHRConnectionGroupRepresentation getConnectionGroup(SmartHRSchema schema, Name name, OperationOptions options, Set<String> attributesToGet) {
        return null;
    }

    @Override
    public void assignConnectionGroupsToUser(Uid userUid, List<String> addConnectionGroups, List<String> removeConnectionGroups) {

    }

    @Override
    public void assignConnectionGroupsToUserGroup(Uid userGroupUid, List<String> addConnectionGroups, List<String> removeConnectionGroups) {

    }
}
