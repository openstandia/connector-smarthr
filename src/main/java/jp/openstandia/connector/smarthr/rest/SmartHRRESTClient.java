package jp.openstandia.connector.smarthr.rest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import jp.openstandia.connector.smarthr.*;
import okhttp3.*;
import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.exceptions.*;
import org.identityconnectors.framework.common.objects.*;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static jp.openstandia.connector.smarthr.SmartHRConnectionGroupHandler.CONNECTION_GROUP_OBJECT_CLASS;
import static jp.openstandia.connector.smarthr.SmartHRConnectionHandler.CONNECTION_OBJECT_CLASS;
import static jp.openstandia.connector.smarthr.SmartHRUserGroupHandler.USER_GROUP_OBJECT_CLASS;
import static jp.openstandia.connector.smarthr.SmartHRUserHandler.USER_OBJECT_CLASS;
import static jp.openstandia.connector.smarthr.SmartHRUtils.toSmartHRAttribute;
import static org.identityconnectors.framework.common.objects.OperationalAttributes.ENABLE_NAME;
import static org.identityconnectors.framework.common.objects.OperationalAttributes.PASSWORD_NAME;

public class SmartHRRESTClient implements SmartHRClient {

    private static final Log LOG = Log.getLog(SmartHRRESTClient.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final String instanceName;
    private final SmartHRConfiguration configuration;
    private final OkHttpClient httpClient;

    private String authToken;

    public SmartHRRESTClient(String instanceName, SmartHRConfiguration configuration, OkHttpClient httpClient) {
        this.instanceName = instanceName;
        this.configuration = configuration;
        this.httpClient = httpClient;
    }

    @Override
    public void test() {
        auth();
    }

    @Override
    public boolean auth() {
        FormBody.Builder formBuilder = new FormBody.Builder()
                .add("username", configuration.getApiUsername());

        configuration.getApiPassword().access((password) -> {
            formBuilder.add("password", String.valueOf(password));
        });

        Request request = new Request.Builder()
                .url(configuration.getSmartHRURL() + "tokens")
                .post(formBuilder.build())
                .build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (response.code() == 403) {
                throw new ConnectionFailedException("Unauthorized");
            }
            if (response.code() != 200) {
                // Something wrong..
                String body = response.body().string();
                throw new ConnectionFailedException(String.format("Unexpected authentication response. statusCode: %s, body: %s",
                        response.code(),
                        body));
            }

            Map<String, String> resJson = MAPPER.readValue(response.body().byteStream(), Map.class);
            this.authToken = resJson.get("authToken");
            String username = resJson.get("username");

            if (this.authToken == null) {
                // Something wrong...
                throw new ConnectionFailedException("Cannot get auth token");
            }

            LOG.info("[{0}] SmartHR connector authenticated by {1}", instanceName, username);

            return true;

        } catch (IOException e) {
            throw new ConnectionFailedException("Cannot connect to the smarthr server", e);
        }
    }

    @Override
    public String getAuthToken() {
        return authToken;
    }

    @Override
    public List<SmartHRSchemaRepresentation> schema() {
        try (Response response = get(getSchemaEndpointURL(configuration))) {
            if (response.code() == 404) {
                // Don't throw
                return null;
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get smarthr schema. statusCode: %d", response.code()));
            }

            // Success
            List<SmartHRSchemaRepresentation> schema = MAPPER.readValue(response.body().byteStream(),
                    new TypeReference<List<SmartHRSchemaRepresentation>>() {
                    });
            return schema;

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call smarthr get schema API", e);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public Uid createUser(SmartHRSchema schema, Set<Attribute> createAttributes) throws AlreadyExistsException {
        SmartHRClient.SmartHRUserRepresentation user = createSmartHRUser(schema, createAttributes);

        try (Response response = post(getUserEndpointURL(configuration), user)) {
            if (response.code() == 400) {
                SmartHRErrorRepresentation error = MAPPER.readValue(response.body().byteStream(), SmartHRErrorRepresentation.class);
                if (error.isAlreadyExists()) {
                    throw new AlreadyExistsException(String.format("User '%s' already exists.", user.username));
                }
                throw new InvalidAttributeValueException(String.format("Bad request when creating a user. username: %s", user.username));
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to create smarthr user: %s, statusCode: %d", user.username, response.code()));
            }

            // Created
            return new Uid(user.username);

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call smarthr create user API", e);
        }
    }

    @Override
    public void updateUser(SmartHRSchema schema, Uid userUid, Set<AttributeDelta> modifications, OperationOptions options) throws UnknownUidException {
        // Need to fetch the target user first to update because we need to send all attributes including unmodified attributes
        SmartHRUserRepresentation target = getUser(schema, userUid, options, null);
        if (target == null) {
            throw new UnknownUidException(userUid, USER_OBJECT_CLASS);
        }

        // Apply delta
        modifications.stream().forEach(delta -> {
            if (delta.getName().equals(ENABLE_NAME)) {
                target.applyEnabled(AttributeDeltaUtil.getBooleanValue(delta));
            } else if (delta.getName().equals(PASSWORD_NAME)) {
                target.applyPassword(AttributeDeltaUtil.getGuardedStringValue(delta));
            } else {
                SmartHRAttribute guacaAttr = toSmartHRAttribute(schema.userSchema, delta);
                target.applyAttribute(guacaAttr);
            }
        });

        callUpdate(USER_OBJECT_CLASS, getUserEndpointURL(configuration, userUid), userUid, target);
    }

    @Override
    public void deleteUser(SmartHRSchema schema, Uid userUid, OperationOptions options) throws UnknownUidException {
        callDelete(USER_OBJECT_CLASS, getUserEndpointURL(configuration, userUid), userUid);
    }

    @Override
    public void getUsers(SmartHRSchema schema, SmartHRQueryHandler<SmartHRUserRepresentation> handler, OperationOptions options, Set<String> attributesToGet, int queryPageSize) {
        try (Response response = get(getUserEndpointURL(configuration))) {
            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get smarthr users. statusCode: %d", response.code()));
            }

            // Success
            Map<String, SmartHRUserRepresentation> users = MAPPER.readValue(response.body().byteStream(),
                    new TypeReference<Map<String, SmartHRUserRepresentation>>() {
                    });
            users.entrySet().stream().forEach(entry -> handler.handle(entry.getValue()));

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call smarthr get users API", e);
        }
    }

    @Override
    public SmartHRUserRepresentation getUser(SmartHRSchema schema, Uid uid, OperationOptions options, Set<String> attributesToGet) {
        try (Response response = get(getUserEndpointURL(configuration, uid))) {
            if (response.code() == 404) {
                // Don't throw
                return null;
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get smarthr user. statusCode: %d", response.code()));
            }

            // Success
            SmartHRUserRepresentation user = MAPPER.readValue(response.body().byteStream(), SmartHRUserRepresentation.class);
            return user;

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call smarthr get user API", e);
        }
    }

    @Override
    public void getUserGroupsForUser(String username, SmartHRQueryHandler<String> handler) {
        try (Response response = get(getUserGroupsEndpointURL(configuration, username))) {
            if (response.code() == 404) {
                // Don't throw
                return;
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get smarthr groups for user: %s, statusCode: %d", username, response.code()));
            }

            // Success
            List<String> groups = MAPPER.readValue(response.body().byteStream(),
                    new TypeReference<List<String>>() {
                    });
            groups.stream().forEach(groupName -> handler.handle(groupName));

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call smarthr get user groups API", e);
        }
    }

    @Override
    public void assignUserGroupsToUser(Uid userUid, List<String> addGroups, List<String> removeGroups) {
        callAssign(USER_OBJECT_CLASS, getUserGroupsEndpointURL(configuration, userUid.getUidValue()), "/",
                userUid, addGroups, removeGroups);
    }

    @Override
    public Uid createUserGroup(SmartHRSchema schema, Set<Attribute> createAttributes) throws AlreadyExistsException {
        SmartHRUserGroupRepresentation group = createSmartHRUserGroup(schema, createAttributes);

        try (Response response = post(getUserGroupEndpointURL(configuration), group)) {
            if (response.code() == 400) {
                SmartHRErrorRepresentation error = MAPPER.readValue(response.body().byteStream(), SmartHRErrorRepresentation.class);
                if (error.isAlreadyExists()) {
                    throw new AlreadyExistsException(String.format("Group '%s' already exists.", group.identifier));
                }
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to create smarthr group: %s, statusCode: %d", group.identifier, response.code()));
            }

            // Created
            return new Uid(group.identifier);

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call smarthr API", e);
        }
    }

    @Override
    public void updateUserGroup(SmartHRSchema schema, Uid groupUid, Set<AttributeDelta> modifications, OperationOptions options) throws UnknownUidException {
        // Need to fetch the target group first to update because we need to send all attributes including unmodified attributes
        SmartHRUserGroupRepresentation target = getUserGroup(schema, groupUid, options, null);
        if (target == null) {
            throw new UnknownUidException(groupUid, USER_GROUP_OBJECT_CLASS);
        }

        // Apply delta
        modifications.stream().forEach(delta -> {
            if (delta.getName().equals(ENABLE_NAME)) {
                target.applyEnabled(AttributeDeltaUtil.getBooleanValue(delta));
            } else {
                SmartHRAttribute guacaAttr = toSmartHRAttribute(schema.userGroupSchema, delta);
                target.applyAttribute(guacaAttr);
            }
        });

        callUpdate(USER_GROUP_OBJECT_CLASS, getUserGroupEndpointURL(configuration, groupUid), groupUid, target);
    }

    @Override
    public void deleteUserGroup(SmartHRSchema schema, Uid groupUid, OperationOptions options) throws UnknownUidException {
        callDelete(USER_OBJECT_CLASS, getUserGroupEndpointURL(configuration, groupUid), groupUid);
    }

    @Override
    public void getUserGroups(SmartHRSchema schema, SmartHRQueryHandler<SmartHRUserGroupRepresentation> handler, OperationOptions options, Set<String> attributesToGet, int queryPageSize) {
        try (Response response = get(getUserGroupEndpointURL(configuration))) {
            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get smarthr groups. statusCode: %d", response.code()));
            }

            // Success
            Map<String, SmartHRUserGroupRepresentation> groups = MAPPER.readValue(response.body().byteStream(),
                    new TypeReference<Map<String, SmartHRUserGroupRepresentation>>() {
                    });
            groups.entrySet().stream().forEach(entry -> handler.handle(entry.getValue()));

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call smarthr get groups API", e);
        }
    }

    @Override
    public SmartHRUserGroupRepresentation getUserGroup(SmartHRSchema schema, Uid uid, OperationOptions options, Set<String> attributesToGet) {
        try {
            Response response = get(getUserGroupEndpointURL(configuration, uid));

            if (response.code() == 404) {
                // Don't throw
                return null;
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get smarthr group. statusCode: %d", response.code()));
            }

            // Success
            SmartHRUserGroupRepresentation group = MAPPER.readValue(response.body().byteStream(), SmartHRUserGroupRepresentation.class);
            return group;

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call smarthr get group API", e);
        }
    }

    @Override
    public void getUsersForUserGroup(String groupName, SmartHRQueryHandler<String> handler) {
        try {
            Response response = get(getUserGroupMembersEndpointURL(configuration, groupName));

            if (response.code() == 404) {
                // Don't throw
                return;
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get smarthr users for group: %s, statusCode: %d",
                        groupName, response.code()));
            }

            // Success
            List<String> users = MAPPER.readValue(response.body().byteStream(),
                    new TypeReference<List<String>>() {
                    });
            users.stream().forEach(username -> handler.handle(username));

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call smarthr group members API", e);
        }
    }

    @Override
    public void assignUsersToUserGroup(Uid groupUid, List<String> addUsers, List<String> removeUsers) {
        callAssign(USER_GROUP_OBJECT_CLASS, getUserGroupMembersEndpointURL(configuration, groupUid.getUidValue()), "/",
                groupUid, addUsers, removeUsers);
    }

    @Override
    public void getUserGroupsForUserGroup(String groupName, SmartHRQueryHandler<String> handler) {
        try {
            Response response = get(getUserGroupGroupsEndpointURL(configuration, groupName));

            if (response.code() == 404) {
                // Don't throw
                return;
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get smarthr groups for group: %s, statusCode: %d",
                        groupName, response.code()));
            }

            // Success
            List<String> groups = MAPPER.readValue(response.body().byteStream(),
                    new TypeReference<List<String>>() {
                    });
            groups.stream().forEach(g -> handler.handle(g));

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call smarthr group members API", e);
        }
    }

    @Override
    public void assignUserGroupsToUserGroup(Uid groupUid, List<String> addGroups, List<String> removeGroups) {
        callAssign(USER_GROUP_OBJECT_CLASS, getUserGroupGroupsEndpointURL(configuration, groupUid.getUidValue()), "/",
                groupUid, addGroups, removeGroups);
    }

    // Permission

    @Override
    public void assignUserPermissionsToUser(Uid userUid, List<String> addPermissions, List<String> removePermissions) {
        callAssign(USER_OBJECT_CLASS, getUserPermissionEndpointURL(configuration, userUid.getUidValue()), "/userPermissions/" + userUid.getUidValue(),
                userUid, addPermissions, removePermissions);
    }

    @Override
    public void assignSystemPermissionsToUser(Uid userUid, List<String> addPermissions, List<String> removePermissions) {
        callAssign(USER_OBJECT_CLASS, getUserPermissionEndpointURL(configuration, userUid.getUidValue()), "/systemPermissions",
                userUid, addPermissions, removePermissions);
    }

    @Override
    public void assignSystemPermissionsToUserGroup(Uid groupUid, List<String> addPermissions, List<String> removePermissions) {
        callAssign(USER_GROUP_OBJECT_CLASS, getUserGroupPermissionEndpointURL(configuration, groupUid.getUidValue()), "/systemPermissions",
                groupUid, addPermissions, removePermissions);
    }

    @Override
    public SmartHRPermissionRepresentation getPermissionsForUser(String username) {
        try {
            Response response = get(getUserPermissionEndpointURL(configuration, username));

            if (response.code() == 404) {
                throw new UnknownUidException(new Uid(username), USER_OBJECT_CLASS);
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get smarthr permissions for user: %s, statusCode: %d",
                        username, response.code()));
            }

            // Success
            SmartHRPermissionRepresentation permission = MAPPER.readValue(response.body().byteStream(), SmartHRPermissionRepresentation.class);
            return permission;

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Failed to get smarthr permissions for user: %s", username));
        }
    }

    @Override
    public SmartHRPermissionRepresentation getPermissionsForUserGroup(String groupName) {
        try {
            Response response = get(getUserGroupPermissionEndpointURL(configuration, groupName));

            if (response.code() == 404) {
                throw new UnknownUidException(new Uid(groupName), USER_GROUP_OBJECT_CLASS);
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get smarthr permissions for user: %s, statusCode: %d",
                        groupName, response.code()));
            }

            // Success
            SmartHRPermissionRepresentation permission = MAPPER.readValue(response.body().byteStream(), SmartHRPermissionRepresentation.class);
            return permission;

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Failed to get smarthr permissions for user: %s", groupName));
        }
    }

    // Connection

    @Override
    public Uid createConnection(SmartHRSchema schema, Set<Attribute> attributes) {
        SmartHRClient.SmartHRConnectionRepresentation conn = createSmartHRConnection(schema, attributes);

        try (Response response = post(getConnectionEndpointURL(configuration), conn)) {
            if (response.code() == 400) {
                SmartHRErrorRepresentation error = MAPPER.readValue(response.body().byteStream(), SmartHRErrorRepresentation.class);
                if (error.isAlreadyExists()) {
                    throw new AlreadyExistsException(String.format("Connection '%s' already exists.", conn.name));
                }
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to create smarthr connection: %s, statusCode: %d",
                        conn.name, response.code()));
            }

            SmartHRConnectionRepresentation created = MAPPER.readValue(response.body().byteStream(), SmartHRConnectionRepresentation.class);

            // Created
            return new Uid(created.identifier, new Name(created.parentIdentifier + "/" + created.name));

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Failed to create smarthr connection: %s", conn.name));
        }
    }

    @Override
    public void updateConnection(SmartHRSchema schema, Uid connectionUid, Set<AttributeDelta> modifications, OperationOptions options) {
        // Need to fetch the target connection first to update because we need to send all attributes including unmodified attributes
        SmartHRConnectionRepresentation target = getConnection(schema, connectionUid, options, null);
        if (target == null) {
            throw new UnknownUidException(connectionUid, CONNECTION_OBJECT_CLASS);
        }

        Map<String, String> parameters = getConnectionParameters(schema, connectionUid);
        target.parameters = parameters;

        // Apply delta
        modifications.stream().forEach(delta -> target.applyDelta(schema, delta));

        callUpdate(CONNECTION_OBJECT_CLASS, getConnectionEndpointURL(configuration, connectionUid), connectionUid, target);
    }

    @Override
    public void deleteConnection(SmartHRSchema schema, Uid connectionUid, OperationOptions options) {
        callDelete(CONNECTION_OBJECT_CLASS, getConnectionEndpointURL(configuration, connectionUid), connectionUid);
    }

    @Override
    public void getConnections(SmartHRSchema schema, SmartHRQueryHandler<SmartHRConnectionRepresentation> handler, OperationOptions options, Set<String> attributesToGet, int queryPageSize) {
        try (Response response = get(getConnectionEndpointURL(configuration))) {
            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get smarthr connections. statusCode: %d", response.code()));
            }

            // Success
            Map<String, SmartHRConnectionRepresentation> connections = MAPPER.readValue(response.body().byteStream(),
                    new TypeReference<Map<String, SmartHRConnectionRepresentation>>() {
                    });
            for (Map.Entry<String, SmartHRConnectionRepresentation> entry : connections.entrySet()) {
                if (!handler.handle(entry.getValue())) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call smarthr get connections API", e);
        }
    }

    private Map<String, String> getConnectionParameters(SmartHRSchema schema, Uid uid) {
        try {
            Response response = get(getParametersEndpointURL(configuration, uid.getUidValue()));

            if (response.code() == 404) {
                // Don't throw
                return Collections.emptyMap();
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get smarthr connection parameters: %s, statusCode: %d",
                        uid.getUidValue(), response.code()));
            }

            // Success
            Map<String, String> conn = MAPPER.readValue(response.body().byteStream(), Map.class);
            return conn;

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call smarthr get connection parameters API", e);
        }
    }

    @Override
    public SmartHRConnectionRepresentation getConnection(SmartHRSchema schema, Uid uid, OperationOptions options, Set<String> attributesToGet) {
        try {
            Response response = get(getConnectionEndpointURL(configuration, uid));

            if (response.code() == 404) {
                // Don't throw
                return null;
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get smarthr connection: %s, statusCode: %d",
                        uid.getUidValue(), response.code()));
            }

            // Success
            SmartHRConnectionRepresentation conn = MAPPER.readValue(response.body().byteStream(), SmartHRConnectionRepresentation.class);
            return conn;

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call smarthr get connection API", e);
        }
    }

    @Override
    public SmartHRConnectionRepresentation getConnection(SmartHRSchema schema, Name name, OperationOptions options, Set<String> attributesToGet) {
        LOG.info("getConnection by Name start (it might take a while...)");

        AtomicReference<SmartHRConnectionRepresentation> store = new AtomicReference<>();
        // Need to fetch all connections
        // It might cause performance issue when there are a lot of connections
        getConnections(schema, conn -> {
            if (conn.toUniqueName().equals(name.getNameValue())) {
                store.set(conn);
                return false;
            }
            return true;
        }, options, attributesToGet, -1);

        LOG.info("getConnection by Name end");

        return store.get();
    }

    @Override
    public Map<String, String> getParameters(String identifier) {
        try (Response response = get(getParametersEndpointURL(configuration, identifier))) {
            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get smarthr connection parameters. connection: %s, statusCode: %d",
                        identifier, response.code()));
            }

            // Success
            Map<String, String> conns = MAPPER.readValue(response.body().byteStream(),
                    new TypeReference<Map<String, String>>() {
                    });
            return conns;

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call smarthr get connection parameters API", e);
        }
    }

    @Override
    public void assignConnectionsToUser(Uid userUid, List<String> addConnections, List<String> removeConnections) {
        callAssignConnections(USER_OBJECT_CLASS, getUserPermissionEndpointURL(configuration, userUid.getUidValue()),
                "/connectionPermissions/", userUid, addConnections, removeConnections);
    }

    @Override
    public void assignConnectionsToUserGroup(Uid userGroupUid, List<String> addConnections, List<String> removeConnections) {
        callAssignConnections(USER_GROUP_OBJECT_CLASS, getUserGroupPermissionEndpointURL(configuration, userGroupUid.getUidValue()),
                "/connectionPermissions/", userGroupUid, addConnections, removeConnections);
    }

    // ConnectionGroup

    @Override
    public Uid createConnectionGroup(SmartHRSchema schema, Set<Attribute> attributes) {
        SmartHRClient.SmartHRConnectionGroupRepresentation connGroup = createSmartHRConnectionGroup(schema, attributes);

        try (Response response = post(getConnectionGroupEndpointURL(configuration), connGroup)) {
            if (response.code() == 400) {
                SmartHRErrorRepresentation error = MAPPER.readValue(response.body().byteStream(), SmartHRErrorRepresentation.class);
                if (error.isAlreadyExists()) {
                    throw new AlreadyExistsException(String.format("ConnectionGroup '%s' already exists.", connGroup.name));
                }
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to create smarthr connectionGroup: %s, statusCode: %d",
                        connGroup.name, response.code()));
            }

            SmartHRConnectionRepresentation created = MAPPER.readValue(response.body().byteStream(), SmartHRConnectionRepresentation.class);

            // Created
            return new Uid(created.identifier, new Name(created.parentIdentifier + "/" + created.name));

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Failed to create smarthr connectionGroup: %s", connGroup.name));
        }
    }

    @Override
    public void updateConnectionGroup(SmartHRSchema schema, Uid connectionGroupUid, Set<AttributeDelta> modifications, OperationOptions options) {
        // Need to fetch the target connectionGroup first to update because we need to send all attributes including unmodified attributes
        SmartHRConnectionGroupRepresentation target = getConnectionGroup(schema, connectionGroupUid, options, null);
        if (target == null) {
            throw new UnknownUidException(connectionGroupUid, CONNECTION_GROUP_OBJECT_CLASS);
        }

        // Apply delta
        modifications.stream().forEach(delta -> target.applyDelta(schema, delta));

        callUpdate(CONNECTION_GROUP_OBJECT_CLASS, getConnectionGroupEndpointURL(configuration, connectionGroupUid), connectionGroupUid, target);
    }

    @Override
    public void deleteConnectionGroup(SmartHRSchema schema, Uid connectionGroupUid, OperationOptions options) {
        callDelete(CONNECTION_GROUP_OBJECT_CLASS, getConnectionGroupEndpointURL(configuration, connectionGroupUid), connectionGroupUid);
    }

    @Override
    public void getConnectionGroups(SmartHRSchema schema, SmartHRQueryHandler<SmartHRConnectionGroupRepresentation> handler,
                                    OperationOptions options, Set<String> attributesToGet, int queryPageSize) {
        try (Response response = get(getConnectionGroupEndpointURL(configuration))) {
            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get smarthr connectionGroups. statusCode: %d", response.code()));
            }

            // Success
            Map<String, SmartHRConnectionGroupRepresentation> connections = MAPPER.readValue(response.body().byteStream(),
                    new TypeReference<Map<String, SmartHRConnectionGroupRepresentation>>() {
                    });
            for (Map.Entry<String, SmartHRConnectionGroupRepresentation> entry : connections.entrySet()) {
                if (!handler.handle(entry.getValue())) {
                    break;
                }
            }
        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call smarthr get connectionGroups API", e);
        }
    }

    @Override
    public SmartHRConnectionGroupRepresentation getConnectionGroup(SmartHRSchema schema, Uid connectionGroupUid,
                                                                     OperationOptions options, Set<String> attributesToGet) {
        try {
            Response response = get(getConnectionGroupEndpointURL(configuration, connectionGroupUid));

            if (response.code() == 404) {
                // Don't throw
                return null;
            }

            if (response.code() != 200) {
                throw new ConnectorIOException(String.format("Failed to get smarthr connectionGroup: %s, statusCode: %d",
                        connectionGroupUid.getUidValue(), response.code()));
            }

            // Success
            SmartHRConnectionGroupRepresentation conn = MAPPER.readValue(response.body().byteStream(), SmartHRConnectionGroupRepresentation.class);
            return conn;

        } catch (IOException e) {
            throw new ConnectorIOException("Failed to call smarthr get connectionGroup API", e);
        }
    }

    @Override
    public SmartHRConnectionGroupRepresentation getConnectionGroup(SmartHRSchema schema, Name connectionGroupName,
                                                                     OperationOptions options, Set<String> attributesToGet) {
        LOG.info("getConnectionGroup by Name start (it might take a while...)");

        AtomicReference<SmartHRConnectionGroupRepresentation> store = new AtomicReference<>();
        // Need to fetch all connectionGroups
        // It might cause performance issue when there are a lot of connectionGroups
        getConnectionGroups(schema, connGroup -> {
            if (connGroup.toUniqueName().equals(connectionGroupName.getNameValue())) {
                store.set(connGroup);
                return false;
            }
            return true;
        }, options, attributesToGet, -1);

        LOG.info("getConnectionGroup by Name end");

        return store.get();
    }

    @Override
    public void assignConnectionGroupsToUser(Uid userUid, List<String> addConnectionGroups, List<String> removeConnectionGroups) {
        callAssignConnections(USER_OBJECT_CLASS, getUserPermissionEndpointURL(configuration, userUid.getUidValue()),
                "/connectionGroupPermissions/", userUid, addConnectionGroups, removeConnectionGroups);
    }

    @Override
    public void assignConnectionGroupsToUserGroup(Uid userGroupUid, List<String> addConnectionGroups, List<String> removeConnectionGroups) {
        callAssignConnections(USER_GROUP_OBJECT_CLASS, getUserGroupPermissionEndpointURL(configuration, userGroupUid.getUidValue()),
                "/connectionGroupPermissions/", userGroupUid, addConnectionGroups, removeConnectionGroups);
    }

    // Utilities

    protected void callUpdate(ObjectClass objectClass, String url, Uid uid, Object target) {
        try (Response response = put(url, target)) {
            if (response.code() == 400) {
                throw new InvalidAttributeValueException(String.format("Bad request when updating %s: %s, response: %s",
                        objectClass.getObjectClassValue(), uid.getUidValue(), toBody(response)));
            }

            if (response.code() == 404) {
                throw new UnknownUidException(uid, objectClass);
            }

            if (response.code() != 204) {
                throw new ConnectorIOException(String.format("Failed to update smarthr %s: %s, statusCode: %d, response: %s",
                        objectClass.getObjectClassValue(), uid.getUidValue(), response.code(), toBody(response)));
            }

            // Success

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Failed to update smarthr %s: %s",
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

    /**
     * Generic assign method.
     *
     * @param objectClass
     * @param url
     * @param path
     * @param uid
     * @param add
     * @param remove
     */
    protected void callAssign(ObjectClass objectClass, String url, String path, Uid uid, List<String> add, List<String> remove) {
        Stream<PatchOperation> addOp = add.stream().map(value -> new PatchOperation("add", path, value));
        Stream<PatchOperation> removeOp = remove.stream().map(value -> new PatchOperation("remove", path, value));
        List<PatchOperation> operations = Stream.concat(addOp, removeOp).collect(Collectors.toList());

        if (operations.isEmpty()) {
            LOG.info("No assign {0} {1} to {2} operations", objectClass.getObjectClassValue(), path, uid.getUidValue());
            return;
        }

        try (Response response = patch(url, operations)) {
            if (response.code() == 404) {
                // Missing the group
                throw new UnknownUidException(uid, objectClass);
            }

            if (response.code() != 204) {
                throw new ConnectorIOException(String.format("Failed to assign %s %s to %s, add: %s, remove: %s, statusCode: %d, response: %s",
                        objectClass.getObjectClassValue(), path, uid.getUidValue(), add, remove, response.code(), toBody(response)));
            }

            // Success

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Failed to assign %s %s to %s, add: %s, remove: %s",
                    objectClass.getObjectClassValue(), path, uid.getUidValue(), add, remove), e);
        }
    }

    protected void callAssignConnections(ObjectClass objectClass, String url, String path, Uid uid, List<String> add, List<String> remove) {
        Stream<PatchOperation> addOp = add.stream().map(value -> new PatchOperation("add", path + value, "READ"));
        Stream<PatchOperation> removeOp = remove.stream().map(value -> new PatchOperation("remove", path + value, "READ"));
        List<PatchOperation> operations = Stream.concat(addOp, removeOp).collect(Collectors.toList());

        if (operations.isEmpty()) {
            LOG.info("No assign {0} {1} to {2} operations", objectClass.getObjectClassValue(), path, uid.getUidValue());
            return;
        }

        try (Response response = patch(url, operations)) {
            if (response.code() == 404) {
                // Missing the group
                throw new UnknownUidException(uid, objectClass);
            }

            if (response.code() != 204) {
                throw new ConnectorIOException(String.format("Failed to assign %s %s to %s, add: %s, remove: %s, statusCode: %d, response: %s",
                        objectClass.getObjectClassValue(), path, uid.getUidValue(), add, remove, response.code(), toBody(response)));
            }

            // Success

        } catch (IOException e) {
            throw new ConnectorIOException(String.format("Failed to assign %s %s to %s, add: %s, remove: %s",
                    objectClass.getObjectClassValue(), path, uid.getUidValue(), add, remove), e);
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
        for (int i = 0; i < 2; i++) {
            final Request request = new Request.Builder()
                    .url(url + "?token=" + getAuthToken())
                    .get()
                    .build();

            final Response response = httpClient.newCall(request).execute();

            if (response.code() == 403) {
                SmartHRErrorRepresentation error = MAPPER.readValue(response.body().byteStream(), SmartHRErrorRepresentation.class);
                if (error.isUnauthorized()) {
                    // re-auth
                    auth();
                    continue;
                }
            }

            throwExceptionIfServerError(response);

            return response;
        }

        throw new ConnectorIOException("Failed to call get API");
    }

    private Response post(String url, Object body) throws IOException {
        RequestBody requestBody = createJsonRequestBody(body);

        for (int i = 0; i < 2; i++) {
            final Request request = new Request.Builder()
                    .url(url + "?token=" + getAuthToken())
                    .post(requestBody)
                    .build();

            final Response response = httpClient.newCall(request).execute();

            if (response.code() == 403) {
                SmartHRErrorRepresentation error = MAPPER.readValue(response.body().byteStream(), SmartHRErrorRepresentation.class);
                if (error.isUnauthorized()) {
                    // re-auth
                    auth();
                    continue;
                }
            }

            throwExceptionIfServerError(response);

            return response;
        }

        throw new ConnectorIOException("Failed to call post API");
    }

    private Response put(String url, Object body) throws IOException {
        RequestBody requestBody = createJsonRequestBody(body);

        for (int i = 0; i < 2; i++) {
            final Request request = new Request.Builder()
                    .url(url + "?token=" + getAuthToken())
                    .put(requestBody)
                    .build();

            final Response response = httpClient.newCall(request).execute();

            if (response.code() == 403) {
                SmartHRErrorRepresentation error = MAPPER.readValue(response.body().byteStream(), SmartHRErrorRepresentation.class);
                if (error.isUnauthorized()) {
                    // re-auth
                    auth();
                    continue;
                }
            }

            throwExceptionIfServerError(response);

            return response;
        }

        throw new ConnectorIOException("Failed to call post API");
    }

    private Response patch(String url, Object body) throws IOException {
        RequestBody requestBody = createJsonRequestBody(body);

        for (int i = 0; i < 2; i++) {
            final Request request = new Request.Builder()
                    .url(url + "?token=" + getAuthToken())
                    .patch(requestBody)
                    .build();

            final Response response = httpClient.newCall(request).execute();

            if (response.code() == 403) {
                SmartHRErrorRepresentation error = MAPPER.readValue(response.body().byteStream(), SmartHRErrorRepresentation.class);
                if (error.isUnauthorized()) {
                    // re-auth
                    auth();
                    continue;
                }
            }

            throwExceptionIfServerError(response);

            return response;
        }

        throw new ConnectorIOException("Failed to call patch API");
    }

    private Response delete(String url) throws IOException {
        for (int i = 0; i < 2; i++) {
            final Request request = new Request.Builder()
                    .url(url + "?token=" + getAuthToken())
                    .delete()
                    .build();

            final Response response = httpClient.newCall(request).execute();

            if (response.code() == 403) {
                SmartHRErrorRepresentation error = MAPPER.readValue(response.body().byteStream(), SmartHRErrorRepresentation.class);
                if (error.isUnauthorized()) {
                    // re-auth
                    auth();
                    continue;
                }
            }

            throwExceptionIfServerError(response);

            return response;
        }

        throw new ConnectorIOException("Failed to call delete API");
    }
}
