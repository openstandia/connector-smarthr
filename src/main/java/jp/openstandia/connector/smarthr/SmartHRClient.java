package jp.openstandia.connector.smarthr;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.identityconnectors.common.security.GuardedString;
import org.identityconnectors.framework.common.exceptions.AlreadyExistsException;
import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.exceptions.UnknownUidException;
import org.identityconnectors.framework.common.objects.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static jp.openstandia.connector.smarthr.SmartHRConnectionHandler.ATTR_PARAMETERS;
import static jp.openstandia.connector.smarthr.SmartHRConnectionHandler.ATTR_PROTOCOL;
import static jp.openstandia.connector.smarthr.SmartHRUserGroupHandler.USER_GROUP_OBJECT_CLASS;
import static jp.openstandia.connector.smarthr.SmartHRUserHandler.ATTR_DISABLED;
import static jp.openstandia.connector.smarthr.SmartHRUserHandler.USER_OBJECT_CLASS;
import static jp.openstandia.connector.smarthr.SmartHRUtils.toSmartHRAttribute;
import static org.identityconnectors.framework.common.objects.OperationalAttributes.ENABLE_NAME;


public interface SmartHRClient {
    void test();

    boolean auth();

    String getAuthToken();

    List<SmartHRSchemaRepresentation> schema();


    default String getSchemaEndpointURL(SmartHRConfiguration configuration) {
        String url = configuration.getSmartHRURL();
        return String.format("%ssession/data/%s/schema/userAttributes", url, configuration.getSmartHRDataSource());
    }

    default String getUserEndpointURL(SmartHRConfiguration configuration) {
        String url = configuration.getSmartHRURL();
        return String.format("%ssession/data/%s/users", url, configuration.getSmartHRDataSource());
    }

    default String getUserEndpointURL(SmartHRConfiguration configuration, Uid userUid) {
        String url = configuration.getSmartHRURL();
        return String.format("%ssession/data/%s/users/%s", url, configuration.getSmartHRDataSource(), userUid.getUidValue());
    }

    default String getUserGroupsEndpointURL(SmartHRConfiguration configuration, String username) {
        String url = configuration.getSmartHRURL();
        return String.format("%ssession/data/%s/users/%s/userGroups", url, configuration.getSmartHRDataSource(), username);
    }

    default String getUserGroupEndpointURL(SmartHRConfiguration configuration) {
        String url = configuration.getSmartHRURL();
        return String.format("%ssession/data/%s/userGroups", url, configuration.getSmartHRDataSource());
    }

    default String getUserGroupEndpointURL(SmartHRConfiguration configuration, Uid groupUid) {
        String url = configuration.getSmartHRURL();
        return String.format("%ssession/data/%s/userGroups/%s", url, configuration.getSmartHRDataSource(), groupUid.getUidValue());
    }

    default String getUserGroupMembersEndpointURL(SmartHRConfiguration configuration, String groupName) {
        String url = configuration.getSmartHRURL();
        return String.format("%ssession/data/%s/userGroups/%s/memberUsers", url, configuration.getSmartHRDataSource(), groupName);
    }

    default String getUserGroupGroupsEndpointURL(SmartHRConfiguration configuration, String groupName) {
        String url = configuration.getSmartHRURL();
        return String.format("%ssession/data/%s/userGroups/%s/userGroups", url, configuration.getSmartHRDataSource(), groupName);
    }

    default String getUserPermissionEndpointURL(SmartHRConfiguration configuration, String username) {
        String url = configuration.getSmartHRURL();
        return String.format("%ssession/data/%s/users/%s/permissions", url, configuration.getSmartHRDataSource(), username);
    }

    default String getUserGroupPermissionEndpointURL(SmartHRConfiguration configuration, String groupName) {
        String url = configuration.getSmartHRURL();
        return String.format("%ssession/data/%s/userGroups/%s/permissions", url, configuration.getSmartHRDataSource(), groupName);
    }

    default String getConnectionEndpointURL(SmartHRConfiguration configuration) {
        String url = configuration.getSmartHRURL();
        return String.format("%ssession/data/%s/connections", url, configuration.getSmartHRDataSource());
    }

    default String getConnectionEndpointURL(SmartHRConfiguration configuration, Uid connectionUid) {
        String url = configuration.getSmartHRURL();
        return String.format("%ssession/data/%s/connections/%s", url, configuration.getSmartHRDataSource(), connectionUid.getUidValue());
    }

    default String getParametersEndpointURL(SmartHRConfiguration configuration, String identifier) {
        String url = configuration.getSmartHRURL();
        return String.format("%ssession/data/%s/connections/%s/parameters", url, configuration.getSmartHRDataSource(), identifier);
    }

    default String getConnectionGroupEndpointURL(SmartHRConfiguration configuration) {
        String url = configuration.getSmartHRURL();
        return String.format("%ssession/data/%s/connectionGroups", url, configuration.getSmartHRDataSource());
    }

    default String getConnectionGroupEndpointURL(SmartHRConfiguration configuration, Uid connectionGroupUid) {
        String url = configuration.getSmartHRURL();
        return String.format("%ssession/data/%s/connectionGroups/%s", url, configuration.getSmartHRDataSource(), connectionGroupUid.getUidValue());
    }


    default SmartHRUserRepresentation createSmartHRUser(SmartHRSchema schema, Set<Attribute> attributes) {
        SmartHRUserRepresentation user = new SmartHRUserRepresentation();

        for (Attribute attr : attributes) {
            // Need to get the value from __NAME__ (not __UID__)
            if (attr.getName().equals(Name.NAME)) {
                user.applyUsername(attr);

            } else if (attr.getName().equals(ENABLE_NAME)) {
                user.applyEnabled(AttributeUtil.getBooleanValue(attr));

            } else if (attr.getName().equals(OperationalAttributes.PASSWORD_NAME)) {
                user.applyPassword(AttributeUtil.getGuardedStringValue(attr));

            } else {
                if (!schema.isUserSchema(attr)) {
                    throw new InvalidAttributeValueException(String.format("SmartHR doesn't support to set '%s' attribute of %s",
                            attr.getName(), USER_OBJECT_CLASS.getObjectClassValue()));
                }
                SmartHRAttribute guacaAttr = toSmartHRAttribute(schema.userSchema, attr);
                user.applyAttribute(guacaAttr);
            }
        }

        // Generate username if IDM doesn't have mapping to username
        if (user.username == null) {
            user.username = UUID.randomUUID().toString();
        }

        return user;
    }

    default SmartHRUserGroupRepresentation createSmartHRUserGroup(SmartHRSchema schema, Set<Attribute> attributes) {
        SmartHRUserGroupRepresentation group = new SmartHRUserGroupRepresentation();

        for (Attribute attr : attributes) {
            // Need to get the value from __NAME__ (not __UID__)
            if (attr.getName().equals(Name.NAME)) {
                group.applyIdentifier(attr);

            } else if (attr.getName().equals(ENABLE_NAME)) {
                group.applyEnabled(AttributeUtil.getBooleanValue(attr));

            } else {
                if (!schema.isUserGroupSchema(attr)) {
                    throw new InvalidAttributeValueException(String.format("SmartHR doesn't support to set '%s' attribute of %s",
                            attr.getName(), USER_GROUP_OBJECT_CLASS.getObjectClassValue()));
                }
                SmartHRAttribute guacaAttr = toSmartHRAttribute(schema.userGroupSchema, attr);
                group.applyAttribute(guacaAttr);
            }
        }

        // Generate group identifier if IDM doesn't have mapping to it
        if (group.identifier == null) {
            group.identifier = UUID.randomUUID().toString();
        }

        return group;
    }

    default SmartHRConnectionRepresentation createSmartHRConnection(SmartHRSchema schema, Set<Attribute> attributes) {
        SmartHRConnectionRepresentation conn = new SmartHRConnectionRepresentation();

        attributes.stream().forEach(attr -> conn.apply(schema, attr));

        // Set default value for required attributes if IDM doesn't pass them
        if (conn.protocol == null) {
            conn.protocol = "vnc";
        }

        return conn;
    }

    default SmartHRConnectionGroupRepresentation createSmartHRConnectionGroup(SmartHRSchema schema, Set<Attribute> attributes) {
        SmartHRConnectionGroupRepresentation conn = new SmartHRConnectionGroupRepresentation();

        attributes.stream().forEach(attr -> conn.apply(schema, attr));

        // Set default value for required attributes if IDM doesn't pass them
        if (conn.type == null) {
            conn.type = "ORGANIZATIONAL";
        }

        return conn;
    }

    void close();

    // User

    /**
     * @param schema
     * @param createAttributes
     * @return Username of the created user. Caution! Don't include Name object in the Uid because it throws
     * SchemaException with "No definition for ConnId NAME attribute found in definition crOCD ({http://midpoint.evolveum.com/xml/ns/public/resource/instance-3}User)
     * @throws AlreadyExistsException
     */
    Uid createUser(SmartHRSchema schema, Set<Attribute> createAttributes) throws AlreadyExistsException;

    void updateUser(SmartHRSchema schema, Uid uid, Set<AttributeDelta> modifications, OperationOptions options) throws UnknownUidException;

    void deleteUser(SmartHRSchema schema, Uid uid, OperationOptions options) throws UnknownUidException;

    void getUsers(SmartHRSchema schema, SmartHRQueryHandler<SmartHRUserRepresentation> handler, OperationOptions options, Set<String> attributesToGet, int queryPageSize);

    SmartHRUserRepresentation getUser(SmartHRSchema schema, Uid uid, OperationOptions options, Set<String> attributesToGet);

    void getUserGroupsForUser(String username, SmartHRQueryHandler<String> handler);

    void assignUserGroupsToUser(Uid username, List<String> addGroups, List<String> removeGroups);

    // Group

    /**
     * @param schema
     * @param createAttributes
     * @return Identifier of the created group. Caution! Don't include Name object in the Uid because it throws
     * SchemaException with "No definition for ConnId NAME attribute found in definition crOCD ({http://midpoint.evolveum.com/xml/ns/public/resource/instance-3}UserGroup)
     * @throws AlreadyExistsException
     */
    Uid createUserGroup(SmartHRSchema schema, Set<Attribute> createAttributes) throws AlreadyExistsException;

    void updateUserGroup(SmartHRSchema schema, Uid uid, Set<AttributeDelta> modifications, OperationOptions options) throws UnknownUidException;

    void deleteUserGroup(SmartHRSchema schema, Uid uid, OperationOptions options) throws UnknownUidException;

    void getUserGroups(SmartHRSchema schema, SmartHRQueryHandler<SmartHRUserGroupRepresentation> handler, OperationOptions options, Set<String> attributesToGet, int queryPageSize);

    SmartHRUserGroupRepresentation getUserGroup(SmartHRSchema schema, Uid uid, OperationOptions options, Set<String> attributesToGet);

    void getUsersForUserGroup(String groupName, SmartHRQueryHandler<String> handler);

    void assignUsersToUserGroup(Uid groupName, List<String> addUsers, List<String> removeUsers);

    void getUserGroupsForUserGroup(String groupName, SmartHRQueryHandler<String> handler);

    void assignUserGroupsToUserGroup(Uid groupName, List<String> addGroups, List<String> removeGroups);

    // Permission

    void assignUserPermissionsToUser(Uid userUid, List<String> addPermissions, List<String> removePermissions);

    void assignSystemPermissionsToUser(Uid userUid, List<String> addPermissions, List<String> removePermissions);

    void assignSystemPermissionsToUserGroup(Uid groupUid, List<String> addPermissions, List<String> removePermissions);

    SmartHRPermissionRepresentation getPermissionsForUser(String username);

    SmartHRPermissionRepresentation getPermissionsForUserGroup(String groupName);

    // Connection

    Uid createConnection(SmartHRSchema schema, Set<Attribute> attributes);

    void updateConnection(SmartHRSchema schema, Uid uid, Set<AttributeDelta> modifications, OperationOptions options);

    void deleteConnection(SmartHRSchema schema, Uid uid, OperationOptions options);

    void getConnections(SmartHRSchema schema, SmartHRQueryHandler<SmartHRConnectionRepresentation> handler, OperationOptions options, Set<String> attributesToGet, int queryPageSize);

    SmartHRConnectionRepresentation getConnection(SmartHRSchema schema, Uid uid, OperationOptions options, Set<String> attributesToGet);

    SmartHRConnectionRepresentation getConnection(SmartHRSchema schema, Name name, OperationOptions options, Set<String> attributesToGet);

    Map<String, String> getParameters(String identifier);

    void assignConnectionsToUser(Uid userUid, List<String> addConnections, List<String> removeConnections);

    void assignConnectionsToUserGroup(Uid userGroupUid, List<String> addConnections, List<String> removeConnections);

    // ConnectionGroup

    Uid createConnectionGroup(SmartHRSchema schema, Set<Attribute> attributes);

    void updateConnectionGroup(SmartHRSchema schema, Uid uid, Set<AttributeDelta> modifications, OperationOptions options);

    void deleteConnectionGroup(SmartHRSchema schema, Uid uid, OperationOptions options);

    void getConnectionGroups(SmartHRSchema schema, SmartHRQueryHandler<SmartHRConnectionGroupRepresentation> handler, OperationOptions options, Set<String> attributesToGet, int queryPageSize);

    SmartHRConnectionGroupRepresentation getConnectionGroup(SmartHRSchema schema, Uid uid, OperationOptions options, Set<String> attributesToGet);

    SmartHRConnectionGroupRepresentation getConnectionGroup(SmartHRSchema schema, Name name, OperationOptions options, Set<String> attributesToGet);

    void assignConnectionGroupsToUser(Uid userUid, List<String> addConnectionGroups, List<String> removeConnectionGroups);

    void assignConnectionGroupsToUserGroup(Uid userGroupUid, List<String> addConnectionGroups, List<String> removeConnectionGroups);


    // JSON Representation

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    class SmartHRUserRepresentation {
        public String username;
        public GuardedString password;
        public Map<String, String> attributes = new HashMap<>();

        public void applyUsername(Attribute attr) {
            this.username = AttributeUtil.getAsStringValue(attr);
        }

        public void applyEnabled(Boolean enable) {
            if (Boolean.FALSE.equals(enable)) {
                attributes.put(ATTR_DISABLED, "true");
            } else {
                attributes.put(ATTR_DISABLED, "");
            }
        }

        public void applyPassword(GuardedString password) {
            this.password = password;
        }

        public String getPassword() {
            if (password == null) {
                return null;
            }
            AtomicReference<String> rawPassword = new AtomicReference<>();
            this.password.access((c) -> rawPassword.set(String.valueOf(c)));
            return rawPassword.get();
        }

        public void applyAttribute(SmartHRAttribute attr) {
            this.attributes.put(attr.name, attr.value);
        }

        @JsonIgnore
        public boolean isEnabled() {
            String disabled = attributes.get(ATTR_DISABLED);
            return !"true".equals(disabled);
        }

        public List<SmartHRAttribute> toSmartHRAttributes() {
            return attributes.entrySet().stream().map(a -> new SmartHRAttribute(a.getKey(), a.getValue())).collect(Collectors.toList());
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    class SmartHRSchemaRepresentation {
        public String name;
        public List<SmartHRSchemaFieldRepresentation> fields;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    class SmartHRSchemaFieldRepresentation {
        public String name;
        public String type;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    class SmartHRUserGroupRepresentation {
        public String identifier;
        public Map<String, String> attributes = new HashMap<>();

        public void applyIdentifier(Attribute attr) {
            this.identifier = AttributeUtil.getAsStringValue(attr);
        }

        public void applyEnabled(Boolean enable) {
            if (Boolean.FALSE.equals(enable)) {
                attributes.put(ATTR_DISABLED, "true");
            } else {
                attributes.put(ATTR_DISABLED, "");
            }
        }

        public void applyAttribute(SmartHRAttribute attr) {
            this.attributes.put(attr.name, attr.value);
        }

        @JsonIgnore
        public boolean isEnabled() {
            String disabled = attributes.get(ATTR_DISABLED);
            return !"true".equals(disabled);
        }

        public List<SmartHRAttribute> toSmartHRAttributes() {
            return attributes.entrySet().stream().map(a -> new SmartHRAttribute(a.getKey(), a.getValue())).collect(Collectors.toList());
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    class SmartHRErrorRepresentation {
        public String message;
        public String type;

        public boolean isAlreadyExists() {
            return type.equals("BAD_REQUEST") && message.endsWith("already exists.");
        }

        public boolean isUnauthorized() {
            return type.equals("PERMISSION_DENIED");
        }

        public boolean isBlankUsername() {
            return type.equals("BAD_REQUEST") && message.endsWith("The username must not be blank.");
        }
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

    @JsonIgnoreProperties(ignoreUnknown = true)
    class SmartHRPermissionRepresentation {
        public Map<String, List<String>> connectionGroupPermissions;
        public Map<String, List<String>> connectionPermissions;
        public List<String> systemPermissions;
        public Map<String, List<String>> userPermissions;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    class SmartHRConnectionRepresentation {
        public String identifier;
        public String name;
        public String parentIdentifier;
        public String protocol;
        public Map<String, String> attributes = new HashMap<>();
        public Map<String, String> parameters = new HashMap<>();

        public void apply(SmartHRSchema schema, Attribute attr) {
            if (attr.is(Name.NAME)) {
                applyParentIdentifierAndName(AttributeUtil.getStringValue(attr));

            } else if (attr.is(ATTR_PROTOCOL)) {
                applyProtocol(AttributeUtil.getStringValue(attr));

            } else if (attr.is(ATTR_PARAMETERS)) {
                applyParameters(attr.getValue(), false);

            } else {
                if (!schema.isConnectionSchema(attr)) {
                    throw new InvalidAttributeValueException(String.format("SmartHR doesn't support to set '%s' attribute of Connection",
                            attr.getName()));
                }
                SmartHRAttribute guacaAttr = toSmartHRAttribute(schema.connectionSchema, attr);
                applyAttribute(guacaAttr);
            }
        }

        public void applyDelta(SmartHRSchema schema, AttributeDelta delta) {
            if (delta.is(Name.NAME)) {
                applyParentIdentifierAndName(AttributeDeltaUtil.getStringValue(delta));

            } else if (delta.is(ATTR_PROTOCOL)) {
                applyProtocol(AttributeDeltaUtil.getStringValue(delta));

            } else if (delta.is(ATTR_PARAMETERS)) {
                // We need to apply "ValuesToRemove" first because of replace situation
                applyParameters(delta.getValuesToRemove(), true);
                applyParameters(delta.getValuesToAdd(), false);

            } else {
                if (!schema.isConnectionSchema(delta)) {
                    throw new InvalidAttributeValueException(String.format("SmartHR doesn't support to set '%s' attribute of Connection",
                            delta.getName()));
                }
                SmartHRAttribute guacaAttr = toSmartHRAttribute(schema.connectionSchema, delta);
                applyAttribute(guacaAttr);
            }
        }

        private void applyParentIdentifierAndName(String s) {
            if (!s.contains("/")) {
                throw new InvalidAttributeValueException("Invalid name-with-parentIdentifier. The format must be <parentIdentifier>/<name>. value: " + s);
            }
            String parentIdentifier = s.substring(0, s.indexOf("/"));
            String name = s.substring(s.indexOf("/"));

            if (name.length() == 1) {
                throw new InvalidAttributeValueException("Invalid name-with-parentIdentifier. The format must be <parentIdentifier>/<name>. value: " + s);
            }
            name = name.substring(1);

            this.parentIdentifier = parentIdentifier;
            this.name = name;
        }

        private void applyProtocol(String protocol) {
            this.protocol = protocol;
        }

        private void applyAttribute(SmartHRAttribute attr) {
            this.attributes.put(attr.name, attr.value);
        }

        private void applyParameters(List<Object> values, boolean delete) {
            if (values == null) {
                return;
            }
            for (Object o : values) {
                if (!(o instanceof String) || !(((String) o).contains("="))) {
                    throw new InvalidAttributeValueException("Invalid parameter. It must be 'key=value' string format. value: " + o);
                }
                String kv = (String) o;
                String key = kv.substring(0, kv.indexOf('='));
                String value = kv.substring(kv.indexOf('='));

                if (delete) {
                    parameters.remove(key);
                    continue;
                }

                if (value.length() > 1) {
                    parameters.put(key, value.substring(1));
                } else {
                    parameters.put(key, null);
                }
            }
        }

        public List<SmartHRAttribute> toSmartHRAttributes() {
            return attributes.entrySet().stream().map(a -> new SmartHRAttribute(a.getKey(), a.getValue())).collect(Collectors.toList());
        }

        public List<SmartHRAttribute> toParameters() {
            return parameters.entrySet().stream().map(a -> new SmartHRAttribute(a.getKey(), a.getValue())).collect(Collectors.toList());
        }

        public String toUniqueName() {
            // Add parentIdentifier as prefix to make it unique
            return parentIdentifier + "/" + name;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    class SmartHRConnectionGroupRepresentation {
        public String identifier;
        public String name;
        public String parentIdentifier;
        public String type;
        public Map<String, String> attributes = new HashMap<>();

        public void apply(SmartHRSchema schema, Attribute attr) {
            if (attr.is(Name.NAME)) {
                applyParentIdentifierAndName(AttributeUtil.getStringValue(attr));

            } else {
                if (!schema.isConnectionSchema(attr)) {
                    throw new InvalidAttributeValueException(String.format("SmartHR doesn't support to set '%s' attribute of ConnectionGroup",
                            attr.getName()));
                }
                SmartHRAttribute guacaAttr = toSmartHRAttribute(schema.connectionGroupSchema, attr);
                applyAttribute(guacaAttr);
            }
        }

        public void applyDelta(SmartHRSchema schema, AttributeDelta delta) {
            if (delta.is(Name.NAME)) {
                applyParentIdentifierAndName(AttributeDeltaUtil.getStringValue(delta));

            } else {
                if (!schema.isConnectionSchema(delta)) {
                    throw new InvalidAttributeValueException(String.format("SmartHR doesn't support to set '%s' attribute of ConnectionGroup",
                            delta.getName()));
                }
                SmartHRAttribute guacaAttr = toSmartHRAttribute(schema.connectionGroupSchema, delta);
                applyAttribute(guacaAttr);
            }
        }

        private void applyParentIdentifierAndName(String s) {
            if (!s.contains("/")) {
                throw new InvalidAttributeValueException("Invalid name-with-parentIdentifier. The format must be <parentIdentifier>/<name>. value: " + s);
            }
            String parentIdentifier = s.substring(0, s.indexOf("/"));
            String name = s.substring(s.indexOf("/"));

            if (name.length() == 1) {
                throw new InvalidAttributeValueException("Invalid name-with-parentIdentifier. The format must be <parentIdentifier>/<name>. value: " + s);
            }
            name = name.substring(1);

            this.parentIdentifier = parentIdentifier;
            this.name = name;
        }

        private void applyName(String name) {
            this.name = name;
        }

        private void applyAttribute(SmartHRAttribute attr) {
            this.attributes.put(attr.name, attr.value);
        }

        public List<SmartHRAttribute> toSmartHRAttributes() {
            return attributes.entrySet().stream().map(a -> new SmartHRAttribute(a.getKey(), a.getValue())).collect(Collectors.toList());
        }

        public String toUniqueName() {
            // Add parentIdentifier as prefix to make it unique
            return parentIdentifier + "/" + name;
        }
    }
}