package jp.openstandia.connector.smarthr;

import org.identityconnectors.framework.common.objects.*;
import org.identityconnectors.framework.spi.operations.SearchOp;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Schema for SmartHR objects.
 *
 * @author Hiroyuki Wada
 */
public class SmartHRSchema {

    private final SmartHRConfiguration configuration;
    private final SmartHRClient client;

    public final Schema schema;
    public final Map<String, AttributeInfo> userSchema;
    public final Map<String, AttributeInfo> userGroupSchema;
    public final Map<String, AttributeInfo> connectionSchema;
    public final Map<String, AttributeInfo> connectionGroupSchema;

    public SmartHRSchema(SmartHRConfiguration configuration, SmartHRClient client,
                           List<SmartHRClient.SmartHRSchemaRepresentation> smarthrSchema) {
        this.configuration = configuration;
        this.client = client;

        SchemaBuilder schemaBuilder = new SchemaBuilder(SmartHRConnector.class);

        ObjectClassInfo userSchemaInfo = SmartHRUserHandler.createSchema(smarthrSchema);
        schemaBuilder.defineObjectClass(userSchemaInfo);

        ObjectClassInfo groupSchemaInfo = SmartHRUserGroupHandler.createSchema();
        schemaBuilder.defineObjectClass(groupSchemaInfo);

        ObjectClassInfo connectionSchemaInfo = SmartHRConnectionHandler.createSchema();
        schemaBuilder.defineObjectClass(connectionSchemaInfo);

        ObjectClassInfo connectionGroupSchemaInfo = SmartHRConnectionGroupHandler.createSchema();
        schemaBuilder.defineObjectClass(connectionGroupSchemaInfo);

        schemaBuilder.defineOperationOption(OperationOptionInfoBuilder.buildAttributesToGet(), SearchOp.class);
        schemaBuilder.defineOperationOption(OperationOptionInfoBuilder.buildReturnDefaultAttributes(), SearchOp.class);

        this.schema = schemaBuilder.build();

        Map<String, AttributeInfo> userSchemaMap = new HashMap<>();
        for (AttributeInfo info : userSchemaInfo.getAttributeInfo()) {
            userSchemaMap.put(info.getName(), info);
        }

        Map<String, AttributeInfo> groupSchemaMap = new HashMap<>();
        for (AttributeInfo info : groupSchemaInfo.getAttributeInfo()) {
            groupSchemaMap.put(info.getName(), info);
        }

        Map<String, AttributeInfo> connectionSchema = new HashMap<>();
        for (AttributeInfo info : connectionSchemaInfo.getAttributeInfo()) {
            connectionSchema.put(info.getName(), info);
        }

        Map<String, AttributeInfo> connectionGroupSchema = new HashMap<>();
        for (AttributeInfo info : connectionGroupSchemaInfo.getAttributeInfo()) {
            connectionGroupSchema.put(info.getName(), info);
        }

        this.userSchema = Collections.unmodifiableMap(userSchemaMap);
        this.userGroupSchema = Collections.unmodifiableMap(groupSchemaMap);
        this.connectionSchema = Collections.unmodifiableMap(connectionSchema);
        this.connectionGroupSchema = Collections.unmodifiableMap(connectionGroupSchema);
    }

    public boolean isUserSchema(Attribute attribute) {
        return userSchema.containsKey(attribute.getName());
    }

    public boolean isMultiValuedUserSchema(Attribute attribute) {
        return userSchema.get(attribute.getName()).isMultiValued();
    }

    public boolean isUserSchema(AttributeDelta delta) {
        return userSchema.containsKey(delta.getName());
    }

    public boolean isMultiValuedUserSchema(AttributeDelta delta) {
        return userSchema.get(delta.getName()).isMultiValued();
    }

    public AttributeInfo getUserSchema(String attributeName) {
        return userSchema.get(attributeName);
    }

    public boolean isUserGroupSchema(Attribute attribute) {
        return userGroupSchema.containsKey(attribute.getName());
    }

    public boolean isMultiValuedUserGroupSchema(Attribute attribute) {
        return userGroupSchema.get(attribute.getName()).isMultiValued();
    }

    public boolean isUserGroupSchema(AttributeDelta delta) {
        return userGroupSchema.containsKey(delta.getName());
    }

    public boolean isMultiValuedUserGroupSchema(AttributeDelta delta) {
        return userGroupSchema.get(delta.getName()).isMultiValued();
    }

    public AttributeInfo getUserGroupSchema(String attributeName) {
        return userGroupSchema.get(attributeName);
    }

    public boolean isConnectionSchema(Attribute attribute) {
        return connectionSchema.containsKey(attribute.getName());
    }

    public boolean isConnectionSchema(AttributeDelta delta) {
        return connectionSchema.containsKey(delta.getName());
    }

    public boolean isConnectionGroupSchema(Attribute attribute) {
        return connectionGroupSchema.containsKey(attribute.getName());
    }

    public boolean isConnectionGroupSchema(AttributeDelta delta) {
        return connectionGroupSchema.containsKey(delta.getName());
    }
}