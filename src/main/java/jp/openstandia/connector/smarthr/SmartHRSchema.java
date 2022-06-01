package jp.openstandia.connector.smarthr;

import org.identityconnectors.framework.common.objects.ObjectClass;
import org.identityconnectors.framework.common.objects.OperationOptionInfoBuilder;
import org.identityconnectors.framework.common.objects.Schema;
import org.identityconnectors.framework.common.objects.SchemaBuilder;
import org.identityconnectors.framework.spi.operations.SearchOp;

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

    private Map<String, SmartHRObjectHandler> schemaHandlerMap;

    public SmartHRSchema(SmartHRConfiguration configuration, SmartHRClient client,
                         List<SmartHRClient.CrewCustomField> smarthrSchema) {
        this.configuration = configuration;
        this.client = client;

        SchemaBuilder schemaBuilder = new SchemaBuilder(SmartHRConnector.class);

        SchemaDefinition crewSchema = SmartHRCrewHandler.createSchema(smarthrSchema).build();
        schemaBuilder.defineObjectClass(crewSchema.getObjectClassInfo());

        SchemaDefinition deptSchema = SmartHRDepartmentHandler.createSchema().build();
        schemaBuilder.defineObjectClass(crewSchema.getObjectClassInfo());

        schemaBuilder.defineOperationOption(OperationOptionInfoBuilder.buildAttributesToGet(), SearchOp.class);
        schemaBuilder.defineOperationOption(OperationOptionInfoBuilder.buildReturnDefaultAttributes(), SearchOp.class);

        this.schema = schemaBuilder.build();

        this.schemaHandlerMap = new HashMap<>();
        this.schemaHandlerMap.put(crewSchema.getType(), new SmartHRCrewHandler(configuration, client, crewSchema));
        this.schemaHandlerMap.put(deptSchema.getType(), new SmartHRDepartmentHandler(configuration, client, deptSchema));
    }

    public SmartHRObjectHandler getSchemaHandler(ObjectClass objectClass) {
        return schemaHandlerMap.get(objectClass.getObjectClassValue());
    }
}