package jp.openstandia.connector.smarthr;

import org.identityconnectors.framework.common.objects.*;

import java.util.Set;

public interface SmartHRObjectHandler {

    Uid create(Set<Attribute> attributes);

    Set<AttributeDelta> updateDelta(Uid uid, Set<AttributeDelta> modifications, OperationOptions options);

    void delete(Uid uid, OperationOptions options);

    void getByUid(Uid uid, ResultsHandler resultsHandler, OperationOptions options, Set<String> attributesToGet, boolean allowPartialAttributeValues, int pageSize, int pageOffset);

    void getByName(Name name, ResultsHandler resultsHandler, OperationOptions options, Set<String> attributesToGet, boolean allowPartialAttributeValues, int pageSize, int pageOffset);

    void getAll(ResultsHandler resultsHandler, OperationOptions options, Set<String> attributesToGet, boolean allowPartialAttributeValues, int pageSize, int pageOffset);

    default <T> ConnectorObject toConnectorObject(SchemaDefinition schema, T crew,
                                                  Set<String> attributesToGet, boolean allowPartialAttributeValues) {
        ConnectorObjectBuilder builder = schema.toConnectorObjectBuilder(crew, attributesToGet, allowPartialAttributeValues);
        return builder.build();
    }

    SchemaDefinition getSchema();

}
