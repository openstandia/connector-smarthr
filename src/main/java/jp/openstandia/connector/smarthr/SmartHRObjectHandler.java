package jp.openstandia.connector.smarthr;

import org.identityconnectors.framework.common.objects.*;

import java.util.Set;

public interface SmartHRObjectHandler {

    Uid create(Set<Attribute> attributes);

    Set<AttributeDelta> updateDelta(Uid uid, Set<AttributeDelta> modifications, OperationOptions options);

    void delete(Uid uid, OperationOptions options);

    void query(SmartHRFilter filter, ResultsHandler resultsHandler, OperationOptions options);
}
