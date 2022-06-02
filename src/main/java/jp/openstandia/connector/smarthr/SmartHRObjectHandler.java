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

import org.identityconnectors.framework.common.objects.*;

import java.util.Set;

public interface SmartHRObjectHandler {

    Uid create(Set<Attribute> attributes);

    Set<AttributeDelta> updateDelta(Uid uid, Set<AttributeDelta> modifications, OperationOptions options);

    void delete(Uid uid, OperationOptions options);

    int getByUid(Uid uid, ResultsHandler resultsHandler, OperationOptions options,
                 Set<String> returnAttributesSet, Set<String> fetchFieldsSet,
                 boolean allowPartialAttributeValues, int pageSize, int pageOffset);

    int getByName(Name name, ResultsHandler resultsHandler, OperationOptions options,
                  Set<String> returnAttributesSet, Set<String> fetchFieldsSet,
                  boolean allowPartialAttributeValues, int pageSize, int pageOffset);

    int getAll(ResultsHandler resultsHandler, OperationOptions options,
               Set<String> returnAttributesSet, Set<String> fetchFieldsSet,
               boolean allowPartialAttributeValues, int pageSize, int pageOffset);

    default <T> ConnectorObject toConnectorObject(SchemaDefinition schema, T crew,
                                                  Set<String> returnAttributesSet, boolean allowPartialAttributeValues) {
        ConnectorObjectBuilder builder = schema.toConnectorObjectBuilder(crew, returnAttributesSet, allowPartialAttributeValues);
        return builder.build();
    }

    SchemaDefinition getSchema();

}
