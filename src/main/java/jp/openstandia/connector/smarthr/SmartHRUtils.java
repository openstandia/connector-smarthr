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

import org.identityconnectors.common.logging.Log;
import org.identityconnectors.framework.common.objects.OperationOptions;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Provides utility methods
 *
 * @author Hiroyuki Wada
 */
public class SmartHRUtils {
    private static final Log LOG = Log.getLog(SmartHRUtils.class);

    public static ZonedDateTime toZoneDateTime(String yyyymmdd) {
        LocalDate date = LocalDate.parse(yyyymmdd);
        return date.atStartOfDay(ZoneId.systemDefault());
    }

    /**
     * Check if attrsToGetSet contains the attribute.
     *
     * @param attrsToGetSet
     * @param attr
     * @param isReturnByDefault
     * @return
     */
    public static boolean shouldReturn(Set<String> attrsToGetSet, String attr, boolean isReturnByDefault) {
        if (attrsToGetSet == null) {
            return isReturnByDefault;
        }
        return attrsToGetSet.contains(attr);
    }

    /**
     * Check if ALLOW_PARTIAL_ATTRIBUTE_VALUES == true.
     *
     * @param options
     * @return
     */
    public static boolean shouldAllowPartialAttributeValues(OperationOptions options) {
        // If the option isn't set from IDM, it may be null.
        return Boolean.TRUE.equals(options.getAllowPartialAttributeValues());
    }

    /**
     * Check if RETURN_DEFAULT_ATTRIBUTES == true.
     *
     * @param options
     * @return
     */
    public static boolean shouldReturnDefaultAttributes(OperationOptions options) {
        // If the option isn't set from IDM, it may be null.
        return Boolean.TRUE.equals(options.getReturnDefaultAttributes());
    }

    /**
     * Create full map of ATTRIBUTES_TO_GET which is composed by RETURN_DEFAULT_ATTRIBUTES + ATTRIBUTES_TO_GET.
     * Key: attribute name of the connector (e.g. __UID__)
     * Value: field name for resource fetching
     *
     * @param schema
     * @param options
     * @return
     */
    public static Map<String, String> createFullAttributesToGet(SchemaDefinition schema, OperationOptions options) {
        Map<String, String> attributesToGet = new HashMap<>();
        attributesToGet.putAll(toReturnedByDefaultAttributesSet(schema));

        if (options.getAttributesToGet() != null) {
            for (String a : options.getAttributesToGet()) {
                String fetchField = schema.getFetchField(a);
                if (fetchField == null) {
                    LOG.warn("Requested unknown attribute to get. Ignored it: {0}", a);
                    continue;
                }
                attributesToGet.put(a, fetchField);
            }
        }

        return attributesToGet;
    }

    private static Map<String, String> toReturnedByDefaultAttributesSet(SchemaDefinition schema) {
        return schema.getReturnedByDefaultAttributesSet();
    }

    public static int resolvePageSize(SmartHRConfiguration configuration, OperationOptions options) {
        if (options.getPageSize() != null) {
            return options.getPageSize();
        }
        return configuration.getDefaultQueryPageSize();
    }

    public static int resolvePageOffset(OperationOptions options) {
        if (options.getPagedResultsOffset() != null) {
            return options.getPagedResultsOffset();
        }
        return 0;
    }
}
