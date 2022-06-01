package jp.openstandia.connector.smarthr;

import org.identityconnectors.framework.common.exceptions.InvalidAttributeValueException;
import org.identityconnectors.framework.common.objects.*;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static jp.openstandia.connector.smarthr.SmartHRUtils.shouldReturn;

public class SchemaDefinition {


    public static Builder newBuilder(ObjectClass objectClass) {
        Builder schemaBuilder = new Builder(objectClass);
        return schemaBuilder;
    }

    public static class Builder {
        private final ObjectClass objectClass;
        private final List<AttributeMapper> attributes = new ArrayList<>();

        public Builder(ObjectClass objectClass) {
            this.objectClass = objectClass;
        }

        public <T, C, U, R> void addUid(String name,
                                        Types<T> typeClass,
                                        Class<C> createClass,
                                        Class<U> updateClass,
                                        Class<R> readClass,

                                        BiConsumer<T, C> create,
                                        BiConsumer<T, U> update,
                                        Function<R, T> read,

                                        SchemaOption... options
        ) {
            AttributeMapper attr = new AttributeMapper(name, typeClass, create, update, read, options);
            this.attributes.add(attr);
        }

        public <T, CU, R> void addUid(String name,
                                      Types<T> typeClass,
                                      Class<CU> createOrUpdateClass,
                                      Class<R> readClass,

                                      BiConsumer<T, CU> createOrUpdate,
                                      Function<R, T> read,

                                      SchemaOption... options
        ) {
            AttributeMapper attr = new AttributeMapper(name, typeClass, createOrUpdate, createOrUpdate, read, options);
            this.attributes.add(attr);
        }

        public <T, C, U, R> void addName(String name,
                                         Types<T> typeClass,
                                         Class<C> createClass,
                                         Class<U> updateClass,
                                         Class<R> readClass,

                                         BiConsumer<T, C> create,
                                         BiConsumer<T, U> update,
                                         Function<R, T> read,

                                         SchemaOption... options
        ) {
            AttributeMapper attr = new AttributeMapper(name, typeClass, create, update, read, options);
            this.attributes.add(attr);
        }

        public <T, CU, R> void addName(String name,
                                       Types<T> typeClass,
                                       Class<CU> createOrUpdateClass,
                                       Class<R> readClass,

                                       BiConsumer<T, CU> createOrUpdate,
                                       Function<R, T> read,

                                       SchemaOption... options
        ) {
            AttributeMapper attr = new AttributeMapper(name, typeClass, createOrUpdate, createOrUpdate, read, options);
            this.attributes.add(attr);
        }

        public <T, C, U, R> void add(String name,
                                     Types<T> typeClass,
                                     Class<C> createClass,
                                     Class<U> updateClass,
                                     Class<R> readClass,

                                     BiConsumer<T, C> create,
                                     BiConsumer<T, U> update,
                                     Function<R, T> read,

                                     SchemaOption... options
        ) {
            AttributeMapper attr = new AttributeMapper(name, typeClass, create, update, read, options);
            this.attributes.add(attr);
        }

        public <T, CU, R> void add(String name,
                                   Types<T> typeClass,
                                   Class<CU> createOrUpdateClass,
                                   Class<R> readClass,

                                   BiConsumer<T, CU> createOrUpdate,
                                   Function<R, T> read,

                                   SchemaOption... options
        ) {
            AttributeMapper attr = new AttributeMapper(name, typeClass, createOrUpdate, createOrUpdate, read, options);
            this.attributes.add(attr);
        }

        public <T, C, U, R> void addAsMultiple(String name,
                                               Types<T> typeClass,
                                               Class<C> createClass,
                                               Class<U> updateClass,
                                               Class<R> readClass,

                                               BiConsumer<List<T>, C> create,
                                               BiConsumer<List<T>, U> updateAdd,
                                               BiConsumer<List<T>, U> updateRemove,
                                               Function<R, List<T>> read,

                                               SchemaOption... options
        ) {
            AttributeMapper attr = new AttributeMapper(name, typeClass, create, updateAdd, updateRemove, read, options);
            this.attributes.add(attr);
        }

        public SchemaDefinition build() {
            SchemaDefinition schemaDefinition = new SchemaDefinition(objectClass, buildSchemaInfo(), buildAttributeMap());
            return schemaDefinition;
        }

        public ObjectClassInfo buildSchemaInfo() {
            List<AttributeInfo> list = attributes.stream()
                    .map(attr -> {
                        AttributeInfoBuilder define = AttributeInfoBuilder.define(attr.name);

                        define.setType(attr.type.typeClass);
                        define.setMultiValued(attr.isMultiple);
                        define.setNativeName(attr.name);

                        if (attr.type == Types.UUID) {
                            define.setSubtype(AttributeInfo.Subtypes.STRING_UUID);

                        } else if (attr.type == Types.STRING_CASE_IGNORE) {
                            define.setSubtype(AttributeInfo.Subtypes.STRING_CASE_IGNORE);

                        } else if (attr.type == Types.STRING_URI) {
                            define.setSubtype(AttributeInfo.Subtypes.STRING_URI);

                        } else if (attr.type == Types.STRING_LDAP_DN) {
                            define.setSubtype(AttributeInfo.Subtypes.STRING_LDAP_DN);

                        } else if (attr.type == Types.XML) {
                            define.setSubtype(AttributeInfo.Subtypes.STRING_XML);

                        } else if (attr.type == Types.JSON) {
                            define.setSubtype(AttributeInfo.Subtypes.STRING_JSON);
                        }

                        for (SchemaOption option : attr.options) {
                            switch (option) {
                                case REQUIRED: {
                                    define.setRequired(true);
                                    break;
                                }
                                case NOT_CREATABLE: {
                                    define.setCreateable(false);
                                    break;
                                }
                                case NOT_UPDATABLE: {
                                    define.setUpdateable(false);
                                    break;
                                }
                                case NOT_READABLE: {
                                    define.setReadable(false);
                                    break;
                                }
                                case NOT_RETURN_BY_DEFAULT: {
                                    define.setReturnedByDefault(false);
                                    break;
                                }
                            }
                        }

                        return define.build();
                    })
                    .collect(Collectors.toList());

            ObjectClassInfoBuilder builder = new ObjectClassInfoBuilder();
            builder.setType(objectClass.getObjectClassValue());
            builder.addAllAttributeInfo(list);

            return builder.build();
        }

        public Map<String, AttributeMapper> buildAttributeMap() {
            Map<String, AttributeMapper> map = attributes.stream()
                    .collect(Collectors.toMap(a -> a.name, a -> a));
            return map;
        }
    }

    private final ObjectClass objectClass;
    private final ObjectClassInfo objectClassInfo;
    private final Map<String, AttributeMapper> attributeMap;
    private final Set<String> returnedByDefaultAttributesSet;

    public SchemaDefinition(ObjectClass objectClass, ObjectClassInfo objectClassInfo, Map<String, AttributeMapper> attributeMap) {
        this.objectClass = objectClass;
        this.objectClassInfo = objectClassInfo;
        this.attributeMap = attributeMap;
        this.returnedByDefaultAttributesSet = getObjectClassInfo().getAttributeInfo().stream()
                .filter(i -> i.isReturnedByDefault())
                .map(i -> i.getName())
                .collect(Collectors.toSet());
    }

    public ObjectClassInfo getObjectClassInfo() {
        return objectClassInfo;
    }

    public Set<String> getReturnedByDefaultAttributesSet() {
        return returnedByDefaultAttributesSet;
    }

    public <T> T apply(Set<Attribute> attrs, T dest) {
        for (Attribute attr : attrs) {
            AttributeMapper attributeMapper = attributeMap.get(attr.getName());
            if (attributeMapper == null) {
                throw new InvalidAttributeValueException("Invalid attribute: " + attr.getName());
            }

            attributeMapper.apply(attr, dest);
        }
        return dest;
    }

    public <U> boolean applyDelta(Set<AttributeDelta> deltas, U dest) {
        boolean changed = false;
        for (AttributeDelta delta : deltas) {
            AttributeMapper attributeMapper = attributeMap.get(delta.getName());
            if (attributeMapper == null) {
                throw new InvalidAttributeValueException("Invalid attribute: " + delta.getName());
            }

            attributeMapper.apply(delta, dest);
            changed = true;
        }
        return changed;
    }

    public <R> ConnectorObjectBuilder toConnectorObjectBuilder(R source, Set<String> attributesToGet, boolean allowPartialAttributeValues) {
        final ConnectorObjectBuilder builder = new ConnectorObjectBuilder()
                .setObjectClass(objectClass);

        AttributeMapper uid = attributeMap.get(Uid.NAME);
        builder.addAttribute(uid.apply(source));

        // Need to set __NAME__ because it throws IllegalArgumentException
        AttributeMapper name = attributeMap.get(Name.NAME);
        builder.addAttribute(name.apply(source));

        for (Map.Entry<String, AttributeMapper> entry : attributeMap.entrySet()) {
            if (shouldReturn(attributesToGet, entry.getKey())) {
                Attribute value = entry.getValue().apply(source);
                if (value != null) {
                    builder.addAttribute(value);
                }
            }
        }

        return builder;
    }

    public String getType() {
        return objectClassInfo.getType();
    }

    public static class Types<TC> {
        public static final Types<String> STRING = new Types(String.class);
        public static final Types<String> STRING_CASE_IGNORE = new Types(String.class);
        public static final Types<String> STRING_URI = new Types(String.class);
        public static final Types<String> STRING_LDAP_DN = new Types(String.class);
        public static final Types<String> XML = new Types(String.class);
        public static final Types<String> UUID = new Types(String.class);
        public static final Types<String> JSON = new Types(String.class);
        public static final Types<Integer> INTEGER = new Types(Integer.class);
        public static final Types<Integer> LONG = new Types(Long.class);
        public static final Types<Integer> FLOAT = new Types(Float.class);
        public static final Types<Integer> DOUBLE = new Types(Double.class);
        public static final Types<BigDecimal> BIG_DECIMAL = new Types(BigDecimal.class);
        public static final Types<String> DATE_STRING = new Types(ZonedDateTime.class);
        public static final Types<String> DATETIME_STRING = new Types(ZonedDateTime.class);
        public static final Types<ZonedDateTime> DATE = new Types(ZonedDateTime.class);
        public static final Types<ZonedDateTime> DATETIME = new Types(ZonedDateTime.class);
        public static final Types<Boolean> BOOLEAN = new Types(Boolean.class);

        private final Class<TC> typeClass;

        private Types(Class<TC> typeClass) {
            this.typeClass = typeClass;
        }
    }

    public static SchemaOption[] newOptions(SchemaOption... options) {
        return options;
    }

    static enum SchemaOption {
        REQUIRED,
        NOT_CREATABLE,
        NOT_UPDATABLE,
        NOT_READABLE,
        NOT_RETURN_BY_DEFAULT,
    }

    static class AttributeMapper<T, C, U, R> {
        private final String name;
        private final Types<T> type;
        boolean isMultiple;

        private final BiConsumer<T, C> create;
        private final BiConsumer<T, U> replace;
        private final BiConsumer<List<T>, U> add;
        private final BiConsumer<List<T>, U> remove;
        private final Function<R, Object> read;

        private final SchemaOption[] options;

        private DateTimeFormatter dateFormat;
        private DateTimeFormatter dateTimeFormat;

        private static final DateTimeFormatter DEFAULT_DATE_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;
        private static final DateTimeFormatter DEFAULT_DATE_TIME_FORMAT = DateTimeFormatter.ISO_LOCAL_DATE;

        public AttributeMapper(String name, Types<T> typeClass,
                               BiConsumer<T, C> create,
                               BiConsumer<T, U> replace,
                               Function<R, Object> read,
                               SchemaOption... options
        ) {
            this.name = name;
            this.type = typeClass;
            this.create = create;
            this.replace = replace;
            this.add = null;
            this.remove = null;
            this.read = read;
            this.options = options;

            this.isMultiple = false;
        }

        public AttributeMapper(String name, Types<T> typeClass,
                               BiConsumer<T, C> create,
                               BiConsumer<List<T>, U> add,
                               BiConsumer<List<T>, U> remove,
                               Function<R, Object> read,
                               SchemaOption... options
        ) {
            this.name = name;
            this.type = typeClass;
            this.create = create;
            this.replace = null;
            this.add = add;
            this.remove = remove;
            this.read = read;
            this.options = options;

            this.isMultiple = true;
        }

        public AttributeMapper dateFormat(DateTimeFormatter dateFormat) {
            this.dateFormat = dateFormat;
            return this;
        }

        public AttributeMapper datetimeFormat(DateTimeFormatter datetimeFormat) {
            this.dateTimeFormat = datetimeFormat;
            return this;
        }

        private String formatDate(ZonedDateTime zonedDateTime) {
            if (this.dateFormat == null) {
                return zonedDateTime.format(DEFAULT_DATE_FORMAT);
            }
            return zonedDateTime.format(this.dateFormat);
        }

        private String formatDateTime(ZonedDateTime zonedDateTime) {
            if (this.dateTimeFormat == null) {
                return zonedDateTime.format(DEFAULT_DATE_TIME_FORMAT);
            }
            return zonedDateTime.format(this.dateFormat);
        }

        private ZonedDateTime toDate(String dateString) {
            LocalDate date;
            if (this.dateFormat == null) {
                date = LocalDate.parse(dateString, this.dateFormat);
            } else {
                date = LocalDate.parse(dateString, DEFAULT_DATE_FORMAT);
            }
            return date.atStartOfDay(ZoneId.systemDefault());
        }

        private ZonedDateTime toDateTime(String dateTimeString) {
            ZonedDateTime dateTime;
            if (this.dateTimeFormat == null) {
                dateTime = ZonedDateTime.parse(dateTimeString, this.dateTimeFormat);
            } else {
                dateTime = ZonedDateTime.parse(dateTimeString, DEFAULT_DATE_TIME_FORMAT);
            }
            return dateTime;
        }

        public void apply(Attribute source, C dest) {
            if (create == null) {
                return;
            }

            if (isMultiple) {
                if (type == Types.DATE_STRING) {
                    List<T> values = source.getValue().stream()
                            .map(v -> (ZonedDateTime) v)
                            .map(v -> (T) formatDate(v))
                            .collect(Collectors.toList());
                    create.accept((T) values, dest);

                } else if (type == Types.DATETIME_STRING) {
                    List<T> values = source.getValue().stream()
                            .map(v -> (ZonedDateTime) v)
                            .map(v -> (T) formatDateTime(v))
                            .collect(Collectors.toList());
                    create.accept((T) values, dest);

                } else {
                    List<T> values = source.getValue().stream().map(v -> (T) v).collect(Collectors.toList());
                    create.accept((T) values, dest);
                }

            } else {
                if (type == Types.STRING) {
                    String value = AttributeUtil.getAsStringValue(source);
                    create.accept((T) value, dest);

                } else if (type == Types.INTEGER) {
                    Integer value = AttributeUtil.getIntegerValue(source);
                    create.accept((T) value, dest);

                } else if (type == Types.LONG) {
                    Long value = AttributeUtil.getLongValue(source);
                    create.accept((T) value, dest);

                } else if (type == Types.FLOAT) {
                    Float value = AttributeUtil.getFloatValue(source);
                    create.accept((T) value, dest);

                } else if (type == Types.DOUBLE) {
                    Double value = AttributeUtil.getDoubleValue(source);
                    create.accept((T) value, dest);

                } else if (type == Types.BOOLEAN) {
                    Boolean value = AttributeUtil.getBooleanValue(source);
                    create.accept((T) value, dest);

                } else if (type == Types.BIG_DECIMAL) {
                    BigDecimal value = AttributeUtil.getBigDecimalValue(source);
                    create.accept((T) value, dest);

                } else if (type == Types.DATE) {
                    ZonedDateTime date = (ZonedDateTime) AttributeUtil.getSingleValue(source);
                    String formatted = formatDate(date);
                    create.accept((T) formatted, dest);

                } else if (type == Types.DATETIME) {
                    ZonedDateTime date = (ZonedDateTime) AttributeUtil.getSingleValue(source);
                    String formatted = formatDateTime(date);
                    create.accept((T) formatted, dest);
                }
            }
        }

        public void apply(AttributeDelta source, U dest) {
            if (isMultiple) {
                if (add == null || remove == null) {
                    return;
                }

                if (type == Types.DATE_STRING) {
                    List<T> valuesToAdd = source.getValuesToAdd().stream()
                            .map(v -> (ZonedDateTime) v)
                            .map(v -> (T) formatDate(v))
                            .collect(Collectors.toList());
                    List<T> valuesToRemove = source.getValuesToRemove().stream()
                            .map(v -> (ZonedDateTime) v)
                            .map(v -> (T) formatDate(v))
                            .collect(Collectors.toList());

                    add.accept(valuesToAdd, dest);
                    remove.accept(valuesToRemove, dest);

                } else if (type == Types.DATETIME_STRING) {
                    List<T> valuesToAdd = source.getValuesToAdd().stream()
                            .map(v -> (ZonedDateTime) v)
                            .map(v -> (T) formatDateTime(v))
                            .collect(Collectors.toList());
                    List<T> valuesToRemove = source.getValuesToRemove().stream()
                            .map(v -> (ZonedDateTime) v)
                            .map(v -> (T) formatDateTime(v))
                            .collect(Collectors.toList());

                    add.accept(valuesToAdd, dest);
                    remove.accept(valuesToRemove, dest);

                } else {
                    List<T> valuesToAdd = source.getValuesToAdd().stream().map(v -> (T) v).collect(Collectors.toList());
                    List<T> valuesToRemove = source.getValuesToRemove().stream().map(v -> (T) v).collect(Collectors.toList());

                    add.accept(valuesToAdd, dest);
                    remove.accept(valuesToRemove, dest);
                }

            } else {
                if (replace == null) {
                    return;
                }

                if (type == Types.STRING) {
                    String value = AttributeDeltaUtil.getAsStringValue(source);
                    replace.accept((T) value, dest);

                } else if (type == Types.INTEGER) {
                    Integer value = AttributeDeltaUtil.getIntegerValue(source);
                    replace.accept((T) value, dest);

                } else if (type == Types.LONG) {
                    Long value = AttributeDeltaUtil.getLongValue(source);
                    replace.accept((T) value, dest);

                } else if (type == Types.FLOAT) {
                    Float value = AttributeDeltaUtil.getFloatValue(source);
                    replace.accept((T) value, dest);

                } else if (type == Types.DOUBLE) {
                    Double value = AttributeDeltaUtil.getDoubleValue(source);
                    replace.accept((T) value, dest);

                } else if (type == Types.BOOLEAN) {
                    Boolean value = AttributeDeltaUtil.getBooleanValue(source);
                    replace.accept((T) value, dest);

                } else if (type == Types.BIG_DECIMAL) {
                    BigDecimal value = AttributeDeltaUtil.getBigDecimalValue(source);
                    replace.accept((T) value, dest);

                } else if (type == Types.DATE || type == Types.DATETIME) {
                    ZonedDateTime date = (ZonedDateTime) AttributeDeltaUtil.getSingleValue(source);
                    replace.accept((T) date, dest);

                } else if (type == Types.DATE_STRING) {
                    ZonedDateTime date = (ZonedDateTime) AttributeDeltaUtil.getSingleValue(source);
                    String formatted = formatDate(date);
                    replace.accept((T) formatted, dest);

                } else if (type == Types.DATETIME_STRING) {
                    ZonedDateTime date = (ZonedDateTime) AttributeDeltaUtil.getSingleValue(source);
                    String formatted = formatDateTime(date);
                    replace.accept((T) formatted, dest);
                }
            }
        }

        public Attribute apply(R source) {
            if (read == null) {
                return null;
            }

            Object value = read.apply(source);
            if (value == null) {
                return null;
            }

            if (isMultiple) {
                if (type == Types.DATE_STRING) {
                    List<ZonedDateTime> values = ((List<?>) value).stream()
                            .map(v -> (String) v)
                            .map(v -> toDate(v))
                            .collect(Collectors.toList());
                    return AttributeBuilder.build(name, values);

                } else if (type == Types.DATETIME_STRING) {
                    List<ZonedDateTime> values = ((List<?>) value).stream()
                            .map(v -> (String) v)
                            .map(v -> toDateTime(v))
                            .collect(Collectors.toList());
                    return AttributeBuilder.build(name, values);

                } else {
                    return AttributeBuilder.build(name, (List<?>) value);
                }

            } else {
                if (type == Types.DATE_STRING) {
                    ZonedDateTime date = toDate(value.toString());
                    return AttributeBuilder.build(name, date);

                } else if (type == Types.DATETIME_STRING) {
                    ZonedDateTime dateTime = toDateTime(value.toString());
                    return AttributeBuilder.build(name, dateTime);
                }
                return AttributeBuilder.build(name, value);
            }
        }
    }
}