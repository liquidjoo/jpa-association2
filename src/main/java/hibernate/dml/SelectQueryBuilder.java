package hibernate.dml;

import hibernate.entity.meta.column.EntityColumn;

import java.util.List;
import java.util.stream.Collectors;

public class SelectQueryBuilder {

    private static final String SELECT_ALL_QUERY = "select %s from %s;";
    private static final String SELECT_QUERY = "select %s from %s where %s = %s;";

    private static final String SELECT_QUERY_COLUMN_DELIMITER = ", ";

    public SelectQueryBuilder() {
    }

    public String generateQuery(
            final String tableName,
            final List<String> fieldNames,
            final EntityColumn entityId,
            final Object id
    ) {
        return String.format(SELECT_QUERY, parseColumnQueries(fieldNames), tableName, entityId.getFieldName(), id);
    }

    public String generateAllQuery(final String tableName, final List<String> fieldNames) {
        return String.format(SELECT_ALL_QUERY, parseColumnQueries(fieldNames), tableName);
    }


    public String generateQuery(
            final String tableName,
            final List<String> fieldNames,
            final EntityColumn entityId,
            final Object id,
            final boolean join,
            final String joinTableName,
            final List<String> joinFieldNames
    ) {
        return generateDefaultQuery(tableName, fieldNames, entityId, join, joinTableName, joinFieldNames)
                .append(" ")
                .append("where ")
                .append(parseColumnQuery(tableName, entityId.getFieldName()))
                .append(" = ")
                .append(id)
                .append(";")
                .toString();
    }


    private StringBuilder generateDefaultQuery(String tableName, List<String> fieldNames, EntityColumn entityId, boolean join, String joinTableName, List<String> joinTableFieldNames) {
        final List<String> parsedFieldNames = fieldNames.stream()
                .map(fieldName -> parseColumnQuery(tableName, fieldName))
                .collect(Collectors.toList());

        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("select ")
                .append(parseColumnQueries(parsedFieldNames));
        if (join) {
            queryBuilder.append(", ")
                    .append(parseColumnQueries(joinTableName, joinTableFieldNames))
                    .append(" ");
        } else {
            queryBuilder.append(" ");
        }
        queryBuilder.append("from ")
                .append(tableName)
                .append(" ");
        if (join) {
            queryBuilder.append(parseJoinTableQuery(tableName, entityId, joinTableName));
        }
        return queryBuilder;
    }

    private String parseColumnQueries(final List<String> fieldNames) {
        return String.join(SELECT_QUERY_COLUMN_DELIMITER, fieldNames);
    }

    private String parseColumnQueries(final String tableName, List<String> fieldNames) {
        return fieldNames.stream()
                .map(fieldName -> parseColumnQuery(tableName, fieldName))
                .collect(Collectors.joining(SELECT_QUERY_COLUMN_DELIMITER));
    }

    private String parseColumnQuery(final String tableName, String fieldName) {
        return tableName + "." + fieldName;
    }

    private String parseJoinTableQuery(final String tableName, final EntityColumn entityId, String joinTableName) {
        return "join " + joinTableName + " on " + parseColumnQuery(tableName, entityId.getFieldName()) + " = " + parseColumnQuery(joinTableName, tableName + "_" + entityId.getFieldName());
    }
}
