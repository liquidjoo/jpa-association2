package hibernate.dml;

import hibernate.entity.meta.column.EntityColumn;

public class DeleteQueryBuilder implements QueryBuilder {

    private static final String DELETE_QUERY = "delete from %s where %s = %s;";

    public DeleteQueryBuilder() {
    }

    public String generateQuery(
            final String tableName,
            final EntityColumn entityId,
            final Object id
    ) {
        return String.format(DELETE_QUERY, tableName, entityId.getFieldName(), id);
    }
}
