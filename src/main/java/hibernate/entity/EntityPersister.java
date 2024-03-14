package hibernate.entity;

import hibernate.dml.DeleteQueryBuilder;
import hibernate.dml.InsertQueryBuilder;
import hibernate.dml.UpdateQueryBuilder;
import hibernate.entity.meta.EntityBindingMap;
import hibernate.entity.meta.PersistentClass;
import hibernate.entity.meta.column.EntityColumn;
import jdbc.JdbcTemplate;

import java.util.Map;

public class EntityPersister {

    private final JdbcTemplate jdbcTemplate;
    private final EntityBindingMap entityBindingMap;
    private final InsertQueryBuilder insertQueryBuilder = new InsertQueryBuilder();
    private final DeleteQueryBuilder deleteQueryBuilder = new DeleteQueryBuilder();
    private final UpdateQueryBuilder updateQueryBuilder = new UpdateQueryBuilder();

    public EntityPersister(final JdbcTemplate jdbcTemplate, final EntityBindingMap entityBindingMap) {
        this.jdbcTemplate = jdbcTemplate;
        this.entityBindingMap = entityBindingMap;
    }

    public boolean update(final PersistentClass<?> persistentClass, final Object entityId, final Map<EntityColumn, Object> updateFields) {
        final String query = updateQueryBuilder.generateQuery(
                persistentClass.getEntityName(),
                updateFields,
                persistentClass.getEntityId(),
                entityId
        );
        return jdbcTemplate.executeUpdate(query);
    }

    public Object insert(final PersistentClass<?> persistentClass, final Object entity) {
        final String query = insertQueryBuilder.generateQuery(
                persistentClass.getEntityName(),
                persistentClass.getFieldValues(entity)
        );
        return jdbcTemplate.executeInsert(query);
    }

    public void delete(final PersistentClass<?> persistentClass, final Object entity) {
        EntityColumn entityId = persistentClass.getEntityId();
        final String query = deleteQueryBuilder.generateQuery(
                persistentClass.getEntityName(),
                entityId,
                entityId.getFieldValue(entity)
        );
        jdbcTemplate.execute(query);
    }
}
