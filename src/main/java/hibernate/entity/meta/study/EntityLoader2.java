package hibernate.entity.meta.study;

import hibernate.dml.SelectQueryBuilder;
import hibernate.entity.EntityCollectionLoader;
import hibernate.entity.meta.CollectionBindingMap;
import hibernate.entity.meta.EntityNameExtractor;
import hibernate.entity.meta.PersistentClass;
import jakarta.persistence.FetchType;
import jakarta.persistence.OneToMany;
import jdbc.JdbcTemplate;
import jdbc.ReflectionRowMapper;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

public class EntityLoader2 {
    private final JdbcTemplate jdbcTemplate;
    private final EntityCollectionLoader2 entityCollectionLoader2;
    private final SelectQueryBuilder selectQueryBuilder = new SelectQueryBuilder();

    public EntityLoader2(JdbcTemplate jdbcTemplate, EntityCollectionLoader2 entityCollectionLoader2) {
        this.jdbcTemplate = jdbcTemplate;
        this.entityCollectionLoader2 = entityCollectionLoader2;
    }

    public <T> T find(final PersistentClass<T> persistentClass, final Object id, PersistentCollectionClass2 persistentCollectionClass2) {
        T instance = getInstance(persistentClass, id);

        Field[] fields = persistentClass.getFields();
        boolean eager = Arrays.stream(fields)
                .filter(it -> it.isAnnotationPresent(OneToMany.class))
                .anyMatch(it -> it.getDeclaredAnnotation(OneToMany.class).fetch() == FetchType.EAGER);

        if (eager) {
            //persistentCollectionClass2.getOwner() == persistentClass
            persistentCollectionClass2.getEntityName();
            persistentCollectionClass2.getEntityColumns();
            // order.id, order.name, order_item.id
        } else {

        }

        return instance;
    }

    public <T> List<T> findAll(final PersistentClass<T> persistentClass) {
        final String query = selectQueryBuilder.generateAllQuery(persistentClass.getEntityName(), persistentClass.getFieldNames());
        return jdbcTemplate.query(query, new ReflectionRowMapper<>(persistentClass));
    }

    private <T> T getInstance(PersistentClass<T> persistentClass, Object id) {
        return queryOnlyEntity(persistentClass, id);
    }

    private <T> T queryOnlyEntity(PersistentClass<T> persistentClass, Object id) {
        final String query = selectQueryBuilder.generateQuery(
                persistentClass.getEntityName(),
                persistentClass.getFieldNames(),
                persistentClass.getEntityId(),
                id
        );
        return jdbcTemplate.queryForObject(query, new ReflectionRowMapper<T>(persistentClass));
    }
}
