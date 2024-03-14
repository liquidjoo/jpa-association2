package hibernate.entity;

import hibernate.dml.SelectQueryBuilder;
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

public class EntityLoader {

    private final JdbcTemplate jdbcTemplate;
    private final CollectionBindingMap collectionBindingMap;
    private final SelectQueryBuilder selectQueryBuilder = new SelectQueryBuilder();

    public EntityLoader(JdbcTemplate jdbcTemplate, CollectionBindingMap collectionBindingMap) {
        this.jdbcTemplate = jdbcTemplate;
        this.collectionBindingMap = collectionBindingMap;
    }

    public <T> T find(final PersistentClass<T> persistentClass, final Object id) {
        T instance = getInstance(persistentClass, id);

        Field[] fields = persistentClass.getFields();
        boolean lazy = Arrays.stream(fields)
                .filter(it -> it.isAnnotationPresent(OneToMany.class))
                .anyMatch(it -> it.getDeclaredAnnotation(OneToMany.class).fetch() == FetchType.LAZY);

        if (lazy) {
            Field field = Arrays.stream(fields)
                    .filter(it -> it.isAnnotationPresent(OneToMany.class))
                    .findAny()
                    .orElseThrow();

            EntityNameExtractor entityNameExtractor = new EntityNameExtractor(field.getGenericType());

            EntityCollectionLoader entityCollectionLoader = new EntityCollectionLoader(jdbcTemplate);
            instance = entityCollectionLoader.lazyJoinColumns(collectionBindingMap.getCollection(entityNameExtractor.getEntityName()), instance);
        }

        return instance;
    }

    public <T> List<T> findAll(final PersistentClass<T> persistentClass) {
        final String query = selectQueryBuilder.generateAllQuery(persistentClass.getEntityName(), persistentClass.getFieldNames());
        return jdbcTemplate.query(query, new ReflectionRowMapper<>(persistentClass));
    }

    private <T> T getInstance(PersistentClass<T> persistentClass, Object id) {
        Field[] fields = persistentClass.getFields();
        boolean lazy = Arrays.stream(fields)
                .filter(it -> it.isAnnotationPresent(OneToMany.class))
                .anyMatch(it -> it.getDeclaredAnnotation(OneToMany.class).fetch() == FetchType.LAZY);


        boolean match = Arrays.stream(fields)
                .anyMatch(it -> it.isAnnotationPresent(OneToMany.class));

        if (lazy || !match) {
            return queryOnlyEntity(persistentClass, id);
        }

        Field field = Arrays.stream(fields)
                .filter(it -> it.isAnnotationPresent(OneToMany.class))
                .findAny()
                .orElseThrow();

        EntityNameExtractor entityNameExtractor = new EntityNameExtractor(field.getGenericType());
        EntityCollectionLoader entityCollectionLoader = new EntityCollectionLoader(jdbcTemplate);
        return entityCollectionLoader.queryWithEagerColumn(persistentClass, id, collectionBindingMap.getCollection(entityNameExtractor.getEntityName()));
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
