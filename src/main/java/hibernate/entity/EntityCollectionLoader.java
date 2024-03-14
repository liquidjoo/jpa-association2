package hibernate.entity;

import hibernate.dml.SelectQueryBuilder;
import hibernate.entity.collection.PersistentList;
import hibernate.entity.meta.PersistentClass;
import hibernate.entity.meta.PersistentCollectionClass;
import jdbc.JdbcTemplate;
import jdbc.ReflectionRowMapper;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.LazyLoader;

import java.util.List;

public class EntityCollectionLoader {

    private final JdbcTemplate jdbcTemplate;
    private final SelectQueryBuilder selectQueryBuilder = new SelectQueryBuilder();

    public EntityCollectionLoader(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }


    public <T> List<T> findAll(final PersistentCollectionClass<T> persistentCollectionClass) {
        final String query = selectQueryBuilder.generateAllQuery(persistentCollectionClass.getCollectionEntityName(), persistentCollectionClass.getEntityColumns().getFieldNames());
        return jdbcTemplate.query(query, new ReflectionRowMapper<>(persistentCollectionClass.getOwner(), persistentCollectionClass));
    }

    public <T> T queryWithEagerColumn(PersistentClass<T> persistentClass, Object id, PersistentCollectionClass collectionClass) {
        final String query = selectQueryBuilder.generateQuery(
                persistentClass.getEntityName(),
                persistentClass.getFieldNames(),
                persistentClass.getEntityId(),
                id,
                true,
                collectionClass.getCollectionEntityName(),
                collectionClass.getEntityColumns().getFieldNames()
        );
        return jdbcTemplate.queryForObject(query, new ReflectionRowMapper<T>(persistentClass, collectionClass));
    }


    private <T> T queryOnlyEntity(final PersistentCollectionClass<T> persistentCollectionClass, Object id) {
        final String query = selectQueryBuilder.generateQuery(
                persistentCollectionClass.getCollectionEntityName(),
                persistentCollectionClass.getEntityColumns().getFieldNames(),
                persistentCollectionClass.getEntityColumns().getEntityId(),
                id
        );
        return (T) jdbcTemplate.queryForObject(query, new ReflectionRowMapper<>(persistentCollectionClass.getOwner(), persistentCollectionClass));
    }

    public <T> T lazyJoinColumns(PersistentCollectionClass persistentCollectionClass, T instance) {

        Enhancer enhancer = generateEnhancer(persistentCollectionClass);
        return instance;
    }

    private <T> Enhancer generateEnhancer(PersistentCollectionClass<T> persistentCollectionClass) {
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(List.class);
        enhancer.setCallback((LazyLoader) () -> new PersistentList<>(persistentCollectionClass, EntityCollectionLoader.this));
        return enhancer;
    }
}
