package hibernate.entity;

import hibernate.dml.SelectQueryBuilder;
import hibernate.entity.collection.PersistentList;
import hibernate.entity.meta.PersistentClass;
import hibernate.entity.meta.PersistentCollectionClass;
import jakarta.persistence.OneToMany;
import jdbc.JdbcTemplate;
import jdbc.ReflectionRowMapper;
import net.sf.cglib.proxy.Enhancer;
import net.sf.cglib.proxy.LazyLoader;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;

public class EntityCollectionLoader {

    private final JdbcTemplate jdbcTemplate;
    private final SelectQueryBuilder selectQueryBuilder = new SelectQueryBuilder();

    public EntityCollectionLoader(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }


    public <T> List<T> findAll(final PersistentCollectionClass<T> persistentCollectionClass, boolean lazy) {
        final String query = selectQueryBuilder.generateAllQuery(persistentCollectionClass.getCollectionEntityName(), persistentCollectionClass.getEntityColumns().getFieldNames());
        if (lazy) {
            return jdbcTemplate.query(query, new ReflectionRowMapper<>(null, persistentCollectionClass));
        }
        return jdbcTemplate.query(query, new ReflectionRowMapper<>(persistentCollectionClass.getOwner(), persistentCollectionClass));
    }

    public <T> T queryWithEagerColumn(PersistentClass<T> persistentClass, Object id, PersistentCollectionClass collectionClass) {
        final String query = selectQueryBuilder.generateQuery(persistentClass.getEntityName(), persistentClass.getFieldNames(), persistentClass.getEntityId(), id, true, collectionClass.getCollectionEntityName(), collectionClass.getEntityColumns().getFieldNames());
        return jdbcTemplate.queryForObject(query, new ReflectionRowMapper<T>(persistentClass, collectionClass));
    }


    public <T> T lazyJoinColumns(PersistentCollectionClass persistentCollectionClass, T instance) {

        PersistentClass owner = persistentCollectionClass.getOwner();

        Field field = Arrays.stream(owner.getFields()).filter(it -> it.isAnnotationPresent(OneToMany.class)).findAny().orElseThrow();

        Enhancer enhancer = generateEnhancer(persistentCollectionClass);
        addFieldValue(field, instance, enhancer.create());

        return instance;
    }

    private <T> Enhancer generateEnhancer(PersistentCollectionClass<T> persistentCollectionClass) {
        Enhancer enhancer = new Enhancer();
        enhancer.setSuperclass(List.class);
        enhancer.setCallback((LazyLoader) () -> new PersistentList<>(persistentCollectionClass, EntityCollectionLoader.this, true));
        return enhancer;
    }

    public void addFieldValue(final Field field, final Object entity, final Object value) {
        try {
            field.setAccessible(true);
            field.set(entity, value);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("필드값에 접근할 수 없습니다.");
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Entity 객체에 일치하는 필드값이 없습니다.");
        } finally {
            field.setAccessible(false);
        }
    }

}
