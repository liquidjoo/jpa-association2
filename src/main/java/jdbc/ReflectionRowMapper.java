package jdbc;

import hibernate.entity.meta.PersistentClass;
import hibernate.entity.meta.PersistentCollectionClass;
import hibernate.entity.meta.column.EntityColumn;
import hibernate.entity.meta.column.EntityColumns;
import jakarta.persistence.OneToMany;

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ReflectionRowMapper<T> implements RowMapper<T> {

    private final PersistentClass<T> persistentClass;
    private final PersistentCollectionClass<T> persistentCollectionClass;

    public ReflectionRowMapper(PersistentClass<T> persistentClass, PersistentCollectionClass<T> persistentCollectionClass) {
        this.persistentClass = persistentClass;
        this.persistentCollectionClass = persistentCollectionClass;
    }

    public ReflectionRowMapper(PersistentClass<T> persistentClass) {
        this.persistentClass = persistentClass;
        this.persistentCollectionClass = null;
    }

    public ReflectionRowMapper(PersistentCollectionClass<T> persistentCollectionClass) {
        this.persistentClass = null;
        this.persistentCollectionClass = persistentCollectionClass;
    }

    @Override
    public T mapRow(final ResultSet resultSet) throws SQLException {

        if (persistentClass != null && persistentCollectionClass == null) {
            T instance = persistentClass.newInstance();
            List<EntityColumn> entityColumns = persistentClass.getEntityColumns();
            setEachColumn(entityColumns, instance, resultSet, persistentClass.getEntityName());
            return instance;
        }

        if (persistentCollectionClass != null && persistentClass != null) {
            T instance = persistentClass.newInstance();
            List<EntityColumn> entityColumns = persistentClass.getEntityColumns();
            setEachColumn(entityColumns, instance, resultSet, persistentClass.getEntityName());

            do {
                Field field = Arrays.stream(persistentClass.getFields())
                        .filter(it -> it.isAnnotationPresent(OneToMany.class))
                        .findAny()
                        .orElseThrow();

                Object collectionInstance = persistentCollectionClass.newInstance();
                List<EntityColumn> values = persistentCollectionClass.getEntityColumns().getValues();

                setEachColumn(values, collectionInstance, resultSet, persistentCollectionClass.getCollectionEntityName());
                addFieldValue(field, instance, collectionInstance);
            } while (resultSet.next());

            return instance;
        }

        if (persistentClass == null && persistentCollectionClass != null) {
            T instance = persistentCollectionClass.newInstance();
            List<EntityColumn> entityColumns = persistentCollectionClass.getEntityColumns().getValues();
            setEachColumn(entityColumns, instance, resultSet, persistentCollectionClass.getCollectionEntityName());
            return instance;
        }

        return null;
    }


    private void setEachColumn(
            final List<EntityColumn> subEntityColumns,
            final Object subInstance,
            final ResultSet resultSet,
            final String eagerJoinClass) throws SQLException {
        for (EntityColumn subEntityColumn : subEntityColumns) {
            subEntityColumn.assignFieldValue(subInstance, resultSet.getObject(eagerJoinClass + "." + subEntityColumn.getFieldName()));
        }
    }

    public void addFieldValue(final Field field, final Object entity, final Object value) {
        try {
            field.setAccessible(true);
            List<Object> objects = (List<Object>) field.get(entity);
            if (objects == null) {
                objects = new ArrayList<>();
                objects.add(value);
                field.set(entity, objects);
            } else {
                objects.add(value);
            }
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("필드값에 접근할 수 없습니다.");
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("Entity 객체에 일치하는 필드값이 없습니다.");
        } finally {
            field.setAccessible(false);
        }
    }
}
