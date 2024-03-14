package hibernate.entity.meta;

import hibernate.entity.meta.column.EntityColumn;
import hibernate.entity.meta.column.EntityColumns;
import jakarta.persistence.Entity;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PersistentClass<T> {

    private final String entityName;
    private final EntityColumns entityColumns;
    private final Class<T> clazz;

    public PersistentClass(final Class<T> clazz) {
        if (!clazz.isAnnotationPresent(Entity.class)) {
            throw new IllegalArgumentException("Entity 어노테이션이 없는 클래스는 입력될 수 없습니다.");
        }
        this.entityName = new EntityNameExtractor(clazz).getEntityName();
        this.entityColumns = new EntityColumns(clazz.getDeclaredFields());
        this.clazz = clazz;
    }

    public T newInstance() {
        Constructor<T> constructor = getConstructor();
        try {
            constructor.setAccessible(true);
            return constructor.newInstance();
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("생성자에 접근할 수 없습니다.");
        } catch (Exception e) {
            throw new IllegalStateException("생성자 생성에 문제가 발생했습니다.", e);
        } finally {
            constructor.setAccessible(false);
        }
    }

    private Constructor<T> getConstructor() {
        try {
            return clazz.getDeclaredConstructor();
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("기본 생성자가 존재하지 않습니다.");
        }
    }

    public Map<EntityColumn, Object> getFieldValues(final Object entity) {
        return entityColumns.getFieldValues(entity);
    }

    public String getEntityName() {
        return entityName;
    }

    public EntityColumn getEntityId() {
        return entityColumns.getEntityId();
    }

    public List<EntityColumn> getEntityColumns() {
        return entityColumns.getValues();
    }

    public List<String> getFieldNames() {
        return entityColumns.getFieldNames();
    }

    public Field[] getFields() {
        return clazz.getDeclaredFields();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PersistentClass<?> that = (PersistentClass<?>) o;
        return Objects.equals(clazz, that.clazz);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clazz);
    }
}
