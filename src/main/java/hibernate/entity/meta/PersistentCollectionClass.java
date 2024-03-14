package hibernate.entity.meta;

import hibernate.entity.meta.column.EntityColumns;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.OneToMany;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.Arrays;

public class PersistentCollectionClass<T> {

    private final PersistentClass owner;
    private final String collectionEntityName;
    private final EntityColumns entityColumns;
    private final Class<T> clazz;
    private final boolean lazy;

    public PersistentCollectionClass(PersistentClass owner, Class<T> clazz) {
        if (!clazz.isAnnotationPresent(Entity.class)) {
            throw new IllegalArgumentException("Entity 어노테이션이 없는 클래스는 입력될 수 없습니다.");
        }

        this.owner = owner;
        this.collectionEntityName = new EntityNameExtractor(clazz).getEntityName();
        this.entityColumns = new EntityColumns(clazz.getDeclaredFields());
        this.clazz = clazz;
        this.lazy = isLazy(clazz.getDeclaredFields());
    }

    public PersistentClass getOwner() {
        return owner;
    }

    public String getCollectionEntityName() {
        return collectionEntityName;
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

    private boolean isLazy(Field[] fields) {
        return Arrays.stream(fields)
                .filter(it -> it.isAnnotationPresent(OneToMany.class))
                .anyMatch(it -> it.getDeclaredAnnotation(OneToMany.class).fetch() == FetchType.LAZY);
    }

    public EntityColumns getEntityColumns() {
        return entityColumns;
    }
}
