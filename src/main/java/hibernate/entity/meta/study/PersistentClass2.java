package hibernate.entity.meta.study;

import hibernate.entity.meta.column.EntityColumns;
import jakarta.persistence.Entity;

public class PersistentClass2<T> {

    private final String entityName;
    private final EntityColumns entityColumns;
    private final Class<T> clazz;

    public PersistentClass2(String entityName, EntityColumns entityColumns, Class<T> clazz) {
        if (clazz.isAnnotationPresent(Entity.class)) {
            throw new IllegalArgumentException("");
        }

        this.entityName = entityName;
        this.entityColumns = entityColumns;
        this.clazz = clazz;
    }


}
