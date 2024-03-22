package hibernate.entity.meta.study;

import hibernate.entity.meta.column.EntityColumns;

public class PersistentCollectionClass2<T> {

    private final PersistentClass2 owner;
    private final String entityName;
    private final EntityColumns entityColumns;
    private final Class<T> clazz;

    public PersistentCollectionClass2(PersistentClass2 owner, String entityName, EntityColumns entityColumns, Class<T> clazz) {
        this.owner = owner;
        this.entityName = entityName;
        this.entityColumns = entityColumns;
        this.clazz = clazz;
    }

    public PersistentClass2 getOwner() {
        return owner;
    }

    public String getEntityName() {
        return entityName;
    }

    public EntityColumns getEntityColumns() {
        return entityColumns;
    }

    public Class<T> getClazz() {
        return clazz;
    }
}
