package hibernate.entity.persistencecontext;

import hibernate.EntityEntry;
import hibernate.Status;
import hibernate.entity.meta.EntityBindingMap;
import hibernate.entity.meta.EntityNameExtractor;
import hibernate.entity.meta.PersistentClass;

import java.util.HashMap;
import java.util.Map;

public class SimplePersistenceContext implements PersistenceContext {

    private final Map<EntityKey, Object> entities = new HashMap<>();
    private final Map<EntityKey, EntitySnapshot> snapshotEntities = new HashMap<>();
    private final Map<Object, EntityEntry> entityEntries = new HashMap<>();
    private final EntityBindingMap entityBindingMap;

    public SimplePersistenceContext(EntityBindingMap entityBindingMap) {
        this.entityBindingMap = entityBindingMap;
    }

    @Override
    public Object getEntity(final EntityKey id) {
        return entities.get(id);
    }

    @Override
    public void addEntity(final Object id, final Object entity) {
        addEntity(id, entity, Status.MANAGED);
    }

    @Override
    public void addEntity(Object id, Object entity, Status status) {
        String entityName = new EntityNameExtractor(entity.getClass()).getEntityName();
        PersistentClass persistentClass = entityBindingMap.getPersistentClass(entityName);
        entityEntries.put(entity, new EntityEntry(status));
        entities.put(new EntityKey(id, persistentClass.getEntityName()), entity);
        snapshotEntities.put(new EntityKey(id, persistentClass.getEntityName()), new EntitySnapshot(persistentClass, entity));
        entityEntries.put(entity, new EntityEntry(Status.MANAGED));
    }

    @Override
    public void addEntityEntry(Object entity, Status status) {
        entityEntries.put(entity, new EntityEntry(status));
    }

    @Override
    public void removeEntity(final Object entity) {
        EntityKey entityKey = entities.entrySet()
                .stream()
                .filter(entry -> entry.getValue().equals(entity))
                .findAny()
                .map(Map.Entry::getKey)
                .orElseThrow(() -> new IllegalStateException("영속화되어있지 않은 entity입니다."));

        EntityEntry entityEntry = entityEntries.get(entity);
        entityEntry.updateStatus(Status.GONE);
        entities.remove(entityKey);
        snapshotEntities.remove(entityKey);
    }

    @Override
    public EntitySnapshot getDatabaseSnapshot(final EntityKey id) {
        return snapshotEntities.get(id);
    }
}
