package hibernate.entity;

import hibernate.entity.meta.EntityBindingMap;
import hibernate.entity.meta.EntityNameExtractor;
import hibernate.entity.meta.PersistentClass;
import hibernate.entity.meta.column.EntityColumn;
import hibernate.entity.persistencecontext.EntityKey;
import hibernate.entity.persistencecontext.EntitySnapshot;
import hibernate.entity.persistencecontext.PersistenceContext;

import java.util.Map;

import static hibernate.Status.*;

public class EntityManagerImpl implements EntityManager {

    private final EntityPersister entityPersister;
    private final EntityLoader entityLoader;
    private final PersistenceContext persistenceContext;
    private final EntityBindingMap entityBindingMap;

    public EntityManagerImpl(
            final EntityPersister entityPersister,
            final EntityLoader entityLoader,
            final PersistenceContext persistenceContext,
            final EntityBindingMap entityBindingMap
    ) {
        this.entityPersister = entityPersister;
        this.entityLoader = entityLoader;
        this.persistenceContext = persistenceContext;
        this.entityBindingMap = entityBindingMap;
    }

    @Override
    public <T> T find(final Class<T> clazz, final Object id) {
        String entityName = new EntityNameExtractor(clazz).getEntityName();
        EntityKey entityKey = new EntityKey(id, entityName);
        Object cachedEntity = persistenceContext.getEntity(entityKey);
        if (cachedEntity != null) {
            return (T) cachedEntity;
        }

        PersistentClass<T> persistentClass = entityBindingMap.getPersistentClass(entityName);
        T loadEntity = entityLoader.find(persistentClass, id);
        persistenceContext.addEntity(id, loadEntity, LOADING);
        return loadEntity;
    }

    @Override
    public void persist(final Object entity) {
        String entityName = new EntityNameExtractor(entity.getClass()).getEntityName();
        PersistentClass persistentClass = entityBindingMap.getPersistentClass(entityName);
        EntityColumn entityId = persistentClass.getEntityId();
        Object id = entityId.getFieldValue(entity);
        if (id == null) {
            persistenceContext.addEntityEntry(entity, SAVING);
            Object generatedId = entityPersister.insert(persistentClass, entity);
            entityId.assignFieldValue(entity, generatedId);
            persistenceContext.addEntity(generatedId, entity);
            return;
        }

        if (persistenceContext.getEntity(new EntityKey(id, entityName)) != null) {
            throw new IllegalStateException("이미 영속화되어있는 entity입니다.");
        }
        persistenceContext.addEntity(id, entity, SAVING);
        entityPersister.insert(persistentClass, entity);
    }

    @Override
    public void merge(final Object entity) {
        String entityName = new EntityNameExtractor(entity.getClass()).getEntityName();
        PersistentClass<?> persistentClass = entityBindingMap.getPersistentClass(entityName);
        Object entityId = getNotNullEntityId(persistentClass, entity);
        Map<EntityColumn, Object> changedColumns = getSnapshot(entity, entityId).changedColumns(entity);
        if (changedColumns.isEmpty()) {
            return;
        }
        persistenceContext.addEntity(entityId, entity);
        entityPersister.update(persistentClass, entityId, changedColumns);
    }

    private Object getNotNullEntityId(final PersistentClass<?> persistentClass, final Object entity) {
        Object entityId = persistentClass.getEntityId()
                .getFieldValue(entity);
        if (entityId == null) {
            throw new IllegalStateException("id가 없는 entity는 merge할 수 없습니다.");
        }
        return entityId;
    }

    private EntitySnapshot getSnapshot(final Object entity, final Object entityId) {
        String entityName = new EntityNameExtractor(entity.getClass()).getEntityName();
        EntityKey entityKey = new EntityKey(entityId, entityName);
        EntitySnapshot snapshot = persistenceContext.getDatabaseSnapshot(entityKey);
        if (snapshot == null) {
            find(entity.getClass(), entityId);
            return persistenceContext.getDatabaseSnapshot(entityKey);
        }
        return snapshot;
    }

    @Override
    public void remove(final Object entity) {
        String entityName = new EntityNameExtractor(entity.getClass()).getEntityName();
        PersistentClass<?> persistentClass = entityBindingMap.getPersistentClass(entityName);
        persistenceContext.addEntityEntry(entity, DELETED);
        entityPersister.delete(persistentClass, entity);
        persistenceContext.removeEntity(entity);
    }
}
