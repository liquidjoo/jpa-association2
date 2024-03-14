package repository;

import hibernate.entity.EntityManager;

public class CustomJpaRepository<T, ID> {
    private final EntityManager entityManager;

    public CustomJpaRepository(EntityManager entityManager) {
        this.entityManager = entityManager;
    }

//    public T save(T t) {
//        if (isNewEntity(t)) {
//            return persist(t);
//        }
//        return merge(t);
//    }
//
//    private boolean isNewEntity(T t) {
//        return PersistentClass.getInstance(t.getClass())
//                .getEntityId()
//                .getFieldValue(t) == null;
//    }

    private T persist(T t) {
        entityManager.persist(t);
        return t;
    }

    private T merge(T t) {
        entityManager.merge(t);
        return t;
    }
}
