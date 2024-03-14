package hibernate.entity.meta;

import java.util.HashMap;
import java.util.Map;

public class EntityBindingMap {

    private final Map<String, PersistentClass> entityBindingMap = new HashMap<>();


    public void entityBinding(PersistentClass persistentClass) {
        final String entityName = persistentClass.getEntityName();

        if (entityBindingMap.containsKey(entityName)) {
            throw new DuplicateMappingException(DuplicateMappingException.Type.ENTITY, entityName);
        }


        entityBindingMap.put(entityName, persistentClass);
    }

    public PersistentClass getPersistentClass(String entityName) {
        return entityBindingMap.get(entityName);
    }


}
