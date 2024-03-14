package hibernate.entity.meta;

import java.util.HashMap;
import java.util.Map;

public class CollectionBindingMap {

    private final Map<String, PersistentCollectionClass> collectionBindingMap = new HashMap<>();

    public void addCollectionBinding(PersistentCollectionClass collection) throws DuplicateMappingException {
        final String collectionRole = collection.getCollectionEntityName();
        if (collectionBindingMap.containsKey(collectionRole)) {
            throw new DuplicateMappingException(DuplicateMappingException.Type.COLLECTION, collectionRole);
        }
        collectionBindingMap.put(collectionRole, collection);
    }

    public PersistentCollectionClass getCollection(String name) {
        return collectionBindingMap.get(name);
    }
}
