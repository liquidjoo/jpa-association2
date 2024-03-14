package hibernate.entity.persistencecontext;

import java.util.Objects;

public class EntityKey {

    private final Object id;
    private final String entityName;

    public EntityKey(Object id, String entityName) {
        this.id = id;
        this.entityName = entityName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EntityKey entityKey = (EntityKey) o;
        return Objects.equals(id, entityKey.id) && Objects.equals(entityName, entityKey.entityName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, entityName);
    }
}
