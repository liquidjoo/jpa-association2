package hibernate.entity.meta;

import jakarta.persistence.Table;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public class EntityNameExtractor {

    private final String entityName;

    public EntityNameExtractor(final Class<?> clazz) {
        this.entityName = setEntityName(clazz);
    }

    public EntityNameExtractor(final Type type) {
        this.entityName = setEntityName(collectionClass(type));
    }

    private String setEntityName(final Class<?> clazz) {
        if (!clazz.isAnnotationPresent(Table.class)) {
            return clazz.getSimpleName();
        }
        String entityName = clazz.getAnnotation(Table.class).name();
        if (entityName.isEmpty()) {
            return clazz.getSimpleName();
        }
        return entityName;
    }

    private Class<?> collectionClass(Type type) {
        if (type instanceof ParameterizedType) {
            ParameterizedType parameterizedType = (ParameterizedType) type;
            Type[] typeArguments = parameterizedType.getActualTypeArguments();

            if (typeArguments != null && typeArguments.length > 0) {
                Type typeArgument = typeArguments[0];
                try {
                    return Class.forName(typeArgument.getTypeName());
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
        throw new IllegalArgumentException();
    }

    public String getEntityName() {
        return entityName;
    }
}
