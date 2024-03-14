package hibernate.entity.meta;

public class DuplicateMappingException extends RuntimeException {
    private final String name;
    private final String type;
    public DuplicateMappingException(Type type, String name) {
        this(type.text, name);
    }

    @Deprecated
    public DuplicateMappingException(String type, String name) {
        this("Duplicate " + type + " mapping " + name, type, name);
    }

    public DuplicateMappingException(String customMessage, String type, String name) {
        super(customMessage);
        this.type = type;
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    /**
     * Enumeration of the types of things that can be duplicated.
     */
    public enum Type {
        ENTITY("entity"),
        COLLECTION("collection"),
        TABLE("table"),
        PROPERTY("property"),
        COLUMN("column"),
        COLUMN_BINDING("column-binding"),
        NAMED_ENTITY_GRAPH("NamedEntityGraph"),
        QUERY("query"),
        RESULT_SET_MAPPING("ResultSetMapping"),
        PROCEDURE("NamedStoredProcedureQuery");

        private final String text;

        Type(String text) {
            this.text = text;
        }
    }
}