package schema.registry;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CentralSchemaRegistry extends AbstractSchemaRegistry {

    private Map<String, SchemaRegistry> registries = new HashMap<>();

    public CentralSchemaRegistry(File schemaList, File rootDirectory) throws IOException, ClassNotFoundException {
        super(Collections.unmodifiableMap(
                (Map<String, SchemaInfo>) new ObjectMapper().readValue(schemaList,
                new TypeReference<Map<String, SchemaInfo>>() {
        })), rootDirectory);

        registries.put(ProtobufSchemaRegistry.TYPE, new ProtobufSchemaRegistry(schemas, rootDirectory));
        registries.put(AvroSchemaRegistry.TYPE, new AvroSchemaRegistry(schemas, rootDirectory));
        registries.put(ThriftSchemaRegistry.TYPE, new ThriftSchemaRegistry(schemas, rootDirectory));
    }

    @Override
    public void serialize(String id, String messageName, InputStream in, OutputStream out,
            Map<String, String[]> parameters) throws IOException {
        getRegistry(id).serialize(id, messageName, in, out, parameters);
    }

    @Override
    public void deserialize(String id, String messageName, InputStream in, OutputStream out,
            Map<String, String[]> parameters) throws IOException {
        getRegistry(id).deserialize(id, messageName, in, out, parameters);
    }

    private SchemaRegistry getRegistry(String id) {
        SchemaInfo schema = schemas.get(id);
        if (schema == null) {
            throw new IllegalArgumentException("unknown schema");
        }

        SchemaRegistry registry = registries.get(schema.getType());
        if (registry == null) {
            throw new IllegalArgumentException("unknown schema type");
        }

        return registry;
    }
}
