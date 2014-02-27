package schema.registry;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

public interface SchemaRegistry {

    public void serialize(String id, String messageName, InputStream in, OutputStream out,
            Map<String, String[]> parameters) throws IOException;

    public void deserialize(String id, String messageName, InputStream in, OutputStream out,
            Map<String, String[]> parameters) throws IOException;

    public File getRootDirectory();

    public Map<String, SchemaInfo> getSchemas();
}
