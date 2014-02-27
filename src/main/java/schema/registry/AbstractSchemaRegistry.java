package schema.registry;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public abstract class AbstractSchemaRegistry implements SchemaRegistry {

    protected Map<String, SchemaInfo> schemas;
    protected File rootDirectory;

    public AbstractSchemaRegistry(Map<String, SchemaInfo> schemas, File rootDirectory) {
        this.schemas = schemas;
        this.rootDirectory = rootDirectory;
    }

    @Override
    public File getRootDirectory() {
        return rootDirectory;
    }

    @Override
    public Map<String, SchemaInfo> getSchemas() {
        return schemas;
    }

    public List<String> getAllDependencies(String id) {
        Set<String> result = new TreeSet<>();

        List<String> dependencies = schemas.get(id).getDependencies();
        for (String d : dependencies) {
            result.add(d);

            for (String d2 : getAllDependencies(d)) {
                result.add(d2);
            }
        }

        return Arrays.asList(result.toArray(new String[0]));
    }

    protected URL getSchemaDirectory(String id) throws MalformedURLException, IOException {
        return new URL("file:" + rootDirectory.getCanonicalPath() + "/" + id + "/");
    }
}
