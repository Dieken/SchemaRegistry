package schema.registry;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.twitter.common.io.ThriftCodec;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftSchemaRegistry extends AbstractSchemaRegistry {

    public static String TYPE = "thrift";
    private static Logger logger = LoggerFactory.getLogger(ThriftSchemaRegistry.class);
    private static MappingJsonFactory jsonFactory = new MappingJsonFactory();
    private Map<String, Map<String, Class<? extends TBase>>> thriftClasses = new HashMap<>();

    public ThriftSchemaRegistry(Map<String, SchemaInfo> schemas, File rootDirectory)
            throws IOException, ClassNotFoundException {
        super(schemas, rootDirectory);

        loadClasses();
    }

    @Override
    public void serialize(String id, String messageName, InputStream in, OutputStream out,
            Map<String, String[]> parameters) throws IOException {
        // thrift's TJsonProtocol is very picky on JSON data
        ByteArrayOutputStream compactJsonOut = new ByteArrayOutputStream();

        try (JsonParser parser = jsonFactory.createParser(in);
                JsonGenerator generator = jsonFactory.createGenerator(compactJsonOut)) {
            generator.writeTree(parser.readValueAsTree());
        }

        in = new ByteArrayInputStream(compactJsonOut.toByteArray());

        Class<? extends TBase> c = getThriftClass(id, messageName);

        ThriftCodec jsonDecoder = ThriftCodec.create(c, ThriftCodec.JSON_PROTOCOL);
        ThriftCodec binaryEncoder = ThriftCodec.create(c, getBinaryCodec(parameters));
        binaryEncoder.serialize(jsonDecoder.deserialize(in), out);
    }

    @Override
    public void deserialize(String id, String messageName, InputStream in, OutputStream out,
            Map<String, String[]> parameters) throws IOException {
        Class<? extends TBase> c = getThriftClass(id, messageName);

        ThriftCodec binaryDecoder = ThriftCodec.create(c, getBinaryCodec(parameters));
        ThriftCodec jsonEncoder = ThriftCodec.create(c, ThriftCodec.JSON_PROTOCOL);
        jsonEncoder.serialize(binaryDecoder.deserialize(in), out);
    }

    private void loadClasses() throws IOException, ClassNotFoundException {
        for (Map.Entry<String, SchemaInfo> e : schemas.entrySet()) {
            String id = e.getKey();
            SchemaInfo schema = e.getValue();

            if (TYPE.equals(schema.getType())) {
                loadClass(id);
            }
        }
    }

    private void loadClass(String id) throws IOException, ClassNotFoundException {
        Map<String, Class<? extends TBase>> classes = thriftClasses.get(id);
        if (classes == null) {
            classes = new HashMap<>();
            thriftClasses.put(id, classes);
        }

        List<String> dependencies = getAllDependencies(id);
        URL[] urls = new URL[1 + dependencies.size()];
        urls[0] = getSchemaDirectory(id);
        for (int i = 0; i < dependencies.size(); ++i) {
            urls[i + 1] = getSchemaDirectory(dependencies.get(i));
        }

        URLClassLoader cl = new URLClassLoader(urls, getClass().getClassLoader());

        File f = new File(rootDirectory, id + "/CLASSNAME");
        try (FileInputStream in = new FileInputStream(f);
                BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
            String classname;

            while (null != (classname = br.readLine())) {
                logger.debug("loading class {} for schema ID {}", classname, id);

                Class c = cl.loadClass(classname);
                if (!TBase.class.isAssignableFrom(c)) {
                    logger.warn("bad class {} found for schema ID {}, not implements TBase",
                            classname, id);
                    continue;
                }

                classes.put(c.getSimpleName(), c);
                schemas.get(id).getMessages().add(c.getSimpleName());
            }
        }

        if (classes.isEmpty()) {
            thriftClasses.remove(id);
            logger.warn("no thrift class found for schema ID {}", id);
        } else {
            if (classes.size() == 1) {
                schemas.get(id).setDefaultMessage(classes.keySet().iterator().next());
                return;
            }

            int n = 0;
            String nonTExceptionClassname = null;

            for (Map.Entry<String, Class<? extends TBase>> e : classes.entrySet()) {
                String name = e.getKey();
                Class<? extends TBase> c = e.getValue();

                if (!TException.class.isAssignableFrom(c)) {
                    if (++n > 1) {
                        break;
                    }
                    nonTExceptionClassname = name;
                }
            }

            if (n == 1) {
                schemas.get(id).setDefaultMessage(nonTExceptionClassname);
            }
        }
    }

    private Class<? extends TBase> getThriftClass(String id, String messageName) {
        Map<String, Class<? extends TBase>> classes = thriftClasses.get(id);
        if (classes == null) {
            throw new IllegalArgumentException("unknown schema");
        }

        if (messageName == null || messageName.isEmpty()) {
            messageName = schemas.get(id).getDefaultMessage();
        }

        Class<? extends TBase> c;
        if (messageName != null) {
            c = classes.get(messageName);
        } else {
            c = null;
        }

        if (c == null) {
            throw new IllegalArgumentException("unknown message name, known names are: "
                    + Joiner.on(", ").join(schemas.get(id).getMessages()));
        }

        return c;
    }

    private Function<TTransport, TProtocol> getBinaryCodec(Map<String, String[]> parameters) {
        return "compact".equalsIgnoreCase(ParameterUtil.getParameter(parameters, "thrift.protocol"))
                ? ThriftCodec.COMPACT_PROTOCOL
                : ThriftCodec.BINARY_PROTOCOL;
    }
}
