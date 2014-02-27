package schema.registry;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.protobuf.Message;
import com.google.protobuf.TextFormat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Protobuf supports serialization/deserialization without code generation,
// but the API isn't as convenient as generated Java classes.
//
// $ protoc -I... -o sealed.pb --include_imports --include_source_info some.proto
//
// Possible Java code (not verified):
//  FileDescriptorProto proto = FileDescriptorProto.parseFrom(new FileInputStream("sealed.pb"));
//  FileDescriptor descriptor = FileDescriptor.buildFrom(proto, new FileDescriptor[0]);
//  DynamicMessage.Builder builder = DynamicMessage.newBuilder(descriptor.findMessageTypeByName("MyMessage"));
//      or:  DynamicMessage msg = DynamicMessage.parseFrom(descriptor.findMessageTypeByName("MyMessage"), ...);
//
public class ProtobufSchemaRegistry extends AbstractSchemaRegistry {

    public static String TYPE = "protobuf";
    private static Logger logger = LoggerFactory.getLogger(ProtobufSchemaRegistry.class);
    private Map<String, Map<String, Method>> newBuilderMethods = new HashMap<>();

    static {
        try {
            // XXX: dirty hack for protobuf-2.5.0 before it has TextFormat.printUnicode(...)
            Field printer = TextFormat.class.getDeclaredField("DEFAULT_PRINTER");
            printer.setAccessible(true);
            Method setEscapeNonAscii = printer.getType().getDeclaredMethod("setEscapeNonAscii", boolean.class);
            setEscapeNonAscii.setAccessible(true);
            setEscapeNonAscii.invoke(printer.get(TextFormat.class), false);
        } catch (IllegalAccessException | IllegalArgumentException |
                InvocationTargetException | NoSuchFieldException |
                NoSuchMethodException | SecurityException ex) {
            logger.error("fail to hack protobuf class TextFormat", ex);
        }
    }

    public ProtobufSchemaRegistry(Map<String, SchemaInfo> schemas, File rootDirectory)
            throws IOException, ClassNotFoundException {
        super(schemas, rootDirectory);
        loadClasses();
    }

    // XXX: http://code.google.com/p/protobuf/wiki/ThirdPartyAddOns, encode from and decode to JSON??
    // * http://code.google.com/p/protobuf-java-format/, not support stream API
    // * https://github.com/sijuv/protobuf-codec, seems dead
    @Override
    public void serialize(String id, String messageName, InputStream in, OutputStream out,
            Map<String, String[]> parameters) throws IOException {
        Message.Builder builder = getBuilder(id, messageName);

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {
            if (isDelimitedMessages(parameters)) {
                StringBuilder sb = new StringBuilder();
                String s;

                while (null != (s = reader.readLine())) {
                    s = s.trim();
                    if (s.isEmpty()) {
                        if (sb.length() > 0) {
                            TextFormat.merge(sb, builder);
                            builder.build().writeDelimitedTo(out);
                            builder.clear();
                            sb = new StringBuilder();
                        }
                    } else {
                        sb.append(s).append('\n');
                    }
                }

                if (sb.length() > 0) {
                    TextFormat.merge(sb, builder);
                    builder.build().writeDelimitedTo(out);
                }
            } else {
                TextFormat.merge(reader, builder);
                builder.build().writeTo(out);
            }
        }
    }

    @Override
    public void deserialize(String id, String messageName, InputStream in, OutputStream out,
            Map<String, String[]> parameters) throws IOException {
        Message.Builder builder = getBuilder(id, messageName);

        try (OutputStreamWriter writer = new OutputStreamWriter(out, StandardCharsets.UTF_8)) {
            if (isDelimitedMessages(parameters)) {
                while (builder.mergeDelimitedFrom(in)) {
                    Message msg = builder.build();
                    builder.clear();
                    TextFormat.print(msg, writer);
                    writer.append('\n');
                }
            } else {
                Message msg = builder.mergeFrom(in).build();
                TextFormat.print(msg, writer);
            }
        }
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
            String classname = br.readLine();
            Class c = cl.loadClass(classname);

            findNewBuilderMethod(id, c);
            findDefaultMessageName(id, c);
        }
    }

    private void findNewBuilderMethod(String id, Class c) {
        Map<String, Method> methods = newBuilderMethods.get(id);
        if (methods == null) {
            methods = new HashMap<>();
            newBuilderMethods.put(id, methods);
        }

        Class[] innerClasses = c.getClasses();
        for (Class ic : innerClasses) {
            if (isInnerMessageClass(c, ic)) {
                logger.debug("find protobuf message class {}(simple name: {}) for schema {}",
                        ic.getName(), ic.getSimpleName(), id);

                try {
                    methods.put(ic.getSimpleName(), ic.getMethod("newBuilder"));

                    schemas.get(id).getMessages().add(ic.getSimpleName());

                    findNewBuilderMethod(id, ic);
                } catch (NoSuchMethodException ex) {
                    logger.error("can't find newBuilder() method in generated protobuf class "
                            + ic.getName(), ex);
                }
            }
        }

        if (methods.isEmpty()) {
            logger.error("can't find protobuf message class for schema {}", id);
            newBuilderMethods.remove(id);
        }
    }

    private void findDefaultMessageName(String id, Class c) {
        Class[] innerClasses = c.getClasses();
        String messageName = null;
        int n = 0;

        for (Class ic : innerClasses) {
            if (isInnerMessageClass(c, ic)) {
                ++n;
                messageName = ic.getSimpleName();
            }
        }

        if (n == 1) {
            schemas.get(id).setDefaultMessage(messageName);
        }
    }

    private boolean isInnerMessageClass(Class c, Class inner) {
        if (inner.getEnclosingClass() == c && inner.isMemberClass()
                && Message.class.isAssignableFrom(inner)) {
            return true;
        } else {
            return false;
        }
    }

    private Message.Builder getBuilder(String id, String messageName) throws IOException {
        Map<String, Method> methods = newBuilderMethods.get(id);
        if (methods == null) {
            throw new IllegalArgumentException("unknown schema");
        }

        if ((messageName == null || messageName.isEmpty())) {
            messageName = schemas.get(id).getDefaultMessage();
        }

        Method m;
        if (messageName != null) {
            m = methods.get(messageName);
        } else {
            m = null;
        }

        if (m == null) {
            throw new IllegalArgumentException("unknown message name, known names are: "
                    + Joiner.on(", ").join(schemas.get(id).getMessages()));
        }

        try {
            return (Message.Builder) m.invoke(m.getDeclaringClass());
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException ex) {
            logger.error("fail to call {}.newBuilder() for schema {}", messageName, id);
            throw new IOException(ex);
        }
    }

    private boolean isDelimitedMessages(Map<String, String[]> parameters) {
        return "true".equalsIgnoreCase(ParameterUtil.getParameter(parameters, "protobuf.delimited"));
    }
}
