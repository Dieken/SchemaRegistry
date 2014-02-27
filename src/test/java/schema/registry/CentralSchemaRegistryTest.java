package schema.registry;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CentralSchemaRegistryTest {

    private static Logger logger = LoggerFactory.getLogger(CentralSchemaRegistryTest.class);

    @Test
    public void testIt() throws IOException, ClassNotFoundException {
        CentralSchemaRegistry registry = new CentralSchemaRegistry(new File("schemas.json"), new File("generated"));

        String person = "name: \"my 中文name\"\nid: 1\n";

        Assert.assertEquals(
                query(registry,
                "protobuf-example-addressbook",
                "Person",
                "id:       1\nname:     \"my 中文name\""),
                person);
    }

    private String query(SchemaRegistry registry, String id, String messageName,
            String text, String filters)
            throws IOException {
        ByteArrayInputStream in = new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try (OutputStream filterOut = FilterStreamUtil.filter(out, filters)) {
            registry.serialize(id, messageName, in, filterOut,
                    new HashMap<String, String[]>());
            filterOut.flush();
        }

        try (InputStream filterIn = FilterStreamUtil.filter(
                new ByteArrayInputStream(out.toByteArray()), filters)) {
            out.reset();
            registry.deserialize(id, messageName, filterIn, out,
                    new HashMap<String, String[]>());
            String s = new String(out.toString("UTF-8"));

            logger.debug("schema id={}, message name={}, text={}, \ngot {}",
                    id, messageName, text, s);
            return s;
        }
    }

    private String query(SchemaRegistry registry, String id, String messageName,
            String text) throws IOException {
        return query(registry, id, messageName, text, null);
    }
}
