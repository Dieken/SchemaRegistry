package schema.registry.servlet;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.fasterxml.jackson.databind.ObjectMapper;

import schema.registry.AttachmentUtil;
import schema.registry.SchemaInfo;
import schema.registry.SchemaRegistry;

@WebServlet(name = "SchemaInfoServlet", urlPatterns = {"/i/*"})
public class SchemaInfoServlet extends HttpServlet {

    private static ObjectMapper mapper = new ObjectMapper();

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        SchemaRegistry registry = (SchemaRegistry) request.getServletContext()
                .getAttribute(SchemaRegistryServletContextListener.SCHEMA_REGISTRY);

        response.setContentType("application/json; charset=utf-8");
        try (ServletOutputStream out = response.getOutputStream()) {
            String id = request.getPathInfo();
            if (id == null || id.equals("/")) {
                AttachmentUtil.attach(request, response, "all-schemas.json");
                mapper.writeValue(out, registry.getSchemas());
                return;
            }

            id = id.substring(1);
            SchemaInfo schema = registry.getSchemas().get(id);
            if (schema == null) {
                response.setContentType("text/plain; charset=utf-8");
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                out.println("schema ID isn't found");
                return;
            }

            AttachmentUtil.attach(request, response, id + ".json");
            mapper.writeValue(out, schema);
        }
    }
}
