package schema.registry.servlet;

import java.io.File;
import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.io.Files;

import schema.registry.AttachmentUtil;
import schema.registry.SchemaInfo;
import schema.registry.SchemaRegistry;

@WebServlet(name = "SchemaServlet", urlPatterns = {"/s/*"})
public class SchemaServlet extends HttpServlet {

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        SchemaRegistry registry = (SchemaRegistry) request.getServletContext()
                .getAttribute(SchemaRegistryServletContextListener.SCHEMA_REGISTRY);

        response.setContentType("text/plain; charset=utf-8");
        try (ServletOutputStream out = response.getOutputStream()) {
            String id = request.getPathInfo();
            if (id == null || id.equals("/")) {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                out.println("schema ID isn't specified in URI path");
                return;
            }

            id = id.substring(1);
            SchemaInfo schema = registry.getSchemas().get(id);
            if (schema == null) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                out.println("schema ID isn't found");
                return;
            }

            File f = new File(registry.getRootDirectory(), id + "/" + schema.getFilename() + ".orig");
            if (!f.exists()) {
                f = new File(registry.getRootDirectory(), id + "/" + schema.getFilename());
            }

            if (!f.exists()) {
                response.setStatus(HttpServletResponse.SC_NOT_FOUND);
                out.println("schema is missing");
                return;
            }

            if (!f.canRead()) {
                response.setStatus(HttpServletResponse.SC_FORBIDDEN);
                out.println("file permission forbidden");
                return;
            }

            AttachmentUtil.attach(request, response, id + "_" + schema.getFilename());

            Files.copy(f, out);
        }
    }
}
