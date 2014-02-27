package schema.registry.servlet;

import java.io.IOException;
import java.io.OutputStream;
import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import schema.registry.AttachmentUtil;
import schema.registry.FilterStreamUtil;
import schema.registry.SchemaInfo;
import schema.registry.SchemaRegistry;

@WebServlet(name = "EncodeServlet", urlPatterns = {"/e/*"})
public class EncodeServlet extends HttpServlet {

    protected void processRequest(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        SchemaRegistry registry = (SchemaRegistry) request.getServletContext()
                .getAttribute(SchemaRegistryServletContextListener.SCHEMA_REGISTRY);

        response.setContentType("text/plain; charset=utf-8");
        try (ServletInputStream in = request.getInputStream();
                ServletOutputStream out = response.getOutputStream()) {
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

            OutputStream filterOut = null;
            try {
                response.setContentType("application/octet-stream");
                filterOut = FilterStreamUtil.filter(out, request.getParameter("f"));

                AttachmentUtil.attach(request, response, id + "-" + System.currentTimeMillis() + ".dat");

                registry.serialize(id, request.getParameter("m"), in, filterOut,
                        request.getParameterMap());
            } catch (Exception ex) {
                response.setContentType("text/plain; charset=utf-8");
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
                out.println(ex.getMessage());
                request.getServletContext().log("fail to decode", ex);
            } finally {
                if (out != filterOut && filterOut != null) {
                    filterOut.close();
                }
            }
        }
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }

    @Override
    protected void doPut(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        processRequest(request, response);
    }
}
