package schema.registry;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class AttachmentUtil {

    public static void attach(HttpServletRequest request, HttpServletResponse response, String filename) {
        if (request.getParameter("attachment") != null) {
            response.setHeader("Content-Disposition", "attachment; filename=\""
                    + filename.replace('"', '-') + "\"");
        }
    }
}
