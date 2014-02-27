package schema.registry;

import java.util.Map;

public class ParameterUtil {

    public static String getParameter(Map<String, String[]> parameters, String name) {
        String[] values = parameters.get(name);
        if (values == null || values.length == 0) {
            return null;
        } else {
            return values[0];
        }
    }
}
