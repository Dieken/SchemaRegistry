package schema.registry.servlet;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import schema.registry.CentralSchemaRegistry;
import schema.registry.SchemaRegistry;

@WebListener
public class SchemaRegistryServletContextListener implements ServletContextListener {

    public static String SCHEMA_REGISTRY = "schemaRegistry";
    private static String SCHEMA_LIST = "schemaList";
    private static String ROOT_DIRECTORY = "rootDirectory";
    private static String RELOAD_INTERVAL = "reloadInterval";
    private static int DEFAULT_RELOAD_INTERVAL = 5;
    private Timer timer = new Timer("schemaListReloader");

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        ServletContext sc = sce.getServletContext();
        String schemaList = sc.getInitParameter(SCHEMA_LIST);
        String rootDirectory = sc.getInitParameter(ROOT_DIRECTORY);
        int reloadInterval = DEFAULT_RELOAD_INTERVAL;

        if (sc.getInitParameter(RELOAD_INTERVAL) != null) {
            reloadInterval = Integer.parseInt(sc.getInitParameter(RELOAD_INTERVAL));
        }

        if (schemaList == null || rootDirectory == null) {
            File f = firstExistedPropertyFile(sc,
                    System.getProperty("user.dir"),
                    System.getProperty("user.home"),
                    "/home/y/conf/SchemaRegistry");
            if (f != null) {
                try (FileInputStream in = new FileInputStream(f)) {
                    Properties p = new Properties();
                    p.load(in);
                    schemaList = p.getProperty(SCHEMA_LIST);
                    rootDirectory = p.getProperty(ROOT_DIRECTORY);
                    if (p.getProperty(RELOAD_INTERVAL) != null) {
                        reloadInterval = Integer.parseInt(p.getProperty(RELOAD_INTERVAL));
                    }
                } catch (IOException ex) {
                    sc.log("failed to load " + f.getPath(), ex);
                    throw new RuntimeException(ex);
                }
            }
        }

        if (reloadInterval <= 0) {
            reloadInterval = DEFAULT_RELOAD_INTERVAL;
        }

        if (schemaList == null || rootDirectory == null) {
            throw new RuntimeException("schemaList and rootDirectory must both be provided.");
        }

        try {
            File schemaListFile = new File(schemaList);
            CentralSchemaRegistry registry = new CentralSchemaRegistry(
                    schemaListFile, new File(rootDirectory));
            sc.setAttribute(SCHEMA_REGISTRY, registry);
            startReloadTimerTask(sc, schemaListFile, reloadInterval);
        } catch (IOException | ClassNotFoundException ex) {
            sce.getServletContext().log("fail to create schema registry", ex);
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent sce) {
        timer.cancel();
        sce.getServletContext().log("destroyed");
    }

    private File firstExistedPropertyFile(ServletContext sc, String... directories) {
        for (String dir : directories) {
            File f = new File(dir, "SchemaRegistry.properties");
            sc.log("checking file " + f.getPath());
            if (f.exists()) {
                sc.log("found file " + f.getPath());
                return f;
            }
        }

        return null;
    }

    private void startReloadTimerTask(ServletContext sc, File schemaList, int reloadInterval)
            throws IOException {
        timer.scheduleAtFixedRate(new SchemaListReloader(sc, schemaList),
                5000,
                reloadInterval * 1000);
    }

    private static class SchemaListReloader extends TimerTask {

        private ServletContext sc;
        private File schemaList;
        private long lastModifiedTime;

        public SchemaListReloader(ServletContext sc, File schemaList) throws IOException {
            this.sc = sc;
            this.schemaList = schemaList;
            lastModifiedTime = schemaList.getCanonicalFile().lastModified();
        }

        @Override
        public void run() {
            try {
                long t = schemaList.getCanonicalFile().lastModified();
                if (t == lastModifiedTime) {
                    return;
                }

                CentralSchemaRegistry registry = new CentralSchemaRegistry(schemaList,
                        getRegistry().getRootDirectory());
                sc.setAttribute(SCHEMA_REGISTRY, registry);
                lastModifiedTime = t;
                sc.log("successfully reload " + schemaList.getPath()
                        + ", canonical path is " + schemaList.getCanonicalPath());
            } catch (IOException | ClassNotFoundException ex) {
                try {
                    sc.log("fail to reload schema list " + schemaList.getPath()
                            + ", canonical path is " + schemaList.getCanonicalPath(), ex);
                } catch (IOException ex2) {
                    sc.log("got exception", ex2);
                }
            }
        }

        private SchemaRegistry getRegistry() {
            return (SchemaRegistry) sc.getAttribute(SCHEMA_REGISTRY);
        }
    }
}
