package r2u.tools.utils;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import r2u.tools.config.Configurator;
import r2u.tools.conn.FNConnector;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

public class JSONParser {
    private final static Logger logger = Logger.getLogger(JSONParser.class.getName());

    public void parseJson(String json) throws Exception {
        URI uri = Paths.get(json).toUri();
        JSONObject jsonObject = getJSON(new URI(uri.toString()).toURL());

        String sourceCPE = jsonObject.getString("sourceCPE"),
                sourceCPEObjectStore = jsonObject.getString("sourceCPEObjectStore"),
                sourceCPEUsername = jsonObject.getString("sourceCPEUsername"),
                sourceCPEPassword = jsonObject.getString("sourceCPEPassword"),
                jaasStanzaName = jsonObject.getString("jaasStanzaName"),
                query = jsonObject.getString("query"),
                path = jsonObject.getString("path"),
                csvSeparator = jsonObject.getString("csvSeparator"),
                fileImport = jsonObject.getString("fileImport"),
                regex = jsonObject.getString("regex"),
                export = jsonObject.getString("export");

        checkVariables(sourceCPE, sourceCPEObjectStore, sourceCPEUsername, sourceCPEPassword, jaasStanzaName,
                query, path, csvSeparator, fileImport, regex, export);

        configureData(sourceCPE, sourceCPEObjectStore, sourceCPEUsername, sourceCPEPassword, jaasStanzaName, query, path, csvSeparator, fileImport, regex, export);

        FNConnector fnConnector = new FNConnector();
        fnConnector.initWork();
    }

    private void configureData(String sourceCPE,
                               String sourceCPEObjectStore,
                               String sourceCPEUsername,
                               String sourceCPEPassword,
                               String jaasStanzaName,
                               String query,
                               String path,
                               String csvSeparator,
                               String fileImport,
                               String regex,
                               String export) {
        //Imposto i dati nel configuratore
        Configurator config = Configurator.getInstance();
        config.setSourceCPE(sourceCPE);
        config.setURIObjectStore(sourceCPEObjectStore);
        config.setCPEUsername(sourceCPEUsername);
        config.setCPEPassword(sourceCPEPassword);
        config.setJaasStanzaName(jaasStanzaName);
        config.setQuery(query);
        config.setPath(path);
        config.setCSVSeparator(csvSeparator);
        config.setFileImport(new File(fileImport));
        config.setRegex(regex);
        config.isExport(Boolean.parseBoolean(export));
    }

    private void checkVariables(String sourceCPE,
                                String sourceCPEObjectStore,
                                String sourceCPEUsername,
                                String sourceCPEPassword,
                                String jaasStanzaName,
                                String query,
                                String path,
                                String csvSeparator,
                                String fileImport,
                                String regex,
                                String export) {
        if (sourceCPE.isEmpty()) {
            logger.error("SourceCPE is empty. Aborting!");
            System.exit(-1);
        }
        if (sourceCPEObjectStore.isEmpty()) {
            logger.error("sourceCPEObjectStore is empty. Aborting!");
            System.exit(-1);
        }
        if (sourceCPEUsername.isEmpty()) {
            logger.error("sourceCPEUsername is empty. Aborting!");
            System.exit(-1);
        }
        if (sourceCPEPassword.isEmpty()) {
            logger.error("sourceCPEPassword is empty. Aborting!");
            System.exit(-1);
        }
        if (jaasStanzaName.isEmpty()) {
            logger.error("jaasStanzaName is empty. Aborting!");
            System.exit(-1);
        }
        if (query.isEmpty()) {
            logger.error("query is empty. Aborting!");
            System.exit(-1);
        }
        if (path.isEmpty()) {
            logger.error("path is empty. Aborting!");
            System.exit(-1);
        }
        if (csvSeparator.isEmpty()) {
            logger.error("csvSeparator is empty. Aborting!");
            System.exit(-1);
        }
        if (Files.notExists(Paths.get(fileImport))) {
            logger.error("Unable to find: " + fileImport + " Aborting!");
            System.exit(-1);
        }
        if (regex.isEmpty()) {
            logger.error("Regex is empty. Aborting!");
            System.exit(-1);
        }
        if (export.isEmpty()) {
            logger.error("Export is empty. Aborting!");
            System.exit(-1);
        }
    }

    private static JSONObject getJSON(URL url) throws IOException {
        return new JSONObject(IOUtils.toString(url, StandardCharsets.UTF_8));
    }
}
