package r2u.tools.utils;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import r2u.tools.config.Configurator;
import r2u.tools.conn.FNConnector;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;

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
                documentClass = jsonObject.getString("documentClass"),
                query = jsonObject.getString("query"),
                path = jsonObject.getString("path");

        ArrayList<String> documentList = Converters.stringToArrayListConverter(documentClass);

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
        if (documentList.isEmpty()) {
            logger.error("documentClass is empty. Aborting!");
            System.exit(-1);
        }
        if (path.isEmpty()) {
            logger.error("path is empty. Aborting!");
            System.exit(-1);
        }

        //Imposto i dati nel configuratore
        Configurator instance = Configurator.getInstance();
        instance.setSourceCPE(sourceCPE);
        instance.setURIObjectStore(sourceCPEObjectStore);
        instance.setCPEUsername(sourceCPEUsername);
        instance.setCPEPassword(sourceCPEPassword);
        instance.setJaasStanzaName(jaasStanzaName);
        instance.setDocumentClass(documentList);
        instance.setQuery(query);
        instance.setPath(path);
        FNConnector fnConnector = new FNConnector();
        fnConnector.initWork();
    }

    private static JSONObject getJSON(URL url) throws IOException {
        return new JSONObject(IOUtils.toString(url, StandardCharsets.UTF_8));
    }
}
