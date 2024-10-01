package r2u.tools.config;

import com.filenet.api.core.ObjectStore;

import java.io.File;

/**
 * Classe configuratore che contiene vari campi accessibili ovunque
 */
public class Configurator {
    private static Configurator instance = null;
    private String sourceCPE;
    private String CPEUsername;
    private String CPEPassword;
    private String jaasStanzaName;
    private String query;
    private ObjectStore objectStore;
    private String path;
    private String sourceCPEObjectStore;
    private String CSVSeparator;
    private File fileImport;
    private String regex;
    private boolean isExport;

    private Configurator() {

    }

    public static synchronized Configurator getInstance() {
        if (instance == null) {
            instance = new Configurator();
        }
        return instance;
    }

    public void setSourceCPE(String sourceCPE) {
        this.sourceCPE = sourceCPE;
    }

    public String getSourceCPE() {
        return sourceCPE;
    }

    public void setCPEUsername(String cpeUsername) {
        this.CPEUsername = cpeUsername;
    }

    public String getCPEUsername() {
        return CPEUsername;
    }

    public void setCPEPassword(String cpePassword) {
        this.CPEPassword = cpePassword;
    }

    public String getCPEPassword() {
        return CPEPassword;
    }

    public void setJaasStanzaName(String jaasStanzaName) {
        this.jaasStanzaName = jaasStanzaName;
    }

    public String getJaasStanzaName() {
        return jaasStanzaName;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public String getQuery() {
        return query;
    }

    public void setObjectStore(ObjectStore objectStore) {
        this.objectStore = objectStore;
    }

    public ObjectStore getObjectStore() {
        return objectStore;
    }

    public void setURIObjectStore(String sourceCPEObjectStore) {
        this.sourceCPEObjectStore = sourceCPEObjectStore;
    }

    public String getURIObjectStore() {
        return this.sourceCPEObjectStore;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getPath() {
        return path;
    }

    public void setCSVSeparator(String csvSeparator) {
        this.CSVSeparator = csvSeparator;
    }

    public String getCSVSeparator() {
        return CSVSeparator;
    }

    public void setFileImport(File fileImport) {
        this.fileImport = fileImport;
    }

    public File getFileImport() {
        return fileImport;
    }

    public void setRegex(String regex) {
        this.regex = regex;
    }

    public String getRegex() {
        return regex;
    }

    public void isExport(boolean isExport) {
        this.isExport = isExport;
    }

    public boolean isExport() {
        return isExport;
    }
}
