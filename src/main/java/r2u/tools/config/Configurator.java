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
    private boolean export;
    private boolean anImport;
    private String CSVSeparator;
    private File fileImport;

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

    public void setExport(boolean export) {
        this.export = export;
    }

    public boolean isExport() {
        return export;
    }

    public void setImport(boolean anImport) {
        this.anImport = anImport;
    }

    public boolean isImport() {
        return anImport;
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
}
