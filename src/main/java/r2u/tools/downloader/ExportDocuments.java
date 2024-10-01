package r2u.tools.downloader;

import com.filenet.api.collection.ContentElementList;
import com.filenet.api.core.ContentTransfer;
import com.filenet.api.core.Document;
import com.filenet.api.core.Factory;
import com.filenet.api.property.Properties;
import com.filenet.api.query.RepositoryRow;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import r2u.tools.config.Configurator;
import r2u.tools.constants.Constants;
import r2u.tools.utils.DataFetcher;
import r2u.tools.utils.FNOpUtils;
import r2u.tools.utils.FileSystemUtils;
import r2u.tools.utils.StringUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

/**
 * Classe atto a svolgere attività dell'export degli oggetti sul FileSystem.
 * Si crea una cartella con bo_id all'interno contiene dei file + file.metadata x ciascuno.
 * A sua volta si creeranno delle sotto cartelle con GUID degli oggetti che a loro volta contengono i riferimenti.
 * Esempio di alberatura tra padre e figlio:</br>bo_id/acq_all_contratto/file+file.metadata </br>
 * Esempio di alberatura tra figlio e figli quindi: padre -> figli -> nipoti </br>
 * bo_id/acq_all_contratto/GUID/file+file.metadata
 */
@SuppressWarnings("DuplicatedCode")
public class ExportDocuments {
    private final static Logger logger = Logger.getLogger(ExportDocuments.class.getName());
    private final static Configurator config = Configurator.getInstance();

    public void startExport() {
        final String dataFileName = "data.csv";
        final Path dataCSV = Paths.get(dataFileName);
        try {
            //Creo un file csv con intestazione, ma ancora prima verifico se esiste.
            if (Files.exists(dataCSV)) {
                try {
                    Files.delete(dataCSV);
                    defineCSVColumns();
                } catch (IOException e) {
                    logger.error("UNABLE TO DELETE data.csv", e);
                }
            } else {
                defineCSVColumns();
            }
            String query = config.getQuery();
            if (config.getFileImport() != null) {
                List<String> fileRows = FileUtils.readLines(new File(config.getFileImport().toURI()), StandardCharsets.UTF_8);
                fileRows = fileRows.subList(1, fileRows.size());
                String condition = "";
                for (String row : fileRows) {
                    //noinspection StringConcatenationInLoop
                    condition = condition + "'" + row.split(",")[0] + "',";
                }
                condition = condition.substring(0, condition.length() - 1);
                query = query + " WHERE [bo_id] IN (" + condition + ")";
            }
            //Fase 1: scarico il documento padre
            Iterator<?> contractsIterator = DataFetcher.fetchDataByQuery(query, config.getObjectStore());
            //Padre
            processMainNode(contractsIterator);
        } catch (Exception e) {
            logger.error("SOMETHING WENT WRONG", e);
        }
    }

    /**
     * Metodo atto a creare il file con definizione delle colonne
     */
    private static void defineCSVColumns() {
        try (BufferedWriter createFile = new BufferedWriter(new FileWriter("data.csv", true))) {
            createFile.write("OLD_NAME" + config.getCSVSeparator() +
                    "RETRIEVAL_NAME" + config.getCSVSeparator() +
                    "DOCTYPE" + config.getCSVSeparator() +
                    "DESCRIPTION_FULL" + config.getCSVSeparator() +
                    "MIME_TYPE" + config.getCSVSeparator() +
                    "EXPORT_STATUS" + config.getCSVSeparator() +
                    "NEW_NAME" + config.getCSVSeparator() +
                    "IMPORT_STATUS" + config.getCSVSeparator() +
                    "HAS_ATTACHMENTS" + config.getCSVSeparator() + "SUB_CHILD_LIST\n");
        } catch (IOException e) {
            logger.error("UNABLE TO CREATE " + "data.csv", e);
        }
    }

    /**
     * Metodo atto a svolgere attività sul padre, vedere anche {@link #processChildNode(Iterator, Properties)}
     * e {@link #processSubChildNode(Iterator, String)}
     *
     * @param contractsIterator iteratore che cerca degli BO_ID
     */
    protected static void processMainNode(Iterator<?> contractsIterator) {
        logger.info("Processing main node");
        while (contractsIterator != null && contractsIterator.hasNext()) {
            Document fetchedDocument = null;
            try {
                //Qui non faccio altro che: scaricare comunque i documenti che possa contenere il padre (nun si sa mai)
                RepositoryRow headRepositoryRow = (RepositoryRow) contractsIterator.next();
                Properties headProperties = headRepositoryRow.getProperties();
                fetchedDocument = Factory.Document.fetchInstance(config.getObjectStore(), headProperties.getIdValue("ID"), null);
                logger.info("Found document: " + headProperties.getIdValue("ID") + " bo_id: " + headProperties.getStringValue("bo_id") + " className: " + fetchedDocument.getClassName());
                ContentElementList contentElements = fetchedDocument.get_ContentElements();
                if (!contentElements.isEmpty()) {
                    for (Object docContentElement : contentElements) {
                        ContentTransfer contentTransfer = (ContentTransfer) docContentElement;
                        InputStream inputStream = contentTransfer.accessContentStream();
                        String folderHeadName = headProperties.getStringValue("bo_id") + "/";
                        logger.info("Trying to create folder under path: " + config.getPath() + "/" + folderHeadName);
                        FileUtils.copyInputStreamToFile(inputStream, new File(config.getPath() + folderHeadName));
                    }
                }
                //Fase 2: scarico il documento figlio
                //Mi rivolgo a Relazioni SDI (acq_relation) avendo GUID del padre,
                //Ne ricavo i suoi figli e x ogni figlio:
                //Creo cartella con: "bo_id del padre/classe_documentale/filename"
                Iterator<?> childContractsIterator = DataFetcher.executeQuery(
                        config.getObjectStore() /*object store*/,
                        "" /*colonna*/,
                        "acq_relation" /*tabella*/,
                        "head_chronicle_id" /*colonna di ricerca*/,
                        headProperties.getIdValue("ID").toString() /*valore di ricerca*/
                );
                //Figlio
                processChildNode(childContractsIterator, headProperties);
            } catch (Exception exception) {
                try (BufferedWriter unManagedErrorsWriter = new BufferedWriter(new FileWriter(Objects.requireNonNull(fetchedDocument).getClassName() + "_caught_errors.txt", true))) {
                    unManagedErrorsWriter.write("AN ERROR IS OCCURED WITH DOCUMENT_CLASS: " + fetchedDocument.getClassName() + " ID " + fetchedDocument.getProperties().getIdValue("ID").toString()
                            + "\nERROR IS: " + exception +
                            "\nMESSAGE IS: " + exception.getMessage() +
                            "\nCAUSE IS: " + exception.getCause() +
                            "\nSTACK TRACE: " + Arrays.toString(exception.getStackTrace()) + "\n");
                } catch (IOException e) {
                    logger.error("UNABLE TO CREATE/WRITE TO FILE", e);
                }
            }
        }
    }

    /**
     * Metodo atto a processare i figli
     *
     * @param childContractsIterator iteratore contenente i figli
     * @param headProperties         proprietà del padre
     */
    private static void processChildNode(Iterator<?> childContractsIterator, Properties headProperties) {
        logger.info("Processing child node");
        try {
            while (childContractsIterator != null && childContractsIterator.hasNext()) {
                RepositoryRow childRepositoryRow = (RepositoryRow) childContractsIterator.next();
                Properties childProperties = childRepositoryRow.getProperties();
                Document childDocument = Factory.Document.fetchInstance(config.getObjectStore(),
                        childProperties.getIdValue("tail_chronicle_id"), null);
                ContentElementList childContentElement = childDocument.get_ContentElements();
                String fileMetaData;
                String fileName = config.getPath() + headProperties.getStringValue("bo_id") + "/" + childDocument.getClassName() + "/" + childDocument.get_Name();
                String folderPath = config.getPath() + headProperties.getStringValue("bo_id") + "/" + childDocument.getClassName() + "/";
                //HACK: Per acq_pos_contratto e forse anche per gli forzo la creazione di path: bo_id/symbolic_name,
                //HACK: cosicché riesco a scriverci dentro i meta dati.
                FileUtils.forceMkdir(new File(folderPath));
                if (!childDocument.get_Name().isEmpty()) {
                    fileMetaData = config.getPath() + headProperties.getStringValue("bo_id") + "/" + childDocument.getClassName() + "/" + childDocument.get_Name() + "_metadata";
                } else {
                    Format formatter = new SimpleDateFormat(Constants.printableTimeFormat);
                    fileMetaData = config.getPath() + headProperties.getStringValue("bo_id") + "/" + childDocument.getClassName() + "/" + childDocument.getClassName() + "_" + formatter.format(new Date()) + "_metadata";
                }
                if (childDocument.getClassName().equals("acq_all_contratto")) {
                    FNOpUtils.createACQAllContrattoMetadata(childDocument, fileMetaData);
                }
                if (childDocument.getClassName().equals("acq_pos_contratto")) {
                    BufferedWriter writeData = new BufferedWriter(new FileWriter("data.csv", true));
                    writeData.write(fileMetaData + config.getCSVSeparator() + config.getCSVSeparator() + childDocument.getClassName() + config.getCSVSeparator() + childProperties.getStringValue("description_full") + "\n");
                    writeData.close();
                    FNOpUtils.createACQPosContrattoMetadata(childDocument, fileMetaData);
                } else {
                    //Ricerco figli del figlio.
                    Iterator<?> subChildContractsIterator = DataFetcher.executeQuery(
                            config.getObjectStore() /*object store*/,
                            "" /*colonna*/,
                            "acq_relation" /*tabella*/,
                            "head_chronicle_id" /*colonna di ricerca*/,
                            childDocument.getProperties().getIdValue("ID").toString() /*valore di ricerca*/
                    );
                    if (subChildContractsIterator != null && subChildContractsIterator.hasNext()) {
                        //Figli del figlio, acq_all_doc_contratto
                        String subChildListPath = config.getPath() + headProperties.getStringValue("bo_id") + "/" + childDocument.getClassName() + "/" + childDocument.getProperties().getIdValue("ID");
                        subChildListPath = subChildListPath.replace("{", "").replace("}", "");
                        //HACK 2 Creazione delle sottocartelle con nome del GUID senza graffe
                        FileUtils.forceMkdir(new File(subChildListPath));
                        FileSystemUtils.createFile(folderPath, folderPath, childProperties, childDocument, childContentElement, fileName, "HAS_ATTACHMENT",
                                subChildListPath, true);
                        processSubChildNode(subChildContractsIterator, subChildListPath + "/");
                    }
                }
                FileSystemUtils.createFile(folderPath, folderPath, childProperties, childDocument, childContentElement, fileName, "", "", false);
            }
        } catch (IOException e) {
            logger.error("SOMETHING WENT WRONG", e);
        }
    }

    /**
     * Metodo atto a processare figli dei figli
     *
     * @param subChildContractsIterator iteratore che ne contiene sotto figli esempio acq_all_doc
     * @param subChildListPath          cartella fatta da GUID del padre (acq_all_contratto) quale figlio del nonno (acq_contratto)
     */
    private static void processSubChildNode(Iterator<?> subChildContractsIterator, String subChildListPath) {
        logger.info("Processing sub child node");
        Pattern pattern = Pattern.compile(config.getRegex());
        while (subChildContractsIterator != null && subChildContractsIterator.hasNext()) {
            String path;
            try {
                RepositoryRow subChildRepositoryRow = (RepositoryRow) subChildContractsIterator.next();
                Properties subChildProperties = subChildRepositoryRow.getProperties();
                Document subChildDocument = Factory.Document.fetchInstance(config.getObjectStore(),
                        subChildProperties.getIdValue("tail_chronicle_id"), null);
                ContentElementList subChildDocumentContentElements = subChildDocument.get_ContentElements();

                String fileMetaData;
                String fileName = subChildListPath + subChildDocument.get_Name();
                if (pattern.matcher(fileName).find()) {
                    fileName = StringUtils.removeSpecialCharacter(fileName, config.getRegex());
                }
                path = subChildDocument.getClassName() + "/" + subChildDocument.get_Name();
                if (!subChildDocument.get_Name().isEmpty()) {
                    fileMetaData = subChildListPath + subChildDocument.get_Name() + "_metadata";
                } else {
                    Format formatter = new SimpleDateFormat(Constants.printableTimeFormat);
                    fileMetaData = subChildListPath + "_" + formatter.format(new Date()) + "_metadata";
                }
                if (pattern.matcher(fileMetaData).find()) {
                    fileMetaData = StringUtils.removeSpecialCharacter(fileMetaData, config.getRegex());
                }

                for (Object docContentElement : subChildDocumentContentElements) {
                    ContentTransfer contentTransfer = (ContentTransfer) docContentElement;
                    InputStream inputStream = contentTransfer.accessContentStream();
                    if (Files.notExists(Paths.get(fileName))) {
                        logger.info("RetrievalName: " + contentTransfer.get_RetrievalName());
                        logger.info("Trying to save file {" + fileName + "} under path: " + path);
                        FileUtils.copyInputStreamToFile(inputStream, new File(fileName));
                    } else {
                        logger.warn("THERES FILE ALREADY WITH THE SAME NAME: " + subChildDocument.get_Name());
                        logger.info("RetrievalName: " + contentTransfer.get_RetrievalName());
                        logger.info("Trying to save file {" + fileName + "} under path: " + path);
                        //X gestire i file con lo stesso name
                        Format formatter = new SimpleDateFormat(Constants.printableTimeFormat);
                        FileUtils.copyInputStreamToFile(inputStream, new File(fileName + formatter.format(new Date()) + "_" + subChildDocument.get_Name()));
                    }
                }

                logger.info("SubChildDocument ID: " + subChildDocument.getProperties().getIdValue("ID"));
                if (subChildDocument.getClassName().equals("acq_all_doc_contratto")) {
                    FNOpUtils.createACQAllDocContrattoMetadata(subChildDocument, fileMetaData);
                }
            } catch (Exception e) {
                logger.error("SOMETHING WENT WRONG", e);
            }
        }
    }
}
