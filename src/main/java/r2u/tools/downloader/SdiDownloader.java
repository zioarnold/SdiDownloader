package r2u.tools.downloader;

import com.filenet.api.collection.ContentElementList;
import com.filenet.api.constants.AutoClassify;
import com.filenet.api.constants.CheckinType;
import com.filenet.api.constants.RefreshMode;
import com.filenet.api.core.ContentTransfer;
import com.filenet.api.core.CustomObject;
import com.filenet.api.core.Document;
import com.filenet.api.core.Factory;
import com.filenet.api.property.Properties;
import com.filenet.api.query.RepositoryRow;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import r2u.tools.config.Configurator;
import r2u.tools.constants.Constants;
import r2u.tools.utils.DataFetcher;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Pattern;

public class SdiDownloader {
    private final static Logger logger = Logger.getLogger(SdiDownloader.class.getName());
    private final static Configurator config = Configurator.getInstance();

    /**
     * Metodo atto a svolgere attività di download dei documenti fisici.
     * Nella fase uno dell'export attualmente è previsto che lui crei la cartella che avrà il nome del bo_id del padre, poi scarica la dentro il contenuto ossia i file fisici.
     * Nella fase due dell'import è previsto che lui carica i file fisici al figlio del acq_contratto e qualora non ci fosse lo crea. Consultando prima per&ograve; Relazione SDI (acq_relation)
     */
    public void startDownload() throws IOException {
        final String dataFileName = "data.csv";
        final String outputFileName = "output.csv";
        Path dataCSV = Paths.get(dataFileName);
        if (config.isExport()) {
            //Creo un file csv con intestazione, ma ancora prima verifico se esiste.
            if (Files.exists(dataCSV)) {
                try {
                    Files.delete(dataCSV);
                    defineCSVColumns(dataFileName);
                } catch (IOException e) {
                    logger.error("UNABLE TO DELETE data.csv", e);
                }
            } else {
                defineCSVColumns(dataFileName);
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
        }

        if (config.isImport()) {
            List<String> output = new ArrayList<>();
            output.add("OLD_NAME" + config.getCSVSeparator() +
                    "RETRIEVAL_NAME" + config.getCSVSeparator() +
                    "DOCTYPE" + config.getCSVSeparator() +
                    "DESCRIPTION_FULL" + config.getCSVSeparator() +
                    "MIME_TYPE" + config.getCSVSeparator() +
                    "EXPORT_STATUS" + config.getCSVSeparator() +
                    "NEW_NAME" + config.getCSVSeparator() + "IMPORT_STATUS");
            final String ok = ",STATUS_OK", ko = ",STATUS_KO";

            if (Files.notExists(dataCSV)) {
                logger.error("THERE'S NO data.csv FOUND. PERHAPS U FORGOT TO DO EXPORT FIRST?");
                return;
            }
            try {
                //Qui leggo due file: uno creato nella fase dell'export e altro per leggere sia il vecchio bo_id che nuovo
                List<String> importFile = FileUtils.readLines(config.getFileImport(), "UTF-8");
                importFile = importFile.subList(1, importFile.size());
                List<String> dataFile = FileUtils.readLines(new File(dataFileName), "UTF-8");
                dataFile = dataFile.subList(1, dataFile.size());
                //File di import
                for (String row : importFile) {
                    Iterator<?> newIdMainDocument = DataFetcher.executeQuery(
                            config.getObjectStore(),
                            "",
                            "acq_contratto",
                            "bo_id",
                            "'" + row.split(config.getCSVSeparator())[1]/*new bo_id*/ + "'"
                    );
                    while (newIdMainDocument != null && newIdMainDocument.hasNext()) {
                        String path = "";
                        try {
                            RepositoryRow repositoryRow = (RepositoryRow) newIdMainDocument.next();
                            Properties properties = repositoryRow.getProperties();
                            Document mainDocument = Factory.Document.fetchInstance(
                                    config.getObjectStore(),
                                    properties.getIdValue("ID"),
                                    null);
                            //Cerco di recuperare figlio/figli del "acq_contratto"
                            Iterator<?> childDocumentIterator = DataFetcher.executeQuery(
                                    config.getObjectStore(),
                                    "tail_chronicle_id",
                                    "acq_relation",
                                    "head_chronicle_id",
                                    mainDocument.getProperties().getIdValue("ID").toString()
                            );
                            if (childDocumentIterator != null && childDocumentIterator.hasNext()) {
                                //Se ci sono allora allego i file fisici al figlio/figli
                                while (childDocumentIterator.hasNext()) {
                                    for (int j = 0; j < dataFile.size(); j++) {
                                        String symbolicName /*doctype in csv*/ = dataFile.get(j).split(config.getCSVSeparator())[2];
                                        RepositoryRow childRepositoryRow = (RepositoryRow) childDocumentIterator.next();
                                        Properties childRepositoryRowProperties = childRepositoryRow.getProperties();

                                        Document childDocument = Factory.Document.fetchInstance(config.getObjectStore(), childRepositoryRowProperties.getIdValue("tail_chronicle_id"), null);
                                        logger.info("childDocumentClass: " + childDocument.getClassName());
                                        logger.info("Attaching the files to the existing child document ID: " + childDocument.getProperties().getIdValue("ID") + " " + symbolicName);
                                        if (!childDocument.getClassName().equals("acq_pos_contratto")) {
                                            attachFile(dataFile, j, childDocument, symbolicName);
                                        } else {
                                            fillAcqPosContractData(dataFile, j, childDocument);
                                            childDocument.save(RefreshMode.REFRESH);
                                        }
                                    }
                                }
                            } else {
                                //Se non ci sono, allora apro dataFile (data.csv) scorro ciascuna riga.
                                //Controllo esistenza della bo_id nella data file con quella dell'input-file.
                                for (int j = 0; j < dataFile.size(); j++) {
                                    String oldBoId = row.split(config.getCSVSeparator())[0],
                                            newBoId = "",
                                            replace = "";
                                    if (dataFile.get(j).contains(oldBoId)) {
                                        newBoId = row.split(config.getCSVSeparator())[1];
                                        replace = dataFile.get(j).replace(oldBoId, newBoId);
                                        path = dataFile.get(j).concat(replace.split(config.getCSVSeparator())[0]);
                                    }
                                    String symbolicName = dataFile.get(j).split(config.getCSVSeparator())[2];
                                    logger.info("Creating a new document of: " + symbolicName);
                                    Document newDocument = Factory.Document.createInstance(config.getObjectStore(), symbolicName);
                                    newDocument.getProperties().putValue("bo_id", newBoId);
                                    newDocument.save(RefreshMode.REFRESH);
                                    Document document = Factory.Document.fetchInstance(config.getObjectStore(), newDocument.getProperties().getIdValue("ID"), null);
                                    logger.info("Document created, with GUID: " + document.getProperties().getIdValue("ID"));
                                    //vedo se e` un acq_pos_contratto per cui faccio altro ragionamento
                                    if (document.getClassName().equals("acq_pos_contratto")) {
                                        //HACK Nel caso di posizioni di contratto, csv di output genero` male
                                        path = dataFile.get(j).concat(config.getCSVSeparator() + ok + config.getCSVSeparator() + replace.split(config.getCSVSeparator())[0]);
                                        fillAcqPosContractData(dataFile, j, document);
                                        document.checkin(AutoClassify.DO_NOT_AUTO_CLASSIFY, CheckinType.MAJOR_VERSION);
                                        document.save(RefreshMode.REFRESH);
                                        String concat = path + ok;
                                        output.add(concat);
                                    } else {
                                        attachFile(dataFile, j, document, symbolicName);
                                        String concat = path + ok;
                                        output.add(concat);
                                    }
                                    createRelationship(mainDocument, newDocument, dataFile, j);
                                }
                            }
                        } catch (Exception e) {
                            logger.error("SOMETHING WENT WRONG", e);
                            output.add(path + ko);
                        }
                    }
                }
                try (BufferedWriter createFile = new BufferedWriter(new FileWriter(outputFileName, false))) {
                    for (String line : output) {
                        createFile.write(line + '\n');
                    }
                } catch (IOException e) {
                    logger.error("UNABLE TO CREATE/WRITE " + outputFileName, e);
                }
            } catch (Exception e) {
                logger.error("SOMETHING WENT WRONG", e);
            }
            output.clear();
        }
        //Cleanup
        if (config.isCleanup()) {
            List<String> importFile = FileUtils.readLines(config.getFileImport(), "UTF-8");
            for (int i = 1; i < importFile.size(); i++) {
                FileUtils.deleteDirectory(new File(importFile.get(i).split(config.getCSVSeparator())[0]));
            }
            FileUtils.forceDelete(new File(dataFileName));
        }
    }

    private static void fillAcqPosContractData(List<String> dataFile, int j, Document childDocument) {
        logger.info("Working with childDocument: " + childDocument.getProperties().getIdValue("ID"));
        Document doc = null;
        String[] line = new String[0];
        try {
            BufferedReader reader = new BufferedReader(new FileReader(dataFile.get(j).split(config.getCSVSeparator())[0]));
            logger.info("Reading file on acq_pos_contract: " + dataFile.get(j).split(config.getCSVSeparator())[0]);
            while (reader.ready()) {
                line = reader.readLine().split(config.getCSVSeparator());
                switch (line[0]) {
                    case "ID":
                        doc = Factory.Document.fetchInstance(config.getObjectStore(), line[1], null);
                        break;
                    case "bo_bu":
                    case "bo_ente":
                    case "bo_gm":
                    case "tipo_negoz":
                    case "fk1":
                    case "fk2":
                    case "flag_allegati":
                    case "flag_cancellato":
                    case "system_id":
                    case "old_bo_bu":
                    case "padre":
                    case "fornitore":
                    case "origine":
                        if (!line[1].equals("null")) {
                            childDocument.getProperties().putValue(line[0], Objects.requireNonNull(doc).getProperties().getInteger32Value(line[0]));
                        }
                        break;
                    case "data_emissione":
                    case "creation_date":
                    case "data_fin_val":
                    case "data_in_val":
                        if (!line[1].equals("null")) {
                            childDocument.getProperties().putValue(line[0], Objects.requireNonNull(doc).getProperties().getDateTimeValue(line[0]));
                        }
                        break;
                    case "has_correlazioni":
                    case "has_riferimenti":
                        if (!line[1].equals("null")) {
                            childDocument.getProperties().putValue(line[0], Objects.requireNonNull(doc).getProperties().getBooleanValue(line[0]));
                        }
                        break;
                    default:
                        if (!line[0].isEmpty() && !line[1].equals("null")) {
                            childDocument.getProperties().putValue(line[0], line[1]);
                        }
                        break;
                }
            }
            reader.close();
        } catch (IOException e) {
            logger.error("IO Exception OCCURED", e);
        } catch (NumberFormatException e) {
            logger.error("CHECK INPUT DATA FOR: " + line[0] + " AGAINST: " + line[1], e);
        }
    }

    /**
     * Metodo atto a creare delle relazioni, acq_relation
     *
     * @param mainDocument documento principale
     * @param newDocument  documento creato nuovo
     * @param dataFile     description_full ritrovabile in data.csv
     * @param j            indice
     */
    private static void createRelationship(Document mainDocument, Document newDocument, List<String> dataFile, int j) {
        logger.info("Creating relationship between: " + mainDocument.getClassName() + "/" + mainDocument.getProperties().getIdValue("ID")
                + " and " + newDocument.getClassName() + "/" + newDocument.getProperties().getIdValue("ID"));
        CustomObject acqRelation = Factory.CustomObject.createInstance(config.getObjectStore(), "acq_relation");
        acqRelation.getProperties().putValue("tail_chronicle_id", newDocument.getProperties().getIdValue("ID"));
        acqRelation.getProperties().putValue("head_chronicle_id", mainDocument.getProperties().getIdValue("ID"));
        acqRelation.getProperties().putValue("description_full", dataFile.get(j).split(config.getCSVSeparator())[2]);
        acqRelation.save(RefreshMode.REFRESH);
        logger.info("Saving relationship is successful");
    }

    /**
     * Metodo atto a svolgere attività sul padre, vedere anche {@link #processChildNode(Iterator, Properties)}
     * e {@link #processSubChildNode(Iterator, String, Document)}
     *
     * @param contractsIterator iteratore che cerca degli BO_ID
     */
    private static void processMainNode(Iterator<?> contractsIterator) {
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

                //FIXME: Da pensare come cavolo creare nuove relazioni in futuro tra acq_all_doc_contratto e acq_all_contratto.
                //Ricerco figli del figlio.
                Iterator<?> subChildContractsIterator = DataFetcher.executeQuery(
                        config.getObjectStore() /*object store*/,
                        "" /*colonna*/,
                        "acq_relation" /*tabella*/,
                        "head_chronicle_id" /*colonna di ricerca*/,
                        childDocument.getProperties().getIdValue("ID").toString() /*valore di ricerca*/
                );
                //Figli del figlio, acq_all_doc_contratto
//                processSubChildNode(subChildContractsIterator, folderPath, childDocument);
                //HACK: Per acq_pos_contratto e forse anche per gli forzo la creazione di path: bo_id/symbolic_name,
                //HACK: cosicché riesco a scriverci dentro i meta dati.
                FileUtils.forceMkdir(new File(folderPath));
                if (!childDocument.get_Name().isEmpty()) {
                    fileMetaData = config.getPath() + headProperties.getStringValue("bo_id") + "/" + childDocument.getClassName() + "/" + childDocument.get_Name() + "_metadata";
                } else {
                    Format formatter = new SimpleDateFormat(Constants.printableTimeFormat);
                    fileMetaData = config.getPath() + headProperties.getStringValue("bo_id") + "/" + childDocument.getClassName() + "/" + childDocument.getClassName() + "_" + formatter.format(new Date()) + "_metadata";
                }
                createFile(folderPath, folderPath, childProperties, childDocument, childContentElement, fileName);
                if (childDocument.getClassName().equals("acq_all_contratto")) {
                    createACQContrattoMetadata(childDocument, fileMetaData);
//                    createHeadTailRelationshipFile(childDocument);
                }
                if (childDocument.getClassName().equals("acq_pos_contratto")) {
                    BufferedWriter writeData = new BufferedWriter(new FileWriter("data.csv", true));
                    writeData.write(fileMetaData + config.getCSVSeparator() + config.getCSVSeparator() + childDocument.getClassName() + config.getCSVSeparator() + childProperties.getStringValue("description_full") + "\n");
                    writeData.close();
                    createACQPosContrattoMetadata(childDocument, fileMetaData);
                }
            }
        } catch (IOException e) {
            logger.error("SOMETHING WENT WRONG", e);
        }
    }

    //Questa x far la replace dei caratteri speciali a prescindere del OS.
    //Sembrerebbe che va un cazzo, ma chi se ne sbatte i coglioni x zero EUR in busta.
    private static String removeSpecialCharacter(String string, String regex) {
        return string.replaceAll(regex, "");
    }

    /**
     * Metodo atto a processare figli dei figli
     *
     * @param subChildContractsIterator iteratore che ne contiene sotto figli esempio acq_all_doc
     * @param folderPath                cartella in cui vengono salvati i figli dei figli
     * @param childDocument             padre del figlio
     */
    private static void processSubChildNode(Iterator<?> subChildContractsIterator, String folderPath, Document childDocument) {
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
                String fileName = folderPath + subChildDocument.getClassName() + "/" + subChildDocument.get_Name();
                if (pattern.matcher(fileName).find()) {
                    fileName = removeSpecialCharacter(fileName, config.getRegex());
                }
                path = subChildDocument.getClassName() + "/" + subChildDocument.get_Name();
                if (!subChildDocument.get_Name().isEmpty()) {
                    fileMetaData = folderPath + "/" + subChildDocument.getClassName() + "/" + subChildDocument.get_Name() + "_metadata";
                } else {
                    Format formatter = new SimpleDateFormat(Constants.printableTimeFormat);
                    fileMetaData = folderPath + "/" + subChildDocument.getClassName() + "_" + formatter.format(new Date()) + "_metadata";
                }
                if (pattern.matcher(fileMetaData).find()) {
                    fileMetaData = removeSpecialCharacter(fileMetaData, config.getRegex());
                }
                logger.info("SubChildDocument ID: " + subChildDocument.getProperties().getIdValue("ID"));
                createFile(folderPath, path, subChildProperties, subChildDocument, subChildDocumentContentElements, fileName);
                if (subChildDocument.getClassName().equals("acq_all_doc_contratto")) {
                    createACQAllDocContrattoMetadata(subChildDocument, fileMetaData);
                }
            } catch (IOException e) {
                logger.error("SOMETHING WENT WRONG", e);
            }
        }
    }

    private static void createHeadTailRelationshipFile(Document childDocument) {
        Iterator<?> headTailRelationshipFileIterator = DataFetcher.executeQuery(
                config.getObjectStore() /*object store*/,
                "" /*colonna*/,
                "acq_relation" /*tabella*/,
                "head_chronicle_id" /*colonna di ricerca*/,
                childDocument.getProperties().getIdValue("ID").toString() /*valore di ricerca*/
        );
        while (headTailRelationshipFileIterator != null && headTailRelationshipFileIterator.hasNext()) {
            RepositoryRow headTailRelRepositoryRow = (RepositoryRow) headTailRelationshipFileIterator.next();
            Properties headTailRelRepositoryRowProperties = headTailRelRepositoryRow.getProperties();
            try (BufferedWriter createFile = new BufferedWriter(new FileWriter("test.t", true))) {
                createFile.write(childDocument.getProperties().getIdValue("ID").toString() + config.getCSVSeparator()
                        + headTailRelRepositoryRowProperties.getIdValue("tail_chronicle_id").toString() + config.getCSVSeparator()
                        + headTailRelRepositoryRowProperties.getStringValue("description_full") + "\n");
                logger.info("foundSubChild IDS: " + headTailRelRepositoryRowProperties.getIdValue("tail_chronicle_id"));
            } catch (IOException e) {
                logger.error("UNABLE TO CREATE data.csv", e);
            }
        }
    }

    //Creo file fisici sul FS.
    private static void createFile(String folderPath, String path, Properties subChildProperties, Document subChildDocument, ContentElementList subChildDocumentContentElements, String fileName) throws IOException {
        if (!subChildDocumentContentElements.isEmpty()) {
            for (Object docContentElement : subChildDocumentContentElements) {
                ContentTransfer contentTransfer = (ContentTransfer) docContentElement;
                InputStream inputStream = contentTransfer.accessContentStream();
                String line;
                //Qui produco un file diciamo temporaneo di riepilogo del download
                //Formato e`: OLD_NAME, DOCTYPE, DESCRIPTION_FULL, EXPORT_STATUS
                if (Files.notExists(Paths.get(fileName))) {
                    logger.info("RetrievalName: " + contentTransfer.get_RetrievalName());
                    logger.info("Trying to save file {" + fileName + "} under path: " + path);
                    FileUtils.copyInputStreamToFile(inputStream, new File(fileName));
                    line = fileName + config.getCSVSeparator() + contentTransfer.get_RetrievalName() + config.getCSVSeparator() + subChildDocument.getClassName() + config.getCSVSeparator()
                            + subChildProperties.getStringValue("description_full") + config.getCSVSeparator() + contentTransfer.get_ContentType() + config.getCSVSeparator() + "STATUS_OK" + config.getCSVSeparator() + "\n";
                    BufferedWriter writeData = new BufferedWriter(new FileWriter("data.csv", true));
                    writeData.write(line);
                    writeData.close();
                } else {
                    logger.warn("THERES FILE ALREADY WITH THE SAME NAME: " + subChildDocument.get_Name());
                    logger.info("RetrievalName: " + contentTransfer.get_RetrievalName());
                    logger.info("Trying to save file {" + fileName + "} under path: " + path);
                    //X gestire i file con lo stesso name
                    Format formatter = new SimpleDateFormat(Constants.printableTimeFormat);
                    FileUtils.copyInputStreamToFile(inputStream, new File(folderPath + formatter.format(new Date()) + "_" + subChildDocument.get_Name()));
                    line = folderPath + formatter.format(new Date()) + "_" + subChildDocument.get_Name() + config.getCSVSeparator() + contentTransfer.get_RetrievalName() + config.getCSVSeparator() + subChildDocument.getClassName() + config.getCSVSeparator() + subChildProperties.getStringValue("description_full") + config.getCSVSeparator() + contentTransfer.get_ContentType() + config.getCSVSeparator() + "STATUS_OK" + config.getCSVSeparator() + "\n";
                    BufferedWriter writeData = new BufferedWriter(new FileWriter("data.csv", true));
                    writeData.write(line);
                    writeData.close();
                }
            }
        } else {
            logger.info("No document found associated with child");
        }
    }

    /**
     * Metodo atto a creare un file d'appoggio che ne contiene informazioni di meta dati posizioni di contratto
     *
     * @param childDocument classe documentale
     * @param fileMetaData  nome del file da creare
     */
    private static void createACQPosContrattoMetadata(Document childDocument, String fileMetaData) {
        try (BufferedWriter writeMetaData = new BufferedWriter(new FileWriter(fileMetaData, true))) {
            writeMetaData.write(
                    "ID" + config.getCSVSeparator() + childDocument.getProperties().getIdValue("ID") + "\n" +
                            "bo_bu" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("bo_bu") + "\n" +
                            "bo_ente" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("bo_ente") + "\n" +
                            "bo_gm" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("bo_gm") + "\n" +
                            "bo_pos" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("bo_pos") + "\n" +
                            "data_emissione" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_emissione") + "\n" +
                            "tipo_negoz" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("tipo_negoz") + "\n" +
                            "bo_gm_chronid_ref" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("bo_gm_chronid_ref") + "\n" +
                            "creation_date" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("creation_date") + "\n" +
                            "data_fin_val" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_fin_val") + "\n" +
                            "data_in_val" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_in_val") + "\n" +
                            "divisione" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("divisione") + "\n" +
                            "fk1" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("fk1") + "\n" +
                            "fk2" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("fk2") + "\n" +
                            "flag_allegati" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("flag_allegati") + "\n" +
                            "flag_cancellato" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("flag_cancellato") + "\n" +
                            "flag_migrato" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("flag_migrato") + "\n" +
                            "gracq" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("gracq") + "\n" +
                            "system_id" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("system_id") + "\n" +
                            "mandante" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("mandante") + "\n" +
                            "migrato_da" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("migrato_da") + "\n" +
                            "old_bo_bu" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("old_bo_bu") + "\n" +
                            "padre" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("padre") + "\n" +
                            "sid" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("sid") + "\n" +
                            "sigla" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("sigla") + "\n" +
                            "bo_pu_chronid_ref" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("bo_pu_chronid_ref") + "\n" +
                            "ente_emittente_chronid_ref" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("ente_emittente_chronid_ref") + "\n" +
                            "has_correlazioni" + config.getCSVSeparator() + childDocument.getProperties().getBooleanValue("has_correlazioni") + "\n" +
                            "has_riferimenti" + config.getCSVSeparator() + childDocument.getProperties().getBooleanValue("has_riferimenti") + "\n" +
                            "buyer" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("buyer") + "\n" +
                            "codice_negoziazione" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("codice_negoziazione") + "\n" +
                            "fornitore" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("fornitore") + "\n" +
                            "importo" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("importo") + "\n" +
                            "bo_bu_chronid_ref" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("bo_bu_chronid_ref") + "\n" +
                            "cancellato_str" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("cancellato_str") + "\n" +
                            "origine" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("origine") + "\n" +
                            "fornitori_chronid_ref" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("fornitori_chronid_ref") + "\n"
            );
        } catch (IOException e) {
            logger.error("UNABLE TO CREATE/WRITE FILE: " + fileMetaData, e);
        }
    }

    /**
     * Metodo atto a creare un file d'appoggio che ne contiene informazioni di meta dati degli allegati al contratto
     *
     * @param childDocument classe documentale
     * @param fileMetaData  nome del file da creare
     */
    private static void createACQAllDocContrattoMetadata(Document childDocument, String fileMetaData) {
        try (BufferedWriter writeMetaData = new BufferedWriter(new FileWriter(fileMetaData, true))) {
            writeMetaData.write(
                    "ID" + config.getCSVSeparator() + childDocument.getProperties().getIdValue("ID") + "\n" +
                            "DocumentTitle" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("DocumentTitle") + "\n" +
                            "bu_rda" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("bu_rda") + "\n" +
                            "bu_societa" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("bu_societa") + "\n" +
                            "codice" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("codice") + "\n" +
                            "codice_oda" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("codice_oda") + "\n" +
                            "codice_rda" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("codice_rda") + "\n" +
                            "ente_emittente" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("ente_emittente") + "\n" +
                            "fornitori_not_anag" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("fornitori_not_anag") + "\n" +
                            "tipo_documento" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("tipo_documento") + "\n" +
                            "description_full" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("description_full") + "\n" +
                            "tipo_autorizzazione" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("tipo_autorizzazione") + "\n" +
                            "stato_conservazione" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("stato_conservazione") + "\n" +
                            "stato" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("stato") + "\n" +
                            "codice_contratto" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("codice_contratto") + "\n" +
                            "data_allegato" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_allegato") + "\n" +
                            "data_decorrenza" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_decorrenza") + "\n" +
                            "fornitori" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("fornitori") + "\n" +
                            "ragione_sociale" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("ragione_sociale") + "\n" +
                            "sede" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("sede") + "\n" +
                            "riservatezza" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("riservatezza") + "\n" +
                            "pubblicabile" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("pubblicabile") + "\n" +
                            "assegnatario" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("assegnatario") + "\n" +
                            "data_documento" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_documento") + "\n" +
                            "dimensione" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("dimensione") + "\n" +
                            "bo_gm" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("bo_gm") + "\n" +
                            "strutture_organizzative" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("strutture_organizzative") + "\n" +
                            "note" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("note") + "\n" +
                            "tipologie_trattative" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("tipologie_trattative") + "\n" +
                            "tipologie" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("tipologie") + "\n" +
                            "valore" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("valore") + "\n" +
                            "data_scadenza_contratto" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_scadenza_contratto") + "\n" +
                            "data_invio" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_invio") + "\n" +
                            "data_partenza" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_partenza") + "\n" +
                            "firmatario" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("firmatario") + "\n" +
                            "numero_protocollo" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("numero_protocollo") + "\n" +
                            "riferimento_nomina" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("riferimento_nomina") + "\n" +
                            "societa_beneficiarie" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("societa_beneficiarie") + "\n" +
                            "pu" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("pu") + "\n" +
                            "struttura_autorizzata" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("struttura_autorizzata") + "\n" +
                            "trattamenti" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("trattamenti") + "\n" +
                            "link_doc_esterno" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("link_doc_esterno") + "\n" +
                            "archive_type" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("archive_type") + "\n" +
                            "contesto" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("contesto") + "\n" +
                            "numero_documento" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("numero_documento") + "\n" +
                            "system_id" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("system_id") + "\n" +
                            "hash_allegato" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("hash_allegato") + "\n" +
                            "is_pec" + config.getCSVSeparator() + childDocument.getProperties().getBooleanValue("is_pec") + "\n" +
                            "bo_pu_chronid_ref" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("bo_pu_chronid_ref") + "\n" +
                            "autore" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("autore") + "\n" +
                            "tipo_negoz" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("tipo_negoz") + "\n" +
                            "bu_all_chronid_ref" + config.getCSVSeparator() + Arrays.toString(childDocument.getProperties().getStringListValue("bu_all_chronid_ref").toArray()) + "\n" +
                            "gm_all_chronid_ref" + config.getCSVSeparator() + Arrays.toString(childDocument.getProperties().getStringListValue("gm_all_chronid_ref").toArray()) + "\n" +
                            "strut_aut_all_chronid_ref" + config.getCSVSeparator() + Arrays.toString(childDocument.getProperties().getStringListValue("strut_aut_all_chronid_ref").toArray()) + "\n" +
                            "fornitori_all_chronid_ref" + config.getCSVSeparator() + Arrays.toString(childDocument.getProperties().getStringListValue("fornitori_all_chronid_ref").toArray()) + "\n" +
                            "ente_all_chronid_ref" + config.getCSVSeparator() + Arrays.toString(childDocument.getProperties().getStringListValue("ente_all_chronid_ref").toArray()) + "\n" +
                            //                        "bo_id"+config.getCSVSeparator() + childDocument.getProperties().getStringValue("bo_id") + "\n" +
                            "has_riferimenti" + config.getCSVSeparator() + childDocument.getProperties().getBooleanValue("has_riferimenti") + "\n" +
                            "autore_str" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("autore_str") + "\n" +
                            "ente_emittente_chronid_ref" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("ente_emittente_chronid_ref") + "\n" +
                            "fornitori_chronid_ref" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("fornitori_chronid_ref") + "\n" +
                            "bo_bu_chronid_ref" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("bo_bu_chronid_ref") + "\n" +
                            "data_conservazione" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_conservazione") + "\n" +
                            "data_emissione" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_emissione") + "\n" +
                            "responsabile_conservazione" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("responsabile_conservazione") + "\n" +
                            "volume_conservazione" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("volume_conservazione") + "\n"
            );
        } catch (IOException e) {
            logger.error("UNABLE TO CREATE/WRITE FILE: " + fileMetaData, e);
        }
    }

    /**
     * Metodo atto a creare un file d'appoggio che ne contiene informazioni di meta dati del contratto
     *
     * @param childDocument classe documentale
     * @param fileMetaData  nome del file da creare
     */
    private static void createACQContrattoMetadata(Document childDocument, String fileMetaData) {
        try (BufferedWriter writeMetaData = new BufferedWriter(new FileWriter(fileMetaData, true))) {
            writeMetaData.write("ID" + config.getCSVSeparator() + childDocument.getProperties().getIdValue("ID") + "\n" +
                    "DocumentTitle" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("DocumentTitle") + "\n" +
                    "bu_societa" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("bu_societa") + "\n" +
                    "codice" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("codice") + "\n" +
                    "codice_oda" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("codice_oda") + "\n" +
                    "codice_rda" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("codice_rda") + "\n" +
                    "ente_emittente" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("ente_emittente") + "\n" +
                    "fornitori_not_anag" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("fornitori_not_anag") + "\n" +
                    "tipo_documento" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("tipo_documento") + "\n" +
                    "description_full" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("description_full") + "\n" +
                    "tipo_autorizzazione" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("tipo_autorizzazione") + "\n" +
                    "stato_conservazione" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("stato_conservazione") + "\n" +
                    "stato" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("stato") + "\n" +
                    "codice_contratto" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("codice_contratto") + "\n" +
                    "data_allegato" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_allegato") + "\n" +
                    "data_decorrenza" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_decorrenza") + "\n" +
                    "fornitori" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("fornitori") + "\n" +
                    "ragione_sociale" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("ragione_sociale") + "\n" +
                    "sede" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("sede") + "\n" +
                    "riservatezza" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("riservatezza") + "\n" +
                    "pubblicabile" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("pubblicabile") + "\n" +
                    "assegnatario" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("assegnatario") + "\n" +
                    "data_documento" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_documento") + "\n" +
                    "dimensione" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("dimensione") + "\n" +
                    "bo_gm" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("bo_gm") + "\n" +
                    "strutture_organizzative" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("strutture_organizzative") + "\n" +
                    "note" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("note") + "\n" +
                    "tipologie_trattative" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("tipologie_trattative") + "\n" +
                    "tipologie" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("tipologie") + "\n" +
                    "valore" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("valore") + "\n" +
                    "data_scadenza_contratto" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_scadenza_contratto") + "\n" +
                    "data_invio" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_invio") + "\n" +
                    "data_partenza" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_partenza") + "\n" +
                    "firmatario" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("firmatario") + "\n" +
                    "numero_protocollo" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("numero_protocollo") + "\n" +
                    "riferimento_nomina" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("riferimento_nomina") + "\n" +
                    "societa_beneficiarie" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("societa_beneficiarie") + "\n" +
                    "pu" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("pu") + "\n" +
                    "struttura_autorizzata" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("struttura_autorizzata") + "\n" +
                    "trattamenti" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("trattamenti") + "\n" +
                    "link_doc_esterno" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("link_doc_esterno") + "\n" +
                    "archive_type" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("archive_type") + "\n" +
                    "contesto" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("contesto") + "\n" +
                    "numero_documento" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("numero_documento") + "\n" +
                    "system_id" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("system_id") + "\n" +
                    "hash_allegato" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("hash_allegato") + "\n" +
                    "is_pec" + config.getCSVSeparator() + childDocument.getProperties().getBooleanValue("is_pec") + "\n" +
                    "bo_pu_chronid_ref" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("bo_pu_chronid_ref") + "\n" +
                    "autore" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("autore") + "\n" +
                    "tipo_negoz" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("tipo_negoz") + "\n" +
                    "bu_all_chronid_ref" + config.getCSVSeparator() + Arrays.toString(childDocument.getProperties().getStringListValue("bu_all_chronid_ref").toArray()) + "\n" +
                    "gm_all_chronid_ref" + config.getCSVSeparator() + Arrays.toString(childDocument.getProperties().getStringListValue("gm_all_chronid_ref").toArray()) + "\n" +
                    "strut_aut_all_chronid_ref" + config.getCSVSeparator() + Arrays.toString(childDocument.getProperties().getStringListValue("strut_aut_all_chronid_ref").toArray()) + "\n" +
                    "fornitori_all_chronid_ref" + config.getCSVSeparator() + Arrays.toString(childDocument.getProperties().getStringListValue("fornitori_all_chronid_ref").toArray()) + "\n" +
                    "ente_all_chronid_ref" + config.getCSVSeparator() + Arrays.toString(childDocument.getProperties().getStringListValue("ente_all_chronid_ref").toArray()) + "\n" +
                    //                        "bo_id"+config.getCSVSeparator() + childDocument.getProperties().getStringValue("bo_id") + "\n" +
                    "has_riferimenti" + config.getCSVSeparator() + childDocument.getProperties().getBooleanValue("has_riferimenti") + "\n" +
                    "autore_str" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("autore_str") + "\n" +
                    "esito_conferma_fornitore" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("esito_conferma_fornitore") + "\n" +
                    "buyer" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("buyer") + "\n" +
                    "codice_negoziazione" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("codice_negoziazione") + "\n" +
                    "cognome_cancellazione_richiesta" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("cognome_cancellazione_richiesta") + "\n" +
                    "data_cancellazione_richiesta" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_cancellazione_richiesta") + "\n" +
                    "data_conferma_fornitore" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_conferma_fornitore") + "\n" +
                    "data_conservazione" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_conservazione") + "\n" +
                    "data_emissione" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_emissione") + "\n" +
                    "data_scadenza" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_scadenza") + "\n" +
                    "divisione" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("divisione") + "\n" +
                    "enti_vis_testo_di_contratto" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("enti_vis_testo_di_contratto") + "\n" +
                    "importo" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("importo") + "\n" +
                    "nome_cancellazione_richiesta" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("nome_cancellazione_richiesta") + "\n" +
                    "pu_assegnataria" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("pu_assegnataria") + "\n" +
                    "proivenienza" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("proivenienza") + "\n" +
                    "responsabile_conservazione" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("responsabile_conservazione") + "\n" +
                    "guid_riferimento_documento_testo_srm" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("guid_riferimento_documento_testo_srm") + "\n" +
                    "guid_riferimento_documento_srm" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("guid_riferimento_documento_srm") + "\n" +
                    "codice_tipologia_ritrascrizione" + config.getCSVSeparator() + childDocument.getProperties().getInteger32Value("codice_tipologia_ritrascrizione") + "\n" +
                    "stato_richiesta_conferma" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("stato_richiesta_conferma") + "\n" +
                    "volume_conservazione" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("volume_conservazione") + "\n" +
                    "sid" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("sid") + "\n" +
                    "guid_srm" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("guid_srm") + "\n" +
                    "data_variante" + config.getCSVSeparator() + childDocument.getProperties().getDateTimeValue("data_variante") + "\n" +
                    "variante" + config.getCSVSeparator() + childDocument.getProperties().getBooleanValue("variante") + "\n" +
                    "id_file_ra" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("id_file_ra") + "\n" +
                    "gruppo_acquisti" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("gruppo_acquisti") + "\n" +
                    "tipo_dati_personali" + config.getCSVSeparator() + childDocument.getProperties().getStringValue("tipo_dati_personali") + "\n"
            );
        } catch (IOException e) {
            logger.error("UNABLE TO CREATE/WRITE FILE: " + fileMetaData, e);
        }
    }

    /**
     * Metodo atto a creare il file con definizione delle colonne
     *
     * @param dataFileName passo il nome del file csv colonne cui da definire
     */
    private static void defineCSVColumns(String dataFileName) {
        try (BufferedWriter createFile = new BufferedWriter(new FileWriter(dataFileName, true))) {
            createFile.write("OLD_NAME" + config.getCSVSeparator() +
                    "RETRIEVAL_NAME" + config.getCSVSeparator() +
                    "DOCTYPE" + config.getCSVSeparator() +
                    "DESCRIPTION_FULL" + config.getCSVSeparator() +
                    "MIME_TYPE" + config.getCSVSeparator() +
                    "EXPORT_STATUS" + config.getCSVSeparator() +
                    "NEW_NAME" + config.getCSVSeparator() + "IMPORT_STATUS\n");
        } catch (IOException e) {
            logger.error("UNABLE TO CREATE " + dataFileName, e);
        }
    }

    /**
     * Metodo atto ad allegare i file fisici alla classe documentale esistente o appena creata
     *
     * @param dataFile     ne contiene il path del file da caricare
     * @param j            indice
     * @param document     classe documentale
     * @param symbolicName nome simbolico della classe documentale
     */
    private static void attachFile(List<String> dataFile, int j, Document document, String symbolicName) {
        String path = dataFile.get(j).split(config.getCSVSeparator())[0];
        try {
            if (!symbolicName.equals("acq_pos_contratto")) {
                logger.info("Creating attachments: " + path + " " + document.getProperties().getIdValue("ID") + " symbolicName: " + symbolicName);
                ContentTransfer contentTransfer = Factory.ContentTransfer.createInstance();
                File file = new File(path);
                FileInputStream fileInputStream = new FileInputStream(file);
                contentTransfer.setCaptureSource(fileInputStream);
                contentTransfer.set_ContentType(dataFile.get(j).split(config.getCSVSeparator())[4]);
                if (!dataFile.get(j).split(config.getCSVSeparator())[1].isEmpty()) {
                    contentTransfer.set_RetrievalName(dataFile.get(j).split(config.getCSVSeparator())[1]);
                }
                //noinspection deprecation
                ContentElementList contentElementList = Factory.ContentTransfer.createList();
                //noinspection unchecked
                contentElementList.add(contentTransfer);
                document.set_ContentElements(contentElementList);
            }
            String[] line = new String[0];
            Document doc = null;
            if (
                    document.getClassName().equals("acq_all_contratto") ||
                            document.getClassName().equals("acq_all_doc_contratto")
            ) {
                BufferedReader reader = new BufferedReader(new FileReader(dataFile.get(j).split(config.getCSVSeparator())[0] + "_metadata"));
                logger.info("Reading file on : " + symbolicName + " " + dataFile.get(j).split(config.getCSVSeparator())[0]);
                while (reader.ready()) {
                    try {
                        line = reader.readLine().split(config.getCSVSeparator());
                        if (line[0].equals("ID")) {
                            doc = Factory.Document.fetchInstance(config.getObjectStore(), line[1], null);
                        }
                        //Gestione degli interi
                        if (line[0].equals("stato_conservazione") ||
                                line[0].equals("dimensione") ||
                                line[0].equals("strutture_organizzative") ||
                                line[0].equals("tipologie_trattative") ||
                                line[0].equals("valore") ||
                                line[0].equals("tipo_negoz") ||
                                line[0].equals("tipologie") ||
                                line[0].equals("system_id") ||
                                line[0].equals("ente_emittente") ||
                                line[0].equals("stato") ||
                                line[0].equals("autore") ||
                                line[0].equals("codice_tipologia_ritrascrizione") ||
                                line[0].equals("tipo_documento") ||
                                line[0].equals("fornitori") ||
                                line[0].equals("contesto") ||
                                line[0].equals("bo_gm") ||
                                line[0].equals("bo_bu") ||
                                line[0].equals("bo_ente") ||
                                line[0].equals("fk1") ||
                                line[0].equals("fk2") ||
                                line[0].equals("flag_allegati") ||
                                line[0].equals("flag_cancellato") ||
                                line[0].equals("old_bo_bu") ||
                                line[0].equals("padre") ||
                                line[0].equals("fornitore") ||
                                line[0].equals("origine") ||
                                line[0].equals("bu_rda")) {
                            if (!line[1].equals("null")) {
                                document.getProperties().putValue(line[0], Objects.requireNonNull(doc).getProperties().getInteger32Value(line[0]));
                            }
                            //Gestione delle Date
                        } else if (line[0].equals("data_decorrenza") ||
                                line[0].equals("data_invio") ||
                                line[0].equals("data_emissione") ||
                                line[0].equals("data_conferma_fornitore") ||
                                line[0].equals("data_variante") ||
                                line[0].equals("data_allegato") ||
                                line[0].equals("data_scadenza_contratto") ||
                                line[0].equals("data_cancellazione_richiesta") ||
                                line[0].equals("data_partenza") ||
                                line[0].equals("data_conservazione") ||
                                line[0].equals("data_documento") ||
                                line[0].equals("data_scadenza") ||
                                line[0].equals("creation_date") ||
                                line[0].equals("data_fin_val") ||
                                line[0].equals("data_in_val")) {
                            if (!line[1].equals("null")) {
                                document.getProperties().putValue(line[0], Objects.requireNonNull(doc).getProperties().getDateTimeValue(line[0]));
                            }
                        } else if (line[0].equals("bu_all_chronid_ref") ||
                                line[0].equals("gm_all_chronid_ref") ||
                                line[0].equals("strut_aut_all_chronid_ref") ||
                                line[0].equals("fornitori_all_chronid_ref") ||
                                line[0].equals("ente_all_chronid_ref")) {
                            document.getProperties().putValue(line[0], Objects.requireNonNull(doc).getProperties().getStringListValue(line[0]));
                        } else if (line[0].equals("ID")) {
                            logger.info("ID is Read-Only");
                            //Gestione degli Booleani
                        } else if (line[0].equals("is_pec") ||
                                line[0].equals("variante") ||
                                line[0].equals("has_riferimenti") ||
                                line[0].equals("has_correlazioni")) {
                            document.getProperties().putValue(line[0], Objects.requireNonNull(doc).getProperties().getBooleanValue(line[0]));
                        } else if (line[0].equals("DocumentTitle")) {
                            document.getProperties().putValue(line[0], line[1]);
                        } else {
                            if (!line[0].isEmpty() && !line[1].equals("null")) {
                                document.getProperties().putValue(line[0], line[1]);
                            }
                        }
                    } catch (IOException e) {
                        logger.error("IO Exception OCCURED", e);
                    } catch (NumberFormatException e) {
                        logger.error("CHECK INPUT DATA FOR: " + line[0] + " AGAINST: " + line[1], e);
                    }
                }
                reader.close();
            }
            //Pubblico documento gia' in stato Release
            document.checkin(AutoClassify.DO_NOT_AUTO_CLASSIFY, CheckinType.MAJOR_VERSION);
            document.save(RefreshMode.REFRESH);
            logger.info("Saving attachments of: " + symbolicName + " successful, GUID document: " + document.getProperties().getIdValue("ID"));
        } catch (FileNotFoundException e) {
            logger.error("THERE`S NO FILE FOUND AT PATH: " + path + " UNABLE TO UPLOAD.", e);
        } catch (IOException e) {
            logger.error("SOMETHING WENT WRONG WITH READING FILE: " + dataFile.get(j).split(config.getCSVSeparator())[0] + "_metadata", e);
        }
    }
}
