package r2u.tools.downloader;

import com.filenet.api.collection.ContentElementList;
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

import javax.activation.MimetypesFileTypeMap;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.*;

public class SdiDownloader {
    private final static Logger logger = Logger.getLogger(SdiDownloader.class.getName());
    private final static Configurator config = Configurator.getInstance();

    /**
     * Metodo atto a svolgere attività di download dei documenti fisici.
     * Nella fase uno dell'export attualmente è previsto che lui crei la cartella che avr&agrave; il nome del bo_id del padre e poi scarica la dentro il contenuto ossia i file fisici
     * Nella fase due dell'import è previsto che lui carica i file fisici al figlio del acq_contratto e qualora non ci fosse lo crea. Consultando prima per&ograve; Relazione SDI (acq_relation)
     */
    public void startDownload() {
        Document fetchedDocument = null;
        final String dataFileName = "data.csv";
        final String outputFileName = "output.csv";
        Path dataCSV = Paths.get(dataFileName);
        if (config.isExport()) {
            //Creo un file csv con intestazione, ma ancora prima verifico se esiste.
            if (Files.exists(dataCSV)) {
                try {
                    Files.delete(dataCSV);
                    try (BufferedWriter createFile = new BufferedWriter(new FileWriter(dataFileName, true))) {
                        createFile.write("OLD_NAME" + config.getCSVSeparator() +
                                "DOCTYPE" + config.getCSVSeparator() +
                                "DESCRIPTION_FULL" + config.getCSVSeparator() +
                                "EXPORT_STATUS" + config.getCSVSeparator() +
                                "NEW_NAME" + config.getCSVSeparator() + "IMPORT_STATUS\n");
                    } catch (IOException e) {
                        logger.error("UNABLE TO CREATE data.csv", e);
                    }
                } catch (IOException e) {
                    logger.error("UNABLE TO DELETE data.csv", e);
                }
            } else {
                try (BufferedWriter createFile = new BufferedWriter(new FileWriter(dataFileName, true))) {
                    createFile.write("OLD_NAME" + config.getCSVSeparator() +
                            "DOCTYPE" + config.getCSVSeparator() +
                            "DESCRIPTION_FULL" + config.getCSVSeparator() +
                            "EXPORT_STATUS" + config.getCSVSeparator() +
                            "NEW_NAME" + config.getCSVSeparator() + "IMPORT_STATUS\n");
                } catch (IOException e) {
                    logger.error("UNABLE TO CREATE data.csv", e);
                }
            }
            //Fase 1: scarico il documento padre
            Iterator<?> contractsIterator = DataFetcher.fetchDataByQuery(config.getQuery(), config.getObjectStore());
            while (contractsIterator != null && contractsIterator.hasNext()) {
                try {
                    //Qui non faccio altro che: scaricare comunque i documenti che possa contenere il padre (nun si sa mai)
                    RepositoryRow headRepositoryRow = (RepositoryRow) contractsIterator.next();
                    Properties headProperties = headRepositoryRow.getProperties();
                    fetchedDocument = Factory.Document.fetchInstance(config.getObjectStore(), headProperties.getIdValue("ID"), null);
                    logger.info("Found document: " + headProperties.getIdValue("ID") + " bo_id: " + headProperties.getStringValue("bo_id"));
                    ContentElementList contentElements = fetchedDocument.get_ContentElements();
                    if (!contentElements.isEmpty()) {
                        for (Object docContentElement : contentElements) {
                            ContentTransfer contentTransfer = (ContentTransfer) docContentElement;
                            InputStream inputStream = contentTransfer.accessContentStream();
                            String folderHeadName = headProperties.getStringValue("bo_id") + "/";
                            logger.info("Trying to create folder under path: " + config.getPath() + "/" + folderHeadName);
                            FileUtils.copyInputStreamToFile(inputStream, new File(config.getPath() + folderHeadName));
                        }
                    } else {
                        logger.info("There's no document associated");
                    }
                    //Fase 2: scarico il documento figlio
                    //Mi rivolgo a Relazioni SDI (acq_relation) avendo GUID del padre,
                    //Ne ricavo i suoi figli e x ogni figlio:
                    //Creo cartella con: "bo_id del padre/classe_documentale/filename"
                    Iterator<?> childContractsIterator = DataFetcher.executeQuery(
                            config.getObjectStore() /*object store*/,
                            "tail_chronicle_id" /*colonna*/,
                            "acq_relation" /*tabella*/,
                            "head_chronicle_id" /*colonna di ricerca*/,
                            headProperties.getIdValue("ID").toString() /*valore di ricerca*/
                    );
                    while (childContractsIterator != null && childContractsIterator.hasNext()) {
                        RepositoryRow childRepositoryRow = (RepositoryRow) childContractsIterator.next();
                        Properties childProperties = childRepositoryRow.getProperties();
                        Document childDocument = Factory.Document.fetchInstance(config.getObjectStore(),
                                childProperties.getIdValue("tail_chronicle_id"), null);
                        ContentElementList childContentElement = childDocument.get_ContentElements();
                        if (!childContentElement.isEmpty()) {
                            for (Object docContentElement : childContentElement) {
                                ContentTransfer contentTransfer = (ContentTransfer) docContentElement;
                                InputStream inputStream = contentTransfer.accessContentStream();
                                String line;
                                String fileName = config.getPath() + headProperties.getStringValue("bo_id") + "/" + childDocument.getClassName() + "/" + childDocument.get_Name();
                                String folderPath = config.getPath() + headProperties.getStringValue("bo_id") + "/" + childDocument.getClassName() + "/";

                                //Qui produco un file diciamo temporaneo di riepilogo del download
                                //Formato e`: OLD_NAME, DOCTYPE, DESCRIPTION_FULL, EXPORT_STATUS
                                if (Files.notExists(Paths.get(fileName))) {
                                    logger.info("Trying to save file under path: " + folderPath);
                                    FileUtils.copyInputStreamToFile(inputStream, new File(fileName));
                                    line = fileName + config.getCSVSeparator() + childDocument.getClassName() + config.getCSVSeparator() + childProperties.getStringValue("description_full") + config.getCSVSeparator() + "STATUS_OK" + config.getCSVSeparator() + "\n";
                                    BufferedWriter writeData = new BufferedWriter(new FileWriter(dataFileName, true));
                                    writeData.write(line);
                                    writeData.close();
                                } else {
                                    logger.warn("THERES FILE ALREADY WITH THE SAME NAME: " + childDocument.get_Name());
                                    logger.info("Trying to save file under path: " + folderPath);
                                    //X gestire i file con lo stesso name
                                    Format formatter = new SimpleDateFormat(Constants.printableTimeFormat);
                                    FileUtils.copyInputStreamToFile(inputStream, new File(folderPath + formatter.format(new Date()) + "_" + childDocument.get_Name()));
                                    line = folderPath + formatter.format(new Date()) + "_" + childDocument.get_Name() + config.getCSVSeparator() + childDocument.getClassName() + config.getCSVSeparator() + childProperties.getStringValue("description_full") + config.getCSVSeparator() + "STATUS_OK" + config.getCSVSeparator() + "\n";
                                    BufferedWriter writeData = new BufferedWriter(new FileWriter(dataFileName, true));
                                    writeData.write(line);
                                    writeData.close();
                                }
                            }
                        } else {
                            logger.info("There's no child document associated with head document");
                        }
                    }
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
        if (config.isImport()) {
            List<String> output = new ArrayList<>();
            final String ok = ",STATUS_OK", ko = ",STATUS_KO";
            String path = "";
            if (Files.notExists(dataCSV)) {
                logger.error("THERE'S NO data.csv FOUND. PERHAPS U FORGOT TO DO EXPORT FIRST.");
                return;
            }
            try {
                //Qui leggo due file: uno creato nella fase dell'export e altro per leggere sia il vecchio bo_id che nuovo
                List<String> importFile = FileUtils.readLines(config.getFileImport(), "UTF-8");
                List<String> dataFile = FileUtils.readLines(new File(dataFileName), "UTF-8");
                output.add(dataFile.get(0));
                //File di import
                for (int i = 1; i < importFile.size(); i++) {
                    //File prodotto
                    for (int j = 1; j < dataFile.size(); j++) {
                        //Verifico se combaciano i dati (OLD BO_ID e NEW BO_ID)
                        //noinspection GrazieInspection
                        try {
                            //Confronto tra i contenuti di due file e se ne trovo la corrispondenza per bo_id,
                            //allora ne prostituisco per poi scrivere nuova informazione nel file di riepilogo "output.csv".
                            if (dataFile.get(j).contains(importFile.get(i).split(config.getCSVSeparator())[0])) {
                                String replace = dataFile.get(j).replace(importFile.get(i).split(
                                                config.getCSVSeparator())[0],//old bo_id
                                        importFile.get(i).split(config.getCSVSeparator())[1]//new bo_id
                                );
                                path = dataFile.get(j).concat(replace.split(config.getCSVSeparator())[0]);
                                String s = importFile.get(i).split(config.getCSVSeparator())[1];
                                //Ricerco il contratto per bo_id nuovo
                                Iterator<?> mainDocumentIterator = DataFetcher.executeQuery(
                                        config.getObjectStore(),
                                        "",
                                        "acq_contratto",
                                        "bo_id",
                                        "'" + s + "'"
                                );
                                while (mainDocumentIterator != null && mainDocumentIterator.hasNext()) {
                                    try {
                                        RepositoryRow repositoryRow = (RepositoryRow) mainDocumentIterator.next();
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
                                                mainDocument.getProperties().getIdValue("ID").toString());
                                        if (childDocumentIterator != null && childDocumentIterator.hasNext()) {
                                            //Se ci sono allora allego i file fisici al figlio/figli
                                            while (childDocumentIterator.hasNext()) {
                                                String classId = dataFile.get(j).split(config.getCSVSeparator())[1];
                                                logger.info("Creating a new document of: " + classId);
                                                RepositoryRow childRepositoryRow = (RepositoryRow) childDocumentIterator.next();
                                                Properties childRepositoryRowProperties = childRepositoryRow.getProperties();
                                                Document childDocument = Factory.Document.fetchInstance(config.getObjectStore(), childRepositoryRowProperties.getIdValue("tail_chronicle_id"), null);
                                                attachFile(dataFile, j, childDocument, classId);
                                            }
                                        } else {
                                            //Se non ci sono, allora creo "acq_all_contratto" allego i file e acq_relationship.
                                            //Indicando il tipo di relazione tramite description_full.
                                            //La description_full la ottengo dal file "data.csv" che serve x lavorare localmente.
                                            String classId = dataFile.get(j).split(config.getCSVSeparator())[1];
                                            logger.info("Creating a new document of: " + classId);
                                            Document newDocument = Factory.Document.createInstance(config.getObjectStore(), classId);
                                            attachFile(dataFile, j, newDocument, classId);
                                            logger.info("Creating relationship between: " + mainDocument.getClassName() + "/" + mainDocument.getProperties().getIdValue("ID")
                                                    + " and " + newDocument.getClassName() + "/" + newDocument.getProperties().getIdValue("ID"));
                                            CustomObject acqRelation = Factory.CustomObject.createInstance(config.getObjectStore(), "acq_relation");
                                            acqRelation.getProperties().putValue("tail_chronicle_id", newDocument.getProperties().getIdValue("ID"));
                                            acqRelation.getProperties().putValue("head_chronicle_id", mainDocument.getProperties().getIdValue("ID"));
                                            acqRelation.getProperties().putValue("description_full", dataFile.get(j).split(config.getCSVSeparator())[2]);
                                            acqRelation.save(RefreshMode.REFRESH);
                                            logger.info("Saving relationship is successful");
                                        }
                                    } catch (Exception e) {
                                        logger.error("SOMETHING WENT WRONG", e);
                                        output.add(path + ko);
                                    }
                                }
                                String concat = path + ok;
                                output.add(concat);
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
                    logger.error("UNABLE TO CREATE data.csv", e);
                }
            } catch (Exception e) {
                logger.error("SOMETHING WENT WRONG", e);
            }
        }
    }

    private static void attachFile(List<String> dataFile, int j, Document childDocument, String classId) {
        String path = dataFile.get(j).split(config.getCSVSeparator())[0];
        try {
            logger.info("Creating attachments: " + path);
            ContentTransfer contentTransfer = Factory.ContentTransfer.createInstance();
            File file = new File(path);
            FileInputStream fileInputStream = new FileInputStream(file);
            contentTransfer.setCaptureSource(fileInputStream);
            contentTransfer.set_ContentType(new MimetypesFileTypeMap().getContentType(file));
            //noinspection deprecation
            ContentElementList contentElementList = Factory.ContentTransfer.createList();
            //noinspection unchecked
            contentElementList.add(contentTransfer);
            childDocument.set_ContentElements(contentElementList);
            childDocument.save(RefreshMode.REFRESH);
            logger.info("Saving attachments of: " + classId + " successful, GUID document: " + childDocument.getProperties().getIdValue("ID"));
        } catch (FileNotFoundException e) {
            logger.error("THERE`S NO FILE FOUND AT PATH: " + path + " UNABLE TO UPLOAD.", e);
        }
    }
}
