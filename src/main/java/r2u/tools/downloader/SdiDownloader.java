package r2u.tools.downloader;

import com.filenet.api.collection.ContentElementList;
import com.filenet.api.core.ContentTransfer;
import com.filenet.api.core.CustomObject;
import com.filenet.api.core.Document;
import com.filenet.api.core.Factory;
import com.filenet.api.property.Properties;
import com.filenet.api.query.RepositoryRow;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import r2u.tools.config.Configurator;
import r2u.tools.utils.DataFetcher;

import java.io.*;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;

public class SdiDownloader {
    private final static Logger logger = Logger.getLogger(SdiDownloader.class.getName());
    private final Configurator instance = Configurator.getInstance();

    /**
     * Metodo atto a svolgere attività di download dei documenti fisici.
     * Nella fase uno attualmente è previsto che lui crei la cartella che avr&agrave; il nome del bo_id del padre
     * Nella fase due è previsto che lui scarica dei documenti legati al padre ricavati tramite acq_relation (Relazione SDI)
     * @throws IOException scatta quando salta fuori qualsiasi cosa non gestita
     */
    public void startDownload() throws Exception {
        Document fetchedDocument = null;
        for (String docClass : instance.getDocumentClass()) {
            switch (docClass) {
                default: {
                    logger.info("CustomObject & Folder are not implemented!");
                }
                break;
                case "Document": {
                    //Creo un file csv con intestazione:
                    BufferedWriter createFile = new BufferedWriter(new FileWriter("data.csv", true));
                    createFile.write("NOME_CARTELLA;FILE_PATH\n");
                    createFile.close();
                    //Fase 1: scarico il documento padre
                    Iterator<?> contractsIterator = DataFetcher.fetchDataByQuery(instance.getQuery(), instance.getObjectStore());
                    while (contractsIterator != null && contractsIterator.hasNext()) {
                        try {
                            RepositoryRow headRepositoryRow = (RepositoryRow) contractsIterator.next();
                            Properties headProperties = headRepositoryRow.getProperties();
                            fetchedDocument = Factory.Document.fetchInstance(instance.getObjectStore(), headProperties.getIdValue("ID"), null);
                            logger.info("Found document: " + headProperties.getIdValue("ID") + " bo_id: " + headProperties.getStringValue("bo_id"));
                            ContentElementList contentElements = fetchedDocument.get_ContentElements();
                            if (!contentElements.isEmpty()) {
                                for (Object docContentElement : contentElements) {
                                    ContentTransfer contentTransfer = (ContentTransfer) docContentElement;
                                    InputStream inputStream = contentTransfer.accessContentStream();
                                    String folderHeadName = headProperties.getStringValue("bo_id") + "/";
                                    logger.info("Trying to save file under path: " + instance.getPath() + "/" + folderHeadName);
                                    try {
                                        FileUtils.copyInputStreamToFile(inputStream, new File(instance.getPath() + folderHeadName));
                                        BufferedWriter writeData = new BufferedWriter(new FileWriter("data.csv", true));
                                        writeData.write(headProperties.getStringValue("bo_id") + ";" + instance.getPath() + headProperties.getStringValue("bo_id") + "\n");
                                        writeData.close();
                                    } catch (IOException e) {
                                        logger.error("Unable to save file.", e);
                                    }
                                }
                            } else {
                                logger.info("There's no document associated");
                            }
                            //Fase 2: scarico il documento figlio
                            Iterator<?> childContractsIterator = DataFetcher.fetchTailIdByRelationHeadId(headProperties.getIdValue("ID").toString(), instance.getObjectStore());
                            while (childContractsIterator != null && childContractsIterator.hasNext()) {
                                RepositoryRow childRepositoryRow = (RepositoryRow) childContractsIterator.next();
                                Properties childProperties = childRepositoryRow.getProperties();
                                Document childDocument = Factory.Document.fetchInstance(instance.getObjectStore(),
                                        childProperties.getIdValue("tail_chronicle_id"), null);
                                ContentElementList childContentElement = childDocument.get_ContentElements();
                                if (!childContentElement.isEmpty()) {
                                    for (Object docContentElement : childContentElement) {
                                        ContentTransfer contentTransfer = (ContentTransfer) docContentElement;
                                        InputStream inputStream = contentTransfer.accessContentStream();
                                        logger.info("Trying to save file under path: " + instance.getPath() + headProperties.getStringValue("bo_id") + "/");
                                        try {
                                            FileUtils.copyInputStreamToFile(inputStream, new File(instance.getPath() + headProperties.getStringValue("bo_id") + "/" + childDocument.get_Name()));
                                            BufferedWriter writeData = new BufferedWriter(new FileWriter("data.csv", true));
                                            writeData.write(headProperties.getStringValue("bo_id") + ";" + instance.getPath() + headProperties.getStringValue("bo_id") + "/" + childDocument.get_Name() + "\n");
                                            writeData.close();
                                        } catch (IOException e) {
                                            logger.error("Unable to save file.", e);
                                        }
                                    }
                                } else {
                                    logger.info("There's no child document associated with head document");
                                }
                            }
                        } catch (Exception exception) {
                            BufferedWriter unManagedErrorsWriter = new BufferedWriter(new FileWriter(Objects.requireNonNull(fetchedDocument).getClassName()
                                    + "_caught_errors.txt", true));
                            unManagedErrorsWriter.write("AN ERROR IS OCCURED WITH DOCUMENT_CLASS: " + fetchedDocument.getClassName() + " ID " + fetchedDocument.getProperties().getIdValue("ID").toString()
                                    + "\nERROR IS: " + exception +
                                    "\nMESSAGE IS: " + exception.getMessage() +
                                    "\nCAUSE IS: " + exception.getCause() +
                                    "\nSTACK TRACE: " + Arrays.toString(exception.getStackTrace()) + "\n");
                            unManagedErrorsWriter.close();
                        }
                    }
                }
                break;
            }
        }
    }
}
