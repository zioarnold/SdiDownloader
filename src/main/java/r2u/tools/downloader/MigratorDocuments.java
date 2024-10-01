package r2u.tools.downloader;

import com.filenet.api.collection.ContentElementList;
import com.filenet.api.constants.AutoClassify;
import com.filenet.api.constants.CheckinType;
import com.filenet.api.constants.RefreshMode;
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

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Classe atto a svolgere delle attività dell'export e dell'import diretto fra gli classi documentali
 */
public class MigratorDocuments {
    private final static Logger logger = Logger.getLogger(MigratorDocuments.class.getName());
    private final static Configurator config = Configurator.getInstance();

    /**
     * Metodo atto a svolgere attività di "migrazione" dei classi documentali e dei file allegati dal vecchio bo_id al nuovo bo_id.
     * In primo luogo, viene letto il file dal {@link MigratorDocuments#config}.getFileImport(). Poi x ciascuna associazione, vengono ricavate
     * bo_id nuovo e vecchio. Per il nuovo bo_id comunque vengono create nuovi classi documentali partendo dal vecchio bo_id.
     * Per dire: se nel acq_contratto vi sono molti acq_all_contratto e varie questi vengono creati al nuovo con metadati e file presenti.
     * Pure allegati degli allegati vengono recepiti: acq_all_doc_contratto.
     */
    public void startDownload() {
        final String outputFileName = "output.csv";
        final String boIdsNotFound = "bo_id.csv";
        FNOpUtils fnOpUtils = new FNOpUtils();
        try {
            //Qui leggo due file: uno creato nella fase dell'export e altro per leggere sia il vecchio bo_id che nuovo
            List<String> importFile = FileUtils.readLines(config.getFileImport(), StandardCharsets.UTF_8);
            importFile = importFile.subList(1, importFile.size());
            //File di import
            for (String row : importFile) {
                logger.info("Looking for new bo_id: " + row.split(config.getCSVSeparator())[1]);
                Iterator<?> newIdMainDocument = DataFetcher.executeQuery(
                        config.getObjectStore(),
                        "",
                        "acq_contratto",
                        "bo_id",
                        "'" + row.split(config.getCSVSeparator())[1]/*new bo_id*/ + "'"
                );
                //Verifico se esiste il nuovo bo_id a sistema
                if (newIdMainDocument.hasNext()) {
                    while (newIdMainDocument.hasNext()) {
                        try {
                            RepositoryRow repositoryRow = (RepositoryRow) newIdMainDocument.next();
                            Properties properties = repositoryRow.getProperties();
                            Document mainDocument = Factory.Document.fetchInstance(
                                    config.getObjectStore(),
                                    properties.getIdValue("ID"),
                                    null);
                            //Cerco di recuperare figlio/figli del nuovo "acq_contratto"
                            Iterator<?> childDocumentIterator = DataFetcher.executeQuery(
                                    config.getObjectStore(),
                                    "tail_chronicle_id",
                                    "acq_relation",
                                    "head_chronicle_id",
                                    mainDocument.getProperties().getIdValue("ID").toString()
                            );
                            //Se ci sono i figli del nuovo bo_id ne creo degli altri prendendo dal vecchio
                            if (childDocumentIterator.hasNext()) {
                                while (childDocumentIterator.hasNext()) {
                                    String newBoId = row.split(config.getCSVSeparator())[1]/*new bo_id*/,
                                            oldBoId = row.split(config.getCSVSeparator())[0]/*old bo_id*/;
                                    //Qui ricerco il vecchio bo_id
                                    logger.info("Info looking for bo_id: " + oldBoId);
                                    Iterator<?> oldBoIdIterator = DataFetcher.fetchDataByQuery(config.getQuery() + " WHERE [bo_id] = " + "'" + oldBoId + "'", config.getObjectStore());
                                    oldBoIdLookup(oldBoIdIterator, newBoId, fnOpUtils, mainDocument, oldBoId);
                                }
                            } else {
                                //Altrimenti ne creo comunque i nuovi figli prendendo i vecchi
                                String newBoId = row.split(config.getCSVSeparator())[1]/*new bo_id*/,
                                        oldBoId = row.split(config.getCSVSeparator())[0]/*old bo_id*/;
                                //Qui ricerco il vecchio bo_id
                                Iterator<?> oldBoIdIterator = DataFetcher.fetchDataByQuery(config.getQuery() + " WHERE [bo_id] = " + "'" + oldBoId + "'", config.getObjectStore());
                                oldBoIdLookup(oldBoIdIterator, newBoId, fnOpUtils, mainDocument, oldBoId);
                            }
                            BufferedWriter createFile = new BufferedWriter(new FileWriter(outputFileName, true));
                            createFile.write(row + "\n");
                            createFile.close();
                        } catch (Exception e) {
                            logger.error("SOMETHING WENT WRONG", e);
                        }
                    }
                } else {
                    try (BufferedWriter createFile = new BufferedWriter(new FileWriter(boIdsNotFound, true))) {
                        createFile.write(row + "\n");
                    } catch (IOException e) {
                        logger.error("UNABLE TO CREATE/WRITE " + boIdsNotFound, e);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("SOMETHING WENT WRONG", e);
        }
    }

    /**
     * Metodo atto a svolgere la ricerca del vecchio bo_id
     *
     * @param oldBoIdIterator iteratore che ne contiene degli oggetti
     * @param newBoId         valore del nuovo bo_id
     * @param fnOpUtils       utility
     * @param mainDocument    documento
     * @param oldBoId         vecchio bo_id
     */
    private static void oldBoIdLookup(Iterator<?> oldBoIdIterator, String newBoId, FNOpUtils fnOpUtils, Document mainDocument, String oldBoId) {
        while (oldBoIdIterator != null && oldBoIdIterator.hasNext()) {
            RepositoryRow oldBoIdRepositoryRow = (RepositoryRow) oldBoIdIterator.next();
            Properties oldBoIdProperties = oldBoIdRepositoryRow.getProperties();
            logger.info("Working with: " + oldBoIdProperties.getIdValue("ID"));
            //Qui ricerco i figli del vecchio bo_id (padre).
            Iterator<?> childIterator = DataFetcher.executeQuery(config.getObjectStore(),
                    "",
                    "acq_relation",
                    "head_chronicle_id",
                    oldBoIdProperties.getIdValue("ID").toString());
            childLookup(newBoId, fnOpUtils, mainDocument, oldBoId, childIterator);
        }
    }

    /**
     * Metodo atto a svolgere attività di ricerca, di popolamento e di allegamento dei file.
     * Leggendo dal {@link MigratorDocuments#oldBoIdLookup(Iterator, String, FNOpUtils, Document, String)} i figli con quale si
     * recepiscono i metadati e i file da creare ex-novo ma nel nuovo contratto padre (bo_id)
     *
     * @param newBoId       nuovo contratto padre
     * @param fnOpUtils     utility
     * @param mainDocument  documento
     * @param oldBoId       vecchio bo_id
     * @param childIterator iteratore che ne contiene i figli
     */
    private static void childLookup(String newBoId, FNOpUtils fnOpUtils, Document mainDocument, String oldBoId, Iterator<?> childIterator) {
        String descriptionFull;
        while (childIterator != null && childIterator.hasNext()) {
            RepositoryRow childRepositoryRow = (RepositoryRow) childIterator.next();
            Properties childProperties = childRepositoryRow.getProperties();
            descriptionFull = childProperties.getStringValue("description_full");
            Document child = Factory.Document.fetchInstance(config.getObjectStore(), childProperties.getIdValue("tail_chronicle_id"), null);
            logger.info("Working with child: " + child.getClassName() + ", " + child.getProperties().getIdValue("ID"));
            Document newChild = Factory.Document.createInstance(config.getObjectStore(), child.getClassName());
            newChild.save(RefreshMode.REFRESH);
            logger.info("Document created, GUID: " + newChild.getProperties().getIdValue("ID").toString() + ", " + newChild.getClassName());
            newChild.getProperties().putValue("bo_id", newBoId);
            //Qui lavoro i documenti in base al tipo di relazione
            switch (descriptionFull) {
                case Constants.BO_2_POS: {
                    logger.info("Description of relation is: " + descriptionFull);
                    fnOpUtils.copyData(child, newChild, descriptionFull);
                    fnOpUtils.createRelationship(mainDocument, newChild, newBoId);
                    break;
                }
                case Constants.CONTRATTO_2_ALL: {
                    logger.info("Description of relation is: " + descriptionFull);
                    if (!child.get_ContentElements().isEmpty()) {
                        logger.info("Setting up content elements to: " + newChild.getProperties().getIdValue("ID") + ", " + newChild.getClassName());
                        attachContentElements(child, newChild);
                    } else {
                        logger.warn("No content element/transfer found on: " + child.getProperties().getIdValue("ID") + " ClassName: " + child.getClassName() + " Bo_Id " + oldBoId);
                    }
                    fnOpUtils.copyData(child, newChild, descriptionFull);
                    newChild.save(RefreshMode.REFRESH);
                    newChild.checkin(AutoClassify.DO_NOT_AUTO_CLASSIFY, CheckinType.MAJOR_VERSION);
                    //Gestione di nipoti
                    Iterator<?> subChildIterator = DataFetcher.executeQuery(config.getObjectStore(),
                            "",
                            "acq_relation",
                            "head_chronicle_id",
                            child.getProperties().getIdValue("ID").toString());
                    if (subChildIterator.hasNext()) {
                        subChildLookup(subChildIterator, newBoId, fnOpUtils, newChild, oldBoId);
                    }
                    fnOpUtils.createRelationship(mainDocument, newChild, descriptionFull);
                }
                default: {
                    logger.warn("Unmanaged relationship: " + descriptionFull);
                    break;
                }
            }
        }
    }

    /**
     * Metodo atto a svolgere attività, qualora ci fossero (vedi: {@link MigratorDocuments#childLookup(String, FNOpUtils, Document, String, Iterator)}),
     * di creazione e copiare dei metadati, file dagli vecchi "nipoti" agli nuovi.
     *
     * @param subChildIterator iteratore che né contiene dei "nipoti" vecchi
     * @param newBoId          nuovo bo_id
     * @param fnOpUtils        oggetto di utility
     * @param newChild         nuovo "padre" al quale poi verrà creata associazione
     * @param oldBoId          vecchio "bo_id".
     */
    private static void subChildLookup(Iterator<?> subChildIterator, String newBoId, FNOpUtils fnOpUtils, Document newChild, String oldBoId) {
        String descriptionFull;
        while (subChildIterator.hasNext()) {
            RepositoryRow subChildRepositoryRow = (RepositoryRow) subChildIterator.next();
            Properties subChildProperties = subChildRepositoryRow.getProperties();
            descriptionFull = subChildProperties.getStringValue("description_full");
            Document subChild = Factory.Document.fetchInstance(config.getObjectStore(), subChildProperties.getIdValue("tail_chronicle_id"), null);
            Document newSubChild = Factory.Document.createInstance(config.getObjectStore(), subChild.getClassName());
            newSubChild.save(RefreshMode.REFRESH);
            newSubChild.getProperties().putValue("bo_id", newBoId);
            //noinspection SwitchStatementWithTooFewBranches
            switch (descriptionFull) {
                case Constants.ALL_CONTRACT_2_DOC: {
                    logger.info("Description of relation is: " + descriptionFull);
                    if (!subChild.get_ContentElements().isEmpty()) {
                        attachContentElements(subChild, newSubChild);
                        fnOpUtils.copyData(newSubChild, subChild, descriptionFull);
                    } else {
                        logger.warn("No content element/transfer found on: " + subChild.getProperties().getIdValue("ID") + " ClassName: " + subChild.getClassName() + " Bo_Id " + oldBoId);
                    }
                    fnOpUtils.createRelationship(newSubChild, newChild, descriptionFull);
                    break;
                }
                default: {
                    logger.warn("Unmanaged relationship: " + descriptionFull);
                    break;
                }
            }
        }
    }

    /**
     * Metodo atto a svolgere attività di allegare i file (content element) dal vecchio al nuovo
     *
     * @param oldDocument vecchio documento da dove prendere i file
     * @param newDocument nuovo documento al quale allegare i file
     */
    private static void attachContentElements(Document oldDocument, Document newDocument) {
        //noinspection deprecation
        ContentElementList contentElementList = Factory.ContentTransfer.createList();
        for (Object docContentElement : oldDocument.get_ContentElements()) {
            ContentTransfer oldContentTransfer = (ContentTransfer) docContentElement;
            InputStream inputStream = oldContentTransfer.accessContentStream();
            ContentTransfer newContentTransfer = Factory.ContentTransfer.createInstance();
            newContentTransfer.setCaptureSource(inputStream);
            newContentTransfer.set_ContentType(oldContentTransfer.get_ContentType());
            if (!oldContentTransfer.get_RetrievalName().isEmpty()) {
                newContentTransfer.set_RetrievalName(oldContentTransfer.get_RetrievalName());
            }
            //noinspection unchecked
            contentElementList.add(newContentTransfer);
        }
        newDocument.set_ContentElements(contentElementList);
    }
}
