package r2u.tools.utils;

import com.filenet.api.collection.ContentElementList;
import com.filenet.api.core.ContentTransfer;
import com.filenet.api.core.Document;
import com.filenet.api.property.Properties;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import r2u.tools.config.Configurator;
import r2u.tools.constants.Constants;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;

@SuppressWarnings("DuplicatedCode")
public class FileSystemUtils {
    private final static Logger logger = Logger.getLogger(FileSystemUtils.class.getName());
    private final static Configurator config = Configurator.getInstance();

    //Creo file fisici sul FS.
    public static void createFile(String folderPath,
                                  String path,
                                  Properties subChildProperties,
                                  Document subChildDocument,
                                  ContentElementList subChildDocumentContentElements,
                                  String fileName,
                                  String hasAttachment,
                                  String subChildListPath,
                                  boolean isSubChild) throws IOException {
        Path fileNamePath = Paths.get(fileName);
        if (!subChildDocumentContentElements.isEmpty()) {
            for (Object docContentElement : subChildDocumentContentElements) {
                ContentTransfer contentTransfer = (ContentTransfer) docContentElement;
                InputStream inputStream = contentTransfer.accessContentStream();
                String line;
                //Qui produco un file diciamo temporaneo di riepilogo del download
                //Formato e`: OLD_NAME, DOCTYPE, DESCRIPTION_FULL, EXPORT_STATUS
                if (Files.notExists(fileNamePath)) {
                    logger.info("RetrievalName: " + contentTransfer.get_RetrievalName());
                    logger.info("Trying to save file {" + fileName + "} under path: " + path);
                    FileUtils.copyInputStreamToFile(inputStream, new File(fileName));
                    if (!hasAttachment.isEmpty() && !subChildListPath.isEmpty()) {
                        line = fileName + config.getCSVSeparator() + contentTransfer.get_RetrievalName() + config.getCSVSeparator() + subChildDocument.getClassName() + config.getCSVSeparator()
                                + subChildProperties.getStringValue("description_full") + config.getCSVSeparator() + contentTransfer.get_ContentType() + config.getCSVSeparator() + "STATUS_OK" + config.getCSVSeparator() + config.getCSVSeparator() + config.getCSVSeparator() + hasAttachment + config.getCSVSeparator() + subChildListPath + "\n";
                    } else {
                        line = fileName + config.getCSVSeparator() + contentTransfer.get_RetrievalName() + config.getCSVSeparator() + subChildDocument.getClassName() + config.getCSVSeparator()
                                + subChildProperties.getStringValue("description_full") + config.getCSVSeparator() + contentTransfer.get_ContentType() + config.getCSVSeparator() + "STATUS_OK" + config.getCSVSeparator() + config.getCSVSeparator() + config.getCSVSeparator() + "\n";
                    }
                    BufferedWriter writeData = new BufferedWriter(new FileWriter("data.csv", true));
                    writeData.write(line);
                    writeData.close();
                } else {
                    if (isSubChild) {
                        logger.warn("THERES FILE ALREADY WITH THE SAME NAME: " + subChildDocument.get_Name());
                        logger.info("RetrievalName: " + contentTransfer.get_RetrievalName());
                        logger.info("Trying to save file {" + fileName + "} under path: " + path);
                        //X gestire i file con lo stesso name
                        Format formatter = new SimpleDateFormat(Constants.printableTimeFormat);
                        FileUtils.copyInputStreamToFile(inputStream, new File(folderPath + formatter.format(new Date()) + "_" + subChildDocument.get_Name()));
                        if (!hasAttachment.isEmpty() && !subChildListPath.isEmpty()) {
                            line = folderPath + formatter.format(new Date()) + "_" + subChildDocument.get_Name() + config.getCSVSeparator() + contentTransfer.get_RetrievalName() + config.getCSVSeparator() + subChildDocument.getClassName() + config.getCSVSeparator() + subChildProperties.getStringValue("description_full") + config.getCSVSeparator() + contentTransfer.get_ContentType() + config.getCSVSeparator() + "STATUS_OK" + config.getCSVSeparator() + config.getCSVSeparator() + config.getCSVSeparator() + hasAttachment + config.getCSVSeparator() + subChildListPath + "\n";
                        } else {
                            line = folderPath + formatter.format(new Date()) + "_" + subChildDocument.get_Name() + config.getCSVSeparator() + contentTransfer.get_RetrievalName() + config.getCSVSeparator() + subChildDocument.getClassName() + config.getCSVSeparator() + subChildProperties.getStringValue("description_full") + config.getCSVSeparator() + contentTransfer.get_ContentType() + config.getCSVSeparator() + "STATUS_OK" + config.getCSVSeparator() + config.getCSVSeparator() + config.getCSVSeparator() + "\n";
                        }
                        BufferedWriter writeData = new BufferedWriter(new FileWriter("data.csv", true));
                        writeData.write(line);
                        writeData.close();
                    }
                }
            }
        } else {
            logger.info("No document found associated with child");
        }
    }
}
