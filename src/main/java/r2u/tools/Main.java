package r2u.tools;

import org.apache.log4j.Logger;
import r2u.tools.utils.JSONParser;

import java.nio.file.Files;
import java.nio.file.Paths;

public class Main {
    private final static Logger logger = Logger.getLogger(Main.class.getName());

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            logger.error("No config.json passed to the argument...");
            System.exit(-1);
        }
        if (args.length > 1) {
            logger.error("You have to pass only path to config.json...");
            System.exit(-1);
        }
        if (!Files.exists(Paths.get(args[0]))) {
            logger.error("No config.json has been found");
            System.exit(-1);
        }
        JSONParser jsonParser = new JSONParser();
        jsonParser.parseJson(args[0]);
        System.gc();
    }
}