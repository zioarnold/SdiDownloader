package r2u.tools.utils;

import com.filenet.api.core.ObjectStore;
import com.filenet.api.query.SearchSQL;
import com.filenet.api.query.SearchScope;
import org.apache.log4j.Logger;

import java.util.Iterator;

/**
 * Classe atto a definire e implementare le funzioni di ricerca tramite le query su FileNet.
 */
public class DataFetcher {
    private final static Logger logger = Logger.getLogger(DataFetcher.class.getName());

    /**
     * Restituisce i dati a seconda del parametro query
     *
     * @param query       contiene una query che viene recepito da config.json
     * @param objectStore contiene i dati dell'object store
     * @return oggetto di tipo Iterator per poi lavorarli uno x uno.
     */
    public static Iterator<?> fetchDataByQuery(String query, ObjectStore objectStore) {
        logger.info("Fetching data by query: " + query);
        SearchSQL searchSQL = new SearchSQL();
        searchSQL.setQueryString(query);
        return new SearchScope(objectStore).fetchRows(searchSQL, null, null, Boolean.TRUE).iterator();
    }

    /**
     * Funzione atto a ricercare qualcosa su query custom
     *
     * @param column      colonna di ricerca, se non viene specificata allora si usa `*`
     * @param table       tabella di ricerca
     * @param where       criterio di ricerca
     * @param what        valore di ricerca
     * @param objectStore contiene i dati dell'object store
     * @return restituisce il GUID del padre quando si processa l'allegato
     */
    public static Iterator<?> executeQuery(String what, String column, String table, String where, ObjectStore objectStore) {
        String querySource;
        if (!column.isEmpty()) {
            querySource = "SELECT [" + column + "] FROM [" + table + "] WHERE [" + where + "] = " + what;
        } else {
            querySource = "SELECT * FROM [" + table + "] WHERE [" + where + "] = " + what;
        }
        return fetchDataByQuery(querySource, objectStore);
    }
}
