package r2u.tools.utils;

import com.filenet.api.collection.PageIterator;
import com.filenet.api.collection.StringList;
import com.filenet.api.core.ObjectStore;
import com.filenet.api.query.SearchSQL;
import com.filenet.api.query.SearchScope;
import org.apache.log4j.Logger;
import r2u.tools.constants.Constants;

import java.util.Iterator;

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
     * Funzione atto a ricercare su Relazioni SDI (acq_relation) il GUID del figlio, passando il GUID del padre
     *
     * @param headId      variabile di coda su quale si effettua la ricerca presso la relazione sdi
     * @param objectStore contiene i dati dell'object store
     * @return restituisce il GUID del padre quando si processa l'allegato
     */
    public static Iterator<?> fetchTailIdByRelationHeadId(String headId, ObjectStore objectStore) {
        String querySource = "SELECT [tail_chronicle_id] FROM [acq_relation] WHERE [head_chronicle_id] = " + headId;
        return fetchDataByQuery(querySource, objectStore);
    }
}
