package r2u.tools.utils;

import java.util.*;

public class Converters {
    /**
     * Funzione di conversione di una hashmap in array di interi
     *
     * @param netCoServCo hashmap che gli viene passato per la conversione
     * @return uno array di interi contenente le system_id
     */
    public static ArrayList<Integer> getIntegerFromHashMap(HashMap<String, String> netCoServCo) {
        ArrayList<Integer> systemIds = new ArrayList<>();
        for (Map.Entry<String, String> pair : netCoServCo.entrySet()) {
            systemIds.add(Integer.valueOf(pair.getKey()));
        }
        return systemIds;
    }

    /**
     * @param netCo la lista degli system_id appartenenti a NETCO
     * @return hashMap che contiene il system_id come la chiave e l'affisso di quel system_id
     */
    public static HashMap<String, String> netCoConverter(ArrayList<String> netCo) {
        HashMap<String, String> netCoMap = new HashMap<>();
        for (String net : netCo) {
            netCoMap.put(net.split("=")[0], net.split("=")[1]);
        }
        return netCoMap;
    }

    /**
     * @param documentList un oggetto che contiene le classi documentali da processare
     * @return hashMap che contiene, come la chiave la classe documentale, mentre il suo valore e` un boolean
     */
    public static HashMap<String, Boolean> convertArrayList2StringBooleanHashMap(ArrayList<String> documentList) {
        HashMap<String, Boolean> documentMap = new HashMap<>();
        for (String document : documentList) {
            documentMap.put(document.split("=")[0], Boolean.valueOf(document.split("=")[1]));
        }
        return documentMap;
    }

    /**
     * @param objectList una lista di oggetti da convertire
     * @return un'array di stringhe
     */
    public static ArrayList<String> listObject2ArrayListStringConverter(List<Object> objectList) {
        ArrayList<String> arrayList = new ArrayList<>();
        for (Object secProxy : objectList) {
            arrayList.add(secProxy.toString());
        }
        return arrayList;
    }

    public static ArrayList<String> stringToArrayListConverter(String data) {
        String[] split = data.split(",");
        return new ArrayList<>(Arrays.asList(split));
    }
}
