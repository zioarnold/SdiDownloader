package r2u.tools.utils;

import com.filenet.api.constants.AutoClassify;
import com.filenet.api.constants.CheckinType;
import com.filenet.api.constants.RefreshMode;
import com.filenet.api.core.CustomObject;
import com.filenet.api.core.Document;
import com.filenet.api.core.Factory;
import org.apache.log4j.Logger;
import r2u.tools.config.Configurator;
import r2u.tools.constants.Constants;
import r2u.tools.interfaces.CopyData;
import r2u.tools.interfaces.Relationships;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

@SuppressWarnings("DuplicatedCode")
public class FNOpUtils implements Relationships, CopyData {
    private final static Logger logger = Logger.getLogger(FNOpUtils.class.getName());
    private final static Configurator config = Configurator.getInstance();

    /**
     * Metodo atto a creare delle relazioni fra il documento principale e quello secondario
     *
     * @param mainDocument    documento principale
     * @param newDocument     documento secondario
     * @param descriptionFull descrizione della relazione
     */
    @Override
    public void createRelationship(Document mainDocument, Document newDocument, String descriptionFull) {
        logger.info("Creating relationship between: " + mainDocument.getClassName() + "/" + mainDocument.getProperties().getIdValue("ID")
                + " and " + newDocument.getClassName() + "/" + newDocument.getProperties().getIdValue("ID"));
        CustomObject acqRelation = Factory.CustomObject.createInstance(config.getObjectStore(), "acq_relation");
        acqRelation.getProperties().putValue("head_chronicle_id", mainDocument.getProperties().getIdValue("ID"));
        acqRelation.getProperties().putValue("tail_chronicle_id", newDocument.getProperties().getIdValue("ID"));
        acqRelation.getProperties().putValue("description_full", descriptionFull);
        acqRelation.save(RefreshMode.REFRESH);
        logger.info("Saving relationship is successful");
    }

    /**
     * Metodo atto a svolgere attività di popolamento dei metadata del documento nuovo copiando da quello vecchio.
     *
     * @param oldDocument     documento da quale copiare i dati
     * @param newDocument     documento a quale copiare i dati
     * @param descriptionFull descrizione per filtrare il tipo del documento in arrivo
     */
    @Override
    public void copyData(Document oldDocument, Document newDocument, String descriptionFull) {
        switch (descriptionFull) {
            case Constants.BO_2_POS: {
                newDocument.getProperties().putValue("DocumentTitle", oldDocument.getProperties().getStringValue("DocumentTitle"));
                newDocument.getProperties().putValue("bo_bu", oldDocument.getProperties().getInteger32Value("bo_bu"));
                newDocument.getProperties().putValue("bo_ente", newDocument.getProperties().getInteger32Value("bo_ente"));
                newDocument.getProperties().putValue("bo_gm", newDocument.getProperties().getInteger32Value("bo_gm"));
                newDocument.getProperties().putValue("bo_pos", newDocument.getProperties().getStringValue("bo_pos"));
                newDocument.getProperties().putValue("data_emissione", newDocument.getProperties().getDateTimeValue("data_emissione"));
                newDocument.getProperties().putValue("tipo_negoz", newDocument.getProperties().getInteger32Value("tipo_negoz"));
                newDocument.getProperties().putValue("bo_gm_chronid_ref", newDocument.getProperties().getStringValue("bo_gm_chronid_ref"));
                newDocument.getProperties().putValue("creation_date", newDocument.getProperties().getDateTimeValue("creation_date"));
                newDocument.getProperties().putValue("data_fin_val", newDocument.getProperties().getDateTimeValue("data_fin_val"));
                newDocument.getProperties().putValue("data_in_val", newDocument.getProperties().getDateTimeValue("data_in_val"));
                newDocument.getProperties().putValue("divisione", newDocument.getProperties().getStringValue("divisione"));
                newDocument.getProperties().putValue("fk1", newDocument.getProperties().getInteger32Value("fk1"));
                newDocument.getProperties().putValue("fk2", newDocument.getProperties().getInteger32Value("fk2"));
                newDocument.getProperties().putValue("flag_allegati", newDocument.getProperties().getInteger32Value("flag_allegati"));
                newDocument.getProperties().putValue("flag_cancellato", newDocument.getProperties().getInteger32Value("flag_cancellato"));
                newDocument.getProperties().putValue("flag_migrato", newDocument.getProperties().getStringValue("flag_migrato"));
                newDocument.getProperties().putValue("gracq", newDocument.getProperties().getStringValue("gracq"));
                newDocument.getProperties().putValue("system_id", newDocument.getProperties().getInteger32Value("system_id"));
                newDocument.getProperties().putValue("mandante", newDocument.getProperties().getStringValue("mandante"));
                newDocument.getProperties().putValue("migrato_da", newDocument.getProperties().getStringValue("migrato_da"));
                newDocument.getProperties().putValue("old_bo_bu", newDocument.getProperties().getInteger32Value("old_bo_bu"));
                newDocument.getProperties().putValue("padre", newDocument.getProperties().getInteger32Value("padre"));
                newDocument.getProperties().putValue("sid", newDocument.getProperties().getStringValue("sid"));
                newDocument.getProperties().putValue("sigla", newDocument.getProperties().getStringValue("sigla"));
                newDocument.getProperties().putValue("bo_pu_chronid_ref", newDocument.getProperties().getStringValue("bo_pu_chronid_ref"));
                newDocument.getProperties().putValue("ente_emittente_chronid_ref", newDocument.getProperties().getStringValue("ente_emittente_chronid_ref"));
                newDocument.getProperties().putValue("has_correlazioni", newDocument.getProperties().getBooleanValue("has_correlazioni"));
                newDocument.getProperties().putValue("has_riferimenti", newDocument.getProperties().getBooleanValue("has_riferimenti"));
                newDocument.getProperties().putValue("buyer", newDocument.getProperties().getStringValue("buyer"));
                newDocument.getProperties().putValue("codice_negoziazione", newDocument.getProperties().getStringValue("codice_negoziazione"));
                newDocument.getProperties().putValue("fornitore", newDocument.getProperties().getInteger32Value("fornitore"));
                newDocument.getProperties().putValue("importo", newDocument.getProperties().getStringValue("importo"));
                newDocument.getProperties().putValue("bo_bu_chronid_ref", newDocument.getProperties().getStringValue("bo_bu_chronid_ref"));
                newDocument.getProperties().putValue("cancellato_str", newDocument.getProperties().getStringValue("cancellato_str"));
                newDocument.getProperties().putValue("origine", newDocument.getProperties().getInteger32Value("origine"));
                newDocument.getProperties().putValue("fornitori_chronid_ref", newDocument.getProperties().getStringValue("fornitori_chronid_ref"));
                //Salvo il file
                newDocument.save(RefreshMode.REFRESH);
                break;
            }
            case Constants.CONTRATTO_2_ALL: {
                newDocument.getProperties().putValue("DocumentTitle", oldDocument.getProperties().getStringValue("DocumentTitle"));
                newDocument.getProperties().putValue("bu_societa", oldDocument.getProperties().getStringValue("bu_societa"));
                newDocument.getProperties().putValue("codice", oldDocument.getProperties().getStringValue("codice"));
                newDocument.getProperties().putValue("codice_oda", oldDocument.getProperties().getStringValue("codice_oda"));
                newDocument.getProperties().putValue("codice_rda", oldDocument.getProperties().getStringValue("codice_rda"));
                newDocument.getProperties().putValue("ente_emittente", oldDocument.getProperties().getInteger32Value("ente_emittente"));
                newDocument.getProperties().putValue("fornitori_not_anag", oldDocument.getProperties().getStringValue("fornitori_not_anag"));
                newDocument.getProperties().putValue("tipo_documento", oldDocument.getProperties().getInteger32Value("tipo_documento"));
                newDocument.getProperties().putValue("description_full", oldDocument.getProperties().getStringValue("description_full"));
                newDocument.getProperties().putValue("tipo_autorizzazione", oldDocument.getProperties().getStringValue("tipo_autorizzazione"));
                newDocument.getProperties().putValue("stato_conservazione", oldDocument.getProperties().getInteger32Value("stato_conservazione"));
                newDocument.getProperties().putValue("stato", oldDocument.getProperties().getInteger32Value("stato"));
                newDocument.getProperties().putValue("codice_contratto", oldDocument.getProperties().getStringValue("codice_contratto"));
                newDocument.getProperties().putValue("data_allegato", oldDocument.getProperties().getDateTimeValue("data_allegato"));
                newDocument.getProperties().putValue("data_decorrenza", oldDocument.getProperties().getDateTimeValue("data_decorrenza"));
                newDocument.getProperties().putValue("fornitori", oldDocument.getProperties().getInteger32Value("fornitori"));
                newDocument.getProperties().putValue("ragione_sociale", oldDocument.getProperties().getStringValue("ragione_sociale"));
                newDocument.getProperties().putValue("sede", oldDocument.getProperties().getStringValue("sede"));
                newDocument.getProperties().putValue("riservatezza", oldDocument.getProperties().getStringValue("riservatezza"));
                newDocument.getProperties().putValue("pubblicabile", oldDocument.getProperties().getStringValue("pubblicabile"));
                newDocument.getProperties().putValue("assegnatario", oldDocument.getProperties().getStringValue("assegnatario"));
                newDocument.getProperties().putValue("data_documento", oldDocument.getProperties().getDateTimeValue("data_documento"));
                newDocument.getProperties().putValue("dimensione", oldDocument.getProperties().getInteger32Value("dimensione"));
                newDocument.getProperties().putValue("bo_gm", oldDocument.getProperties().getInteger32Value("bo_gm"));
                newDocument.getProperties().putValue("strutture_organizzative", oldDocument.getProperties().getInteger32Value("strutture_organizzative"));
                newDocument.getProperties().putValue("note", oldDocument.getProperties().getStringValue("note"));
                newDocument.getProperties().putValue("tipologie_trattative", oldDocument.getProperties().getInteger32Value("tipologie_trattative"));
                newDocument.getProperties().putValue("tipologie", oldDocument.getProperties().getInteger32Value("tipologie"));
                newDocument.getProperties().putValue("valore", oldDocument.getProperties().getInteger32Value("valore"));
                newDocument.getProperties().putValue("data_scadenza_contratto", oldDocument.getProperties().getDateTimeValue("data_scadenza_contratto"));
                newDocument.getProperties().putValue("data_invio", oldDocument.getProperties().getDateTimeValue("data_invio"));
                newDocument.getProperties().putValue("data_partenza", oldDocument.getProperties().getDateTimeValue("data_partenza"));
                newDocument.getProperties().putValue("firmatario", oldDocument.getProperties().getStringValue("firmatario"));
                newDocument.getProperties().putValue("numero_protocollo", oldDocument.getProperties().getStringValue("numero_protocollo"));
                newDocument.getProperties().putValue("riferimento_nomina", oldDocument.getProperties().getStringValue("riferimento_nomina"));
                newDocument.getProperties().putValue("societa_beneficiarie", oldDocument.getProperties().getStringValue("societa_beneficiarie"));
                newDocument.getProperties().putValue("pu", oldDocument.getProperties().getStringValue("pu"));
                newDocument.getProperties().putValue("struttura_autorizzata", oldDocument.getProperties().getStringValue("struttura_autorizzata"));
                newDocument.getProperties().putValue("trattamenti", oldDocument.getProperties().getStringValue("trattamenti"));
                newDocument.getProperties().putValue("link_doc_esterno", oldDocument.getProperties().getStringValue("link_doc_esterno"));
                newDocument.getProperties().putValue("archive_type", oldDocument.getProperties().getStringValue("archive_type"));
                newDocument.getProperties().putValue("contesto", oldDocument.getProperties().getInteger32Value("contesto"));
                newDocument.getProperties().putValue("numero_documento", oldDocument.getProperties().getStringValue("numero_documento"));
                newDocument.getProperties().putValue("system_id", oldDocument.getProperties().getInteger32Value("system_id"));
                newDocument.getProperties().putValue("hash_allegato", oldDocument.getProperties().getStringValue("hash_allegato"));
                newDocument.getProperties().putValue("is_pec", oldDocument.getProperties().getBooleanValue("is_pec"));
                newDocument.getProperties().putValue("bo_pu_chronid_ref", oldDocument.getProperties().getStringValue("bo_pu_chronid_ref"));
                newDocument.getProperties().putValue("autore", oldDocument.getProperties().getInteger32Value("autore"));
                newDocument.getProperties().putValue("tipo_negoz", oldDocument.getProperties().getInteger32Value("tipo_negoz"));
                newDocument.getProperties().putValue("bu_all_chronid_ref", oldDocument.getProperties().getStringListValue("bu_all_chronid_ref"));
                newDocument.getProperties().putValue("gm_all_chronid_ref", oldDocument.getProperties().getStringListValue("gm_all_chronid_ref"));
                newDocument.getProperties().putValue("strut_aut_all_chronid_ref", oldDocument.getProperties().getStringListValue("strut_aut_all_chronid_ref"));
                newDocument.getProperties().putValue("fornitori_all_chronid_ref", oldDocument.getProperties().getStringListValue("fornitori_all_chronid_ref"));
                newDocument.getProperties().putValue("ente_all_chronid_ref", oldDocument.getProperties().getStringListValue("ente_all_chronid_ref"));
//                newDocument.getProperties().putValue("bo_id", oldDocument.getProperties().getStringValue("bo_id"));
                newDocument.getProperties().putValue("has_riferimenti", oldDocument.getProperties().getBooleanValue("has_riferimenti"));
                newDocument.getProperties().putValue("has_allegati", oldDocument.getProperties().getBooleanValue("has_allegati"));
                newDocument.getProperties().putValue("codice", oldDocument.getProperties().getStringValue("codice"));
                newDocument.getProperties().putValue("codice_oda", oldDocument.getProperties().getStringValue("codice_oda"));
                newDocument.getProperties().putValue("codice_rda", oldDocument.getProperties().getStringValue("codice_rda"));
                newDocument.getProperties().putValue("ente_emittente", oldDocument.getProperties().getInteger32Value("ente_emittente"));
                newDocument.getProperties().putValue("fornitori_not_anag", oldDocument.getProperties().getStringValue("fornitori_not_anag"));
                newDocument.getProperties().putValue("tipo_documento", oldDocument.getProperties().getInteger32Value("tipo_documento"));
                newDocument.getProperties().putValue("description_full", oldDocument.getProperties().getStringValue("description_full"));
                newDocument.getProperties().putValue("tipo_autorizzazione", oldDocument.getProperties().getStringValue("tipo_autorizzazione"));
                newDocument.getProperties().putValue("stato_conservazione", oldDocument.getProperties().getInteger32Value("stato_conservazione"));
                newDocument.getProperties().putValue("stato", oldDocument.getProperties().getInteger32Value("stato"));
                newDocument.getProperties().putValue("codice_contratto", oldDocument.getProperties().getStringValue("codice_contratto"));
                newDocument.getProperties().putValue("data_allegato", oldDocument.getProperties().getDateTimeValue("data_allegato"));
                newDocument.getProperties().putValue("data_decorrenza", oldDocument.getProperties().getDateTimeValue("data_decorrenza"));
                newDocument.getProperties().putValue("fornitori", oldDocument.getProperties().getInteger32Value("fornitori"));
                newDocument.getProperties().putValue("ragione_sociale", oldDocument.getProperties().getStringValue("ragione_sociale"));
                newDocument.getProperties().putValue("sede", oldDocument.getProperties().getStringValue("sede"));
                newDocument.getProperties().putValue("riservatezza", oldDocument.getProperties().getStringValue("riservatezza"));
                newDocument.getProperties().putValue("pubblicabile", oldDocument.getProperties().getStringValue("pubblicabile"));
                newDocument.getProperties().putValue("assegnatario", oldDocument.getProperties().getStringValue("assegnatario"));
                newDocument.getProperties().putValue("data_documento", oldDocument.getProperties().getDateTimeValue("data_documento"));
                newDocument.getProperties().putValue("dimensione", oldDocument.getProperties().getInteger32Value("dimensione"));
                newDocument.getProperties().putValue("bo_gm", oldDocument.getProperties().getInteger32Value("bo_gm"));
                newDocument.getProperties().putValue("strutture_organizzative", oldDocument.getProperties().getInteger32Value("strutture_organizzative"));
                newDocument.getProperties().putValue("note", oldDocument.getProperties().getStringValue("note"));
                newDocument.getProperties().putValue("tipologie_trattative", oldDocument.getProperties().getInteger32Value("tipologie_trattative"));
                newDocument.getProperties().putValue("tipologie", oldDocument.getProperties().getInteger32Value("tipologie"));
                newDocument.getProperties().putValue("valore", oldDocument.getProperties().getInteger32Value("valore"));
                newDocument.getProperties().putValue("data_scadenza_contratto", oldDocument.getProperties().getDateTimeValue("data_scadenza_contratto"));
                newDocument.getProperties().putValue("data_invio", oldDocument.getProperties().getDateTimeValue("data_invio"));
                newDocument.getProperties().putValue("data_partenza", oldDocument.getProperties().getDateTimeValue("data_partenza"));
                newDocument.getProperties().putValue("firmatario", oldDocument.getProperties().getStringValue("firmatario"));
                newDocument.getProperties().putValue("numero_protocollo", oldDocument.getProperties().getStringValue("numero_protocollo"));
                newDocument.getProperties().putValue("riferimento_nomina", oldDocument.getProperties().getStringValue("riferimento_nomina"));
                newDocument.getProperties().putValue("societa_beneficiarie", oldDocument.getProperties().getStringValue("societa_beneficiarie"));
                newDocument.getProperties().putValue("pu", oldDocument.getProperties().getStringValue("pu"));
                newDocument.getProperties().putValue("struttura_autorizzata", oldDocument.getProperties().getStringValue("struttura_autorizzata"));
                newDocument.getProperties().putValue("trattamenti", oldDocument.getProperties().getStringValue("trattamenti"));
                newDocument.getProperties().putValue("link_doc_esterno", oldDocument.getProperties().getStringValue("link_doc_esterno"));
                newDocument.getProperties().putValue("archive_type", oldDocument.getProperties().getStringValue("archive_type"));
                newDocument.getProperties().putValue("contesto", oldDocument.getProperties().getInteger32Value("contesto"));
                newDocument.getProperties().putValue("numero_documento", oldDocument.getProperties().getStringValue("numero_documento"));
                newDocument.getProperties().putValue("system_id", oldDocument.getProperties().getInteger32Value("system_id"));
                newDocument.getProperties().putValue("hash_allegato", oldDocument.getProperties().getStringValue("hash_allegato"));
                newDocument.getProperties().putValue("is_pec", oldDocument.getProperties().getBooleanValue("is_pec"));
                newDocument.getProperties().putValue("bo_pu_chronid_ref", oldDocument.getProperties().getStringValue("bo_pu_chronid_ref"));
                newDocument.getProperties().putValue("autore", oldDocument.getProperties().getInteger32Value("autore"));
                newDocument.getProperties().putValue("tipo_negoz", oldDocument.getProperties().getInteger32Value("tipo_negoz"));
                newDocument.getProperties().putValue("bu_all_chronid_ref", oldDocument.getProperties().getStringListValue("bu_all_chronid_ref"));
                newDocument.getProperties().putValue("gm_all_chronid_ref", oldDocument.getProperties().getStringListValue("gm_all_chronid_ref"));
                newDocument.getProperties().putValue("strut_aut_all_chronid_ref", oldDocument.getProperties().getStringListValue("strut_aut_all_chronid_ref"));
                newDocument.getProperties().putValue("fornitori_all_chronid_ref", oldDocument.getProperties().getStringListValue("fornitori_all_chronid_ref"));
                newDocument.getProperties().putValue("ente_all_chronid_ref", oldDocument.getProperties().getStringListValue("ente_all_chronid_ref"));
//                newDocument.getProperties().putValue("bo_id"+config.getCSVSeparator() + oldDocument.getProperties().getStringValue("bo_id"));
                newDocument.getProperties().putValue("has_riferimenti", oldDocument.getProperties().getBooleanValue("has_riferimenti"));
                newDocument.getProperties().putValue("autore_str", oldDocument.getProperties().getStringValue("autore_str"));
                newDocument.getProperties().putValue("esito_conferma_fornitore", oldDocument.getProperties().getStringValue("esito_conferma_fornitore"));
                newDocument.getProperties().putValue("buyer", oldDocument.getProperties().getStringValue("buyer"));
                newDocument.getProperties().putValue("codice_negoziazione", oldDocument.getProperties().getStringValue("codice_negoziazione"));
                newDocument.getProperties().putValue("cognome_cancellazione_richiesta", oldDocument.getProperties().getStringValue("cognome_cancellazione_richiesta"));
                newDocument.getProperties().putValue("data_cancellazione_richiesta", oldDocument.getProperties().getDateTimeValue("data_cancellazione_richiesta"));
                newDocument.getProperties().putValue("data_conferma_fornitore", oldDocument.getProperties().getDateTimeValue("data_conferma_fornitore"));
                newDocument.getProperties().putValue("data_conservazione", oldDocument.getProperties().getDateTimeValue("data_conservazione"));
                newDocument.getProperties().putValue("data_emissione", oldDocument.getProperties().getDateTimeValue("data_emissione"));
                newDocument.getProperties().putValue("data_scadenza", oldDocument.getProperties().getDateTimeValue("data_scadenza"));
                newDocument.getProperties().putValue("divisione", oldDocument.getProperties().getStringValue("divisione"));
                newDocument.getProperties().putValue("enti_vis_testo_di_contratto", oldDocument.getProperties().getStringValue("enti_vis_testo_di_contratto"));
                newDocument.getProperties().putValue("importo", oldDocument.getProperties().getStringValue("importo"));
                newDocument.getProperties().putValue("nome_cancellazione_richiesta", oldDocument.getProperties().getStringValue("nome_cancellazione_richiesta"));
                newDocument.getProperties().putValue("pu_assegnataria", oldDocument.getProperties().getStringValue("pu_assegnataria"));
                newDocument.getProperties().putValue("proivenienza", oldDocument.getProperties().getStringValue("proivenienza"));
                newDocument.getProperties().putValue("responsabile_conservazione", oldDocument.getProperties().getStringValue("responsabile_conservazione"));
                newDocument.getProperties().putValue("guid_riferimento_documento_testo_srm", oldDocument.getProperties().getStringValue("guid_riferimento_documento_testo_srm"));
                newDocument.getProperties().putValue("guid_riferimento_documento_srm", oldDocument.getProperties().getStringValue("guid_riferimento_documento_srm"));
                newDocument.getProperties().putValue("codice_tipologia_ritrascrizione", oldDocument.getProperties().getInteger32Value("codice_tipologia_ritrascrizione"));
                newDocument.getProperties().putValue("stato_richiesta_conferma", oldDocument.getProperties().getStringValue("stato_richiesta_conferma"));
                newDocument.getProperties().putValue("volume_conservazione", oldDocument.getProperties().getStringValue("volume_conservazione"));
                newDocument.getProperties().putValue("sid", oldDocument.getProperties().getStringValue("sid"));
                newDocument.getProperties().putValue("guid_srm", oldDocument.getProperties().getStringValue("guid_srm"));
                newDocument.getProperties().putValue("data_variante", oldDocument.getProperties().getDateTimeValue("data_variante"));
                newDocument.getProperties().putValue("variante", oldDocument.getProperties().getBooleanValue("variante"));
                newDocument.getProperties().putValue("id_file_ra", oldDocument.getProperties().getStringValue("id_file_ra"));
                newDocument.getProperties().putValue("gruppo_acquisti", oldDocument.getProperties().getStringValue("gruppo_acquisti"));
                newDocument.getProperties().putValue("tipo_dati_personali", oldDocument.getProperties().getStringValue("tipo_dati_personali"));
                //Check-in file in stato Released
                newDocument.checkin(AutoClassify.DO_NOT_AUTO_CLASSIFY, CheckinType.MAJOR_VERSION);
                //Salvo le modifiche
                newDocument.save(RefreshMode.REFRESH);
                break;
            }
            case Constants.ALL_CONTRACT_2_DOC: {
                newDocument.getProperties().putValue("DocumentTitle", oldDocument.getProperties().getStringValue("DocumentTitle"));
                newDocument.getProperties().putValue("bu_rda", oldDocument.getProperties().getInteger32Value("bu_rda"));
                newDocument.getProperties().putValue("bu_societa", oldDocument.getProperties().getStringValue("bu_societa"));
                newDocument.getProperties().putValue("codice", oldDocument.getProperties().getStringValue("codice"));
                newDocument.getProperties().putValue("codice_oda", oldDocument.getProperties().getStringValue("codice_oda"));
                newDocument.getProperties().putValue("codice_rda", oldDocument.getProperties().getStringValue("codice_rda"));
                newDocument.getProperties().putValue("ente_emittente", oldDocument.getProperties().getInteger32Value("ente_emittente"));
                newDocument.getProperties().putValue("fornitori_not_anag", oldDocument.getProperties().getStringValue("fornitori_not_anag"));
                newDocument.getProperties().putValue("tipo_documento", oldDocument.getProperties().getInteger32Value("tipo_documento"));
                newDocument.getProperties().putValue("description_full", oldDocument.getProperties().getStringValue("description_full"));
                newDocument.getProperties().putValue("tipo_autorizzazione", oldDocument.getProperties().getStringValue("tipo_autorizzazione"));
                newDocument.getProperties().putValue("stato_conservazione", oldDocument.getProperties().getInteger32Value("stato_conservazione"));
                newDocument.getProperties().putValue("stato", oldDocument.getProperties().getInteger32Value("stato"));
                newDocument.getProperties().putValue("codice_contratto", oldDocument.getProperties().getStringValue("codice_contratto"));
                newDocument.getProperties().putValue("data_allegato", oldDocument.getProperties().getDateTimeValue("data_allegato"));
                newDocument.getProperties().putValue("data_decorrenza", oldDocument.getProperties().getDateTimeValue("data_decorrenza"));
                newDocument.getProperties().putValue("fornitori", oldDocument.getProperties().getInteger32Value("fornitori"));
                newDocument.getProperties().putValue("ragione_sociale", oldDocument.getProperties().getStringValue("ragione_sociale"));
                newDocument.getProperties().putValue("sede", oldDocument.getProperties().getStringValue("sede"));
                newDocument.getProperties().putValue("riservatezza", oldDocument.getProperties().getStringValue("riservatezza"));
                newDocument.getProperties().putValue("pubblicabile", oldDocument.getProperties().getStringValue("pubblicabile"));
                newDocument.getProperties().putValue("assegnatario", oldDocument.getProperties().getStringValue("assegnatario"));
                newDocument.getProperties().putValue("data_documento", oldDocument.getProperties().getDateTimeValue("data_documento"));
                newDocument.getProperties().putValue("dimensione", oldDocument.getProperties().getInteger32Value("dimensione"));
                newDocument.getProperties().putValue("bo_gm", oldDocument.getProperties().getInteger32Value("bo_gm"));
                newDocument.getProperties().putValue("strutture_organizzative", oldDocument.getProperties().getInteger32Value("strutture_organizzative"));
                newDocument.getProperties().putValue("note", oldDocument.getProperties().getStringValue("note"));
                newDocument.getProperties().putValue("tipologie_trattative", oldDocument.getProperties().getInteger32Value("tipologie_trattative"));
                newDocument.getProperties().putValue("tipologie", oldDocument.getProperties().getInteger32Value("tipologie"));
                newDocument.getProperties().putValue("valore", oldDocument.getProperties().getInteger32Value("valore"));
                newDocument.getProperties().putValue("data_scadenza_contratto", oldDocument.getProperties().getDateTimeValue("data_scadenza_contratto"));
                newDocument.getProperties().putValue("data_invio", oldDocument.getProperties().getDateTimeValue("data_invio"));
                newDocument.getProperties().putValue("data_partenza", oldDocument.getProperties().getDateTimeValue("data_partenza"));
                newDocument.getProperties().putValue("firmatario", oldDocument.getProperties().getStringValue("firmatario"));
                newDocument.getProperties().putValue("numero_protocollo", oldDocument.getProperties().getStringValue("numero_protocollo"));
                newDocument.getProperties().putValue("riferimento_nomina", oldDocument.getProperties().getStringValue("riferimento_nomina"));
                newDocument.getProperties().putValue("societa_beneficiarie", oldDocument.getProperties().getStringValue("societa_beneficiarie"));
                newDocument.getProperties().putValue("pu", oldDocument.getProperties().getStringValue("pu"));
                newDocument.getProperties().putValue("struttura_autorizzata", oldDocument.getProperties().getStringValue("struttura_autorizzata"));
                newDocument.getProperties().putValue("trattamenti", oldDocument.getProperties().getStringValue("trattamenti"));
                newDocument.getProperties().putValue("link_doc_esterno", oldDocument.getProperties().getStringValue("link_doc_esterno"));
                newDocument.getProperties().putValue("archive_type", oldDocument.getProperties().getStringValue("archive_type"));
                newDocument.getProperties().putValue("contesto", oldDocument.getProperties().getInteger32Value("contesto"));
                newDocument.getProperties().putValue("numero_documento", oldDocument.getProperties().getStringValue("numero_documento"));
                newDocument.getProperties().putValue("system_id", oldDocument.getProperties().getInteger32Value("system_id"));
                newDocument.getProperties().putValue("hash_allegato", oldDocument.getProperties().getStringValue("hash_allegato"));
                newDocument.getProperties().putValue("is_pec", oldDocument.getProperties().getBooleanValue("is_pec"));
                newDocument.getProperties().putValue("bo_pu_chronid_ref", oldDocument.getProperties().getStringValue("bo_pu_chronid_ref"));
                newDocument.getProperties().putValue("autore", oldDocument.getProperties().getInteger32Value("autore"));
                newDocument.getProperties().putValue("tipo_negoz", oldDocument.getProperties().getInteger32Value("tipo_negoz"));
                newDocument.getProperties().putValue("bu_all_chronid_ref", oldDocument.getProperties().getStringListValue("bu_all_chronid_ref"));
                newDocument.getProperties().putValue("gm_all_chronid_ref", oldDocument.getProperties().getStringListValue("gm_all_chronid_ref"));
                newDocument.getProperties().putValue("strut_aut_all_chronid_ref", oldDocument.getProperties().getStringListValue("strut_aut_all_chronid_ref"));
                newDocument.getProperties().putValue("fornitori_all_chronid_ref", oldDocument.getProperties().getStringListValue("fornitori_all_chronid_ref"));
                newDocument.getProperties().putValue("ente_all_chronid_ref", oldDocument.getProperties().getStringListValue("ente_all_chronid_ref"));
//newDocument.getProperties().putValue(                                                    "bo_id"+config.getCSVSeparator() + oldDocument.getProperties().getStringValue("bo_id"));
                newDocument.getProperties().putValue("has_riferimenti", oldDocument.getProperties().getBooleanValue("has_riferimenti"));
                newDocument.getProperties().putValue("autore_str", oldDocument.getProperties().getStringValue("autore_str"));
                newDocument.getProperties().putValue("ente_emittente_chronid_ref", oldDocument.getProperties().getStringValue("ente_emittente_chronid_ref"));
                newDocument.getProperties().putValue("fornitori_chronid_ref", oldDocument.getProperties().getStringValue("fornitori_chronid_ref"));
                newDocument.getProperties().putValue("bo_bu_chronid_ref", oldDocument.getProperties().getStringValue("bo_bu_chronid_ref"));
                newDocument.getProperties().putValue("data_conservazione", oldDocument.getProperties().getDateTimeValue("data_conservazione"));
                newDocument.getProperties().putValue("data_emissione", oldDocument.getProperties().getDateTimeValue("data_emissione"));
                newDocument.getProperties().putValue("responsabile_conservazione", oldDocument.getProperties().getStringValue("responsabile_conservazione"));
                newDocument.getProperties().putValue("volume_conservazione", oldDocument.getProperties().getStringValue("volume_conservazione"));
                newDocument.save(RefreshMode.REFRESH);
                newDocument.checkin(AutoClassify.DO_NOT_AUTO_CLASSIFY, CheckinType.MAJOR_VERSION);
                break;
            }
            default:
                break;
        }
    }

    /**
     * Metodo atto a creare un file d'appoggio che ne contiene informazioni di meta dati posizioni di contratto
     *
     * @param childDocument classe documentale
     * @param fileMetaData  nome del file da creare
     */
    public static void createACQPosContrattoMetadata(Document childDocument, String fileMetaData) {
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
    public static void createACQAllDocContrattoMetadata(Document childDocument, String fileMetaData) {
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
    public static void createACQAllContrattoMetadata(Document childDocument, String fileMetaData) {
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
                    "has_allegati" + config.getCSVSeparator() + childDocument.getProperties().getBooleanValue("has_allegati") + "\n" +
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
}
