package r2u.tools.interfaces;

import com.filenet.api.core.Document;

public interface Relationships {
   void createRelationship(Document mainDocument, Document newDocument, String descriptionFull);
}
