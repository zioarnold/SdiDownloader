package r2u.tools.interfaces;

import com.filenet.api.core.Document;

public interface CopyData {
    void copyData(Document oldDocument, Document newDocument, String descriptionFull);
}
