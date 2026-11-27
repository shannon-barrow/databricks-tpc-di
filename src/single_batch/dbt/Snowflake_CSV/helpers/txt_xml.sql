-- STRIP_OUTER_ELEMENT drops the <TPCDI:Actions> root so each <TPCDI:Action>
-- loads as its own row, which keeps every row far below the VARIANT size limit
-- whatever the size of the CustomerMgmt file.
CREATE OR REPLACE FILE FORMAT database.schema.TXT_XML
TYPE = XML
COMPRESSION = 'NONE'
STRIP_OUTER_ELEMENT = TRUE
PRESERVE_SPACE = FALSE
;
