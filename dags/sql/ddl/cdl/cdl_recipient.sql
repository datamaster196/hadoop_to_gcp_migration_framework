CREATE TABLE IF NOT EXISTS `{{ var.value.INTEGRATION_PROJECT }}.cdl.cdl_recipient`(
    iaddrerrorcount         int64  Options(description='Number of errors'),
    iaddrquality            int64  Options(description='Quality rating'),
    iblacklist              int64  Options(description='No longer contact (by any channel)'),
    iblacklistemail         int64  Options(description='No longer contact by email'),
    iblacklistfax           int64  Options(description='No longer contact by fax'),
    iblacklistmobile        int64  Options(description='No longer contact by SMS/MMS'),
    iblacklistphone         int64  Options(description='No longer contact by phone'),
    iblacklistpostalmail    int64  Options(description='No longer contact by direct mail'),
    iboolean1               int64  Options(description='Boolean 1'),
    iboolean2               int64  Options(description='Boolean 2'),
    iboolean3               int64  Options(description='Boolean 3'),
    iemailformat            int64  Options(description='Email format'),
    ifolderid               int64  Options(description='Foreign key of the link Folder (field id)'),
    igender                 String  Options(description='Gender'),
    irecipientid            int64  Options(description='Primary key (internal to Merkle)'),
    istatus                 int64  Options(description='Status'),
    mdata                   String  Options(description='XML memo'),
    saccount                String  Options(description='Account #'),
    saddress1               String  Options(description='Address 1 (apartment)'),
    saddress2               String  Options(description='Address 2'),
    saddress3               String  Options(description='Address 3 (Number and street)'),
    saddress4               String  Options(description='Address 4 (locality)'),
    scity                   String  Options(description='City'),
    scompany                String  Options(description='Company'),
    scountrycode            String  Options(description='Country code'),
    semail                  String  Options(description='Email'),
    sfax                    String  Options(description='Fax'),
    sfirstname              String  Options(description='First name'),
    slanguage               String  Options(description='Language'),
    slastname               String  Options(description='Last name'),
    smiddlename             String  Options(description='Middle name'),
    smobilephone            String  Options(description='Mobile'),
    sorigin                 String  Options(description='Origin'),
    sphone                  String  Options(description='Phone'),
    ssalutation             String  Options(description='Title'),
    sstatecode              String  Options(description='title'),
    stext1                  String  Options(description='Text 1'),
    stext2                  String  Options(description='Text 2'),
    stext3                  String  Options(description='Text 3'),
    stext4                  String  Options(description='Text 4'),
    stext5                  String  Options(description='Text 5'),
    szipcode                String  Options(description='Zip/Postcode'),
    tsaddrlastcheck         datetime Options(description='TimeStamp of the most recent address check'),
    tsbirth                 String Options(description='TimeStamp for the Birthdate'),
    tscreated               datetime Options(description='TimeStamp the record was created'),
    tslastmodified          datetime Options(description='TimeStamp the record was updated'),
	contact_adw_key         String  Options(description='Unique key created for each contact info record')

)
options(description="This table is truncated and reloaded daily with contacts who are being inserted into the Adobe Campaign Management CDL database.")
;