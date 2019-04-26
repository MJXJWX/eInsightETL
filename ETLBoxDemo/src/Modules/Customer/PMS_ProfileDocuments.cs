using System;
using System.Collections.Generic;
using System.Text;

namespace ETLBoxDemo.src.Modules.Customer
{
    public class PMS_ProfileDocuments
    {
        public string Id { get; set; }
        public string FK_Profile { get; set; }
        public string DocType { get; set; }
        public string DocSource { get; set; }
        public string CodeOnDocument { get; set; }
        public string DocNotes { get; set; }
        public string DocId_PII { get; set; }
        public string NameOnDocument_PII { get; set; }
        public string DocumentBody_PII { get; set; }
        public string NationalityOnDocument { get; set; }
        public string EffectiveDate { get; set; }
        public string ExpirationDate { get; set; }
        public string PII_StoredAs { get; set; }
        public string PII_Algorithm { get; set; }
        public string PII_Key { get; set; }
        public string PII_KeyId { get; set; }
        public string Issuer { get; set; }
        public string IssuerAddress1 { get; set; }
        public string IssuerAddress2 { get; set; }
        public string IssuerCity { get; set; }
        public string IssuerStateProv { get; set; }
        public string IssuerPostalCode { get; set; }
        public string IssuerCountry { get; set; }
        public string IsPrimary { get; set; }
        public string InactiveDate { get; set; }
        public string DateCreated { get; set; }
        public string LastUpdated { get; set; }
    }
}
