using System;
using System.Collections.Generic;
using System.Text;

namespace ETLBoxDemo.src.Modules.Customer
{
    public class D_Customer
    {
        public string CustomerID { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Email { get; set; }
        public string PropertyCode { get; set; }
        public string InsertDate { get; set; }
        public string SourceID { get; set; }
        public string AddressStatus { get; set; }
        public string DedupeCheck { get; set; }
        public string AllowEMail { get; set; }
        public string Report_Flag { get; set; }
        public string UNIFOCUS_SCORE { get; set; }
    }

    public class eInsight_L_Languages
    {
        public string ID { get; set; }
        public string Language { get; set; }
        public string Language_en { get; set; }
        public string Globalization { get; set; }
    }

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

    public class PMS_ProfilePolicies
    {
        public string PK_ProfilePolicies { get; set; }
        public string FK_Profiles { get; set; }
        public string FK_PolicyTypes { get; set; }
        public string AttributeName { get; set; }
        public string IntegerValue { get; set; }
        public string StringValue { get; set; }
        public string StartDate { get; set; }
        public string ExpirationDate { get; set; }
        public string Comments { get; set; }
    }

    public class PMS_ContactMethodPolicies
    {
        public string PK_ContactMethodPolicies { get; set; }
        public string FK_ContactMethod { get; set; }
        public string FK_PolicyTypes { get; set; }
        public string AttributeName { get; set; }
        public string IntegerValue { get; set; }
        public string StringValue { get; set; }
        public string StartDate { get; set; }
        public string ExpirationDate { get; set; }
        public string Comments { get; set; }
    }

    public class PMS_ContactMethod
    {
        public string PK_ContactMethod { get; set; }
        public string FK_Reservations { get; set; }
        public string FK_Profiles { get; set; }
        public string CMStatusId { get; set; }
        public string CMType { get; set; }
        public string CMData { get; set; }
        public string CMCategory { get; set; }
        public string CMOptOut { get; set; }
        public string CMSourceDate { get; set; }
        public string DateInserted { get; set; }
        public string LastUpdated { get; set; }
        public string Checksum { get; set; }
        public string IsDirty { get; set; }
        public string IsPrimary { get; set; }
        public string Confirmation { get; set; }
        public string CMExtraData { get; set; }
        public string InactiveDate { get; set; }
        public string RecordStatus { get; set; }
        public string SourceName { get; set; }
    }

    public class PMS_ActionComments
    {
        public string PK_ActionComments { get; set; }
        public string FK_Internal { get; set; }
        public string KeyTable { get; set; }
        public string CommentType { get; set; }
        public string ExternalID { get; set; }
        public string ActionType { get; set; }
        public string ActionText { get; set; }
        public string ActionTypeID { get; set; }
        public string ActionDate { get; set; }
        public string InactiveDate { get; set; }
        public string GuestViewable { get; set; }
        public string PMSCreatorCode { get; set; }
        public string DatePMSCommentCreated { get; set; }
        public string DateInserted { get; set; }
        public string LastUpdated { get; set; }
        public string Checksum { get; set; }
        public string IsDirty { get; set; }
        public string CommentTitle { get; set; }
        public string CRMSourceActionType { get; set; }
        public string RecordStatus { get; set; }
        public string CommentClass { get; set; }
        public string ResortID { get; set; }
    }

    public class PMS_ADDRESS
    {
        public string PK_Address { get; set; }
        public string FK_Profiles { get; set; }
        public string AddressTypeCode { get; set; }
        public string SourceAddressType { get; set; }
        public string RecordStatus { get; set; }
        public string AddressStatus { get; set; }
        public string Attn { get; set; }
        public string Address1 { get; set; }
        public string Address2 { get; set; }
        public string City { get; set; }
        public string StateProvince { get; set; }
        public string PostalCode { get; set; }
        public string CountryCode { get; set; }
        public string DateInserted { get; set; }
        public string LastUpdated { get; set; }
        public string Checksum { get; set; }
        public string IsDirty { get; set; }
        public string IsPrimary { get; set; }
        public string AddressCleansed { get; set; }
        public string AddressLanguage { get; set; }
    }

}
