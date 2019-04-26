using System;
using System.Collections.Generic;
using System.Text;

namespace ETLBoxDemo.src.Modules.Customer
{
    public class D_Customer
    {
        public string PropertyCode{ get; set; }
        public string PK_Profiles{ get; set; }
        public string SourceGuestID{ get; set; }
        public string FirstName{ get; set; }
        public string MiddleName{ get; set; }
        public string LastName{ get; set; }
        public string Salutation{ get; set; }
        public string ShortTitle{ get; set; }
        public string GenderCode{ get; set; }
        public string Company{ get; set; }
        public string CompanyTitle{ get; set; }
        public string JobTitle{ get; set; }
        public string Languages{ get; set; }
        public string SourceID{ get; set; }
        public string DedupeCheck{ get; set; }
        public string DatePMSProfileUpdated{ get; set; }
        public string AllowEMail{ get; set; }
        public string AllowMail{ get; set; }
        public string AllowSMS{ get; set; }
        public string AllowPhone{ get; set; }
        public string Membership { get; set; }
        public string ExternalProfileID2{ get; set; }
        public string VIPID{ get; set; }
        public string VIPCode{ get; set; }
        public string Nationality{ get; set; }
    }
}
