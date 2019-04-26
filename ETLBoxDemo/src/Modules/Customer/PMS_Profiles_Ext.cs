using System;
using System.Collections.Generic;
using System.Text;

namespace ETLBoxDemo.src.Modules.Customer
{
    public class PMS_Profiles_Ext
    {
        public string PK_ProfilesExt{ get; set; } 
        public string FK_Profiles{ get; set; } 
        public string RecordStatus{ get; set; }
        public string PriorityCode{ get; set; }
        public string RoomsPotential{ get; set; }
        public string SalesScope{ get; set; }
        public string ScopeCity{ get; set; }
        public string ActionCode{ get; set; }
        public string BusinessSegment{ get; set; }
        public string AccountType{ get; set; } 
        public string SalesSource{ get; set; }
        public string IndustryCode{ get; set; }
        public string CompetitionCode{ get; set; }
        public string InfluenceCode{ get; set; } 
        public string DateInserted{ get; set; }
        public string LastUpdated{ get; set; }
        public string Checksum{ get; set; }
        public string IsDirty{ get; set; }
        public string Salutation2{ get; set; }
        public string FirstName2{ get; set; }
        public string LastName2{ get; set; }
        public string FamiliarName2{ get; set; }
        public string CompanyName2{ get; set; }
        public string PrimaryLanguage2{ get; set; }
        public string Blacklist{ get; set; }
        public string BlacklistMessage{ get; set; }
        public string AnonymizationStatus { get; set; }
    }
}
