using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ETLBoxDemo.src.Modules.Customer
{
    public class PMS_Address
    {
        public string PK_Address{ get; set; }
        public string FK_Profiles{ get; set; }
        public string AddressTypeCode{ get; set; }
        public string SourceAddressType{ get; set; }
        public string RecordStatus{ get; set; }
        public string AddressStatus{ get; set; }
        public string Attn{ get; set; }
        public string Address1{ get; set; }
        public string Address2{ get; set; }
        public string City{ get; set; }
        public string StateProvince{ get; set; }
        public string PostalCode{ get; set; }
        public string CountryCode{ get; set; }
        public string DateInserted{ get; set; }
        public string LastUpdated{ get; set; }
        public string Checksum{ get; set; }
        public string IsDirty{ get; set; }
        public string IsPrimary{ get; set; }
        public string AddressCleansed{ get; set; }
        public string AddressLanguage { get; set; }
    }
}