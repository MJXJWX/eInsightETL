using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ETLBoxDemo.src.Modules.Customer
{
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
    }
}