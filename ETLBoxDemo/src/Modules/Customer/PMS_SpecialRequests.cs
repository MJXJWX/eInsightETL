using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ETLBoxDemo.src.Modules.Customer
{
    public class PMS_SpecialRequests
    {
        public string PK_SpecialRequests{ get; set; }
        public string FK_Reservations{ get; set; }
        public string FK_Profiles{ get; set; }
        public string ExternalRPH{ get; set; }
        public string RequestType{ get; set; }
        public string RequestCode{ get; set; }
        public string RequestComments{ get; set; }
        public string ResortField{ get; set; }
        public string ActionTypeCode{ get; set; }
        public string SourceActionType{ get; set; }
        public string CRMSourceActionType{ get; set; }
        public string InactiveDate{ get; set; }
        public string DateInserted{ get; set; }
        public string LastUpdated{ get; set; }
        public string Checksum{ get; set; }
        public string IsDirty{ get; set; }
        public string Quantity { get; set; }
    }
}