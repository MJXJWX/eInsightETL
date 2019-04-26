using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ETLBoxDemo.src.Modules.Customer
{
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
}