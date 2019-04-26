using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ETLBoxDemo.src.Modules.Customer
{
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
        public string DateInserted { get; set; }
        public string LastUpdated { get; set; }
    }
}