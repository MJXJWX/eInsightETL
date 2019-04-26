using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ETLBoxDemo.src.Modules.Customer
{
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
        public string DateInserted { get; set; }
        public string LastUpdated { get; set; }
    }
}