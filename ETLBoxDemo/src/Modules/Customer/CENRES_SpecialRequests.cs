using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace ETLBoxDemo.src.Modules.Customer
{
    public class CENRES_SpecialRequests
    {
        public string PK_SpecialRequests{ get; set; }
        public string FK_Profiles{ get; set; }
        public string RequestType{ get; set; }
        public string RequestCode{ get; set; }
        public string RequestComments { get; set; }
    }
}