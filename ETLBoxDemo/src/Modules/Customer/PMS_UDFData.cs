using System;
using System.Collections.Generic;
using System.Text;

namespace ETLBoxDemo.src.Modules.Customer
{
    public class PMS_UDFData
    {
        public string PK_UDFData{ get; set; }
        public string RecordStatus{ get; set; }
        public string CendynPropertyID{ get; set; }
        public string FK_Internal{ get; set; }
        public string ModuleName{ get; set; }
        public string TableName{ get; set; }
        public string ColumnName{ get; set; }
        public string UDFValue{ get; set; }
        public string DateInserted{ get; set; }
        public string LastUpdated{ get; set; }
        public string Checksum{ get; set; }
        public string IsDirty { get; set; }
    }
}
