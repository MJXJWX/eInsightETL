using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace ALE.ETLBox {
    
    public interface ITableData : IDisposable, IDataReader {
        bool needIdentityColumn { get; set; }
        IColumnMappingCollection ColumnMapping { get; }
        Dictionary<string,string> ColumnTypes { get; }
    }
}
