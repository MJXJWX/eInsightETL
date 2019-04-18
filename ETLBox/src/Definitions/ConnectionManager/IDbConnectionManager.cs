using System.Collections.Generic;
using System.Data;

namespace ALE.ETLBox.ConnectionManager {
    public interface IDbConnectionManager : IConnectionManager  {      
        int ExecuteNonQuery(string command, IEnumerable<QueryParameter> parameterList = null);
        object ExecuteScalar(string command, IEnumerable<QueryParameter> parameterList = null);
        IDataReader ExecuteReader(string command, IEnumerable<QueryParameter> parameterList = null);
        void BulkInsert(ITableData data, string tableName);
        void BeforeBulkInsert();
        void AfterBulkInsert();
        IDbConnectionManager Clone();
    }
}
