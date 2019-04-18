using System;

namespace ALE.ETLBox.ConnectionManager {
    public interface IConnectionManager : IDisposable {
        IDbConnectionString ConnectionString { get; }
        void Open();
        void Close();       

    }
}
