namespace ALE.ETLBox.ConnectionManager {
    public interface ICubeConnectionManager : IConnectionManager {
        void Process();
        void DropIfExists();
        ICubeConnectionManager Clone();
    }
}
