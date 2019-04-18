namespace ALE.ETLBox.Helper {
    public static class StringExtension {
        public static string NullOrSqlString(this string s) => s == null ? "null" : $"'{s.Replace("'","''")}'";
    }
}
