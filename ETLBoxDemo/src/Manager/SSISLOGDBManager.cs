using ETLBox.src.Toolbox.Database;
using ETLBoxDemo.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ETLBoxDemo.src.Manager
{
    public class SSISLOGDBManager
    {
        public static string GetLogDbConnection()
        {
            return string.Format(CompanySettings.ConnectionStringFormat_Sql, CompanySettings.ETL_SSIS_Logs_SERVERNAME, CompanySettings.ETL_SSIS_Logs_DATABASENAME, CompanySettings.ETL_SSIS_Logs_DBUSER, CompanySettings.ETL_SSIS_Logs_DBPASSWORD);
        }
        #region Sql Statement
        public static readonly string SQL_LogPackageStart = 
            @"exec dbo.LogPackageStart
            @BatchLogID = '{0}'
            ,@PackageName = '{1}'
            ,@ExecutionInstanceID = '{2}'
            ,@MachineName = '{3}'
            ,@UserName = '{4}'
            ,@StartDatetime = '{5}'
            ,@PackageVersionGUID = N'{6}'
            ,@VersionMajor = '{7}'
            ,@VersionMinor = '{8}'
            ,@VersionBuild = '{9}'
            ,@VersionComment = '{10}'
            ,@PackageGUID = N'{11}'
            ,@CreationDate = '{12}'
            ,@CreatedBy = '{13}'
            ,@CompanyId = '{14}'";

        public static readonly string SQL_LogPackageEnd =
            @"exec dbo.LogPackageEnd
            @PackageLogID = {0}
            ,@BatchLogID = {1}
            ,@EndBatchAudit = {2} ";

        #endregion
        public static Dictionary<string, string> LogPackageStart(string packageName)
        {
            var sql = string.Format(SQL_LogPackageStart, "BatchLogID", packageName, Guid.NewGuid(), Environment.MachineName, "UserName", DateTime.Now.ToLongTimeString(), "PackageVersionGUID", "1", "1", "1", "VersionComment", "PackageGUID", DateTime.Now.ToLongDateString(), "CreatedBy", CompanySettings.CompanyID);
            var result = SQLHelper.GetDbValues(GetLogDbConnection(), "Log Package Start.", sql, null).FirstOrDefault();
            return result;
        }

        public static void LogPackageEnd(string packageLogID, string batchLogID, string endBatchAudit)
        {
            var sql = string.Format(SQL_LogPackageStart, packageLogID, batchLogID, endBatchAudit);
            SQLHelper.InsertOrUpdateDbValue(GetLogDbConnection(), "Log Package End.", sql, null);
        }

    }
}
