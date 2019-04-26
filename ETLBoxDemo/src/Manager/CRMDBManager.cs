using ETLBox.src.Toolbox.Database;
using ETLBoxDemo.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace ETLBoxDemo.src.Manager
{
    public static class CRMDBManager
    {
        public static string GetCRMConnectionString()
        {
            return string.Format(CompanySettings.ConnectionStringFormat_Sql, CompanySettings.SP_ServerName, CompanySettings.SP_DatabaseName, CompanySettings.SP_DBUser, CompanySettings.SP_DBPassword);
        }

        public static readonly string SQL_CreateSourceCode = 
            @"DECLARE @propertycode VARCHAR(50); 
              SET @propertycode = (SELECT TOP 1 propertycode FROM dbo.D_PROPERTY WHERE CenAdminCompanyID = {0}) ;
              IF NOT EXISTS ( SELECT  1 FROM    dbo.L_DATA_SOURCE_CODE WHERE   SourceName = '{1}' AND SubSourceName = '{2}' ) 
              BEGIN
                  INSERT  dbo.L_DATA_SOURCE_CODE ( PropertyCode, SourceName, SubSourceName, SourceNameDes, SubSourceNameDes, IsShowdropdown, DedupPriority, ETLProcess )
                    VALUES  (
                        @propertycode, -- PropertyCode - varchar(20)
                        '{1}', -- SourceName - nvarchar(100)
                        '{2}', -- SubSourceName - nvarchar(100)
                        '', -- SourceNameDes - nvarchar(200)
                        '', -- SubSourceNameDes - nvarchar(200)
                        {3}, -- IsShowdropdown - bit
                        {4}, -- DedupPriority - int
                        {5}  -- ETLProcess - int
		            ); 
                 SELECT TOP 1 SourceID FROM    dbo.L_DATA_SOURCE_CODE WHERE   SourceName = '{1}' AND SubSourceName = '{2}';  
             END
             ELSE 
                SELECT  TOP 1 SourceID FROM    dbo.L_DATA_SOURCE_CODE WHERE   SourceName = '{1}' AND SubSourceName = '{2}'; ";

        public static string CreateSourceCode(string companyId, string sourceName, string subSourceName, byte isShowdropdown, int dedupPriority, int ETLProcess)
        {
            string sql = string.Format(SQL_CreateSourceCode, companyId, sourceName, subSourceName, isShowdropdown, dedupPriority, ETLProcess);
            return SQLHelper.GetDbValues(GetCRMConnectionString(), $"sqlCreateSourceCode_{subSourceName.Trim()}", sql, null).FirstOrDefault();
        }

        public static string GetLastCheckTime(string sourceTable)
        {
            string sqlGetLastCheckTime = $@"SELECT ISNULL(MAX(DriverExecutionDate), DATEADD(yy,-10,GETDATE())) AS LastCheckTime FROM ETL_PACKAGE_LOG WHERE Component = '{sourceTable}' AND EndTime IS NOT NULL";
            List<string> sqlGetLastCheckTimeList = SQLHelper.GetDbValues(GetCRMConnectionString(), $"sqlGet{sourceTable}LastCheckTime", sqlGetLastCheckTime, null);
            return sqlGetLastCheckTimeList.FirstOrDefault();
        }
    }
}
