using ETLBoxDemo.Common;
using ETLBoxDemo.src.Modules.Customer;
using ETLBoxDemo.src.Utility;
using System;
using System.Collections.Generic;
using System.Text;

namespace ETLBoxDemo.src.Manager
{
    public class DataDictionaryDataFlowTaskManager
    {
        public static void DFT_MoveRateTypeDataForBiltmore()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.L_Data_Dictionary";
            var sql = PMSDBManager.SQL_GetRateTypeForBiltmore;
            var lSql = CRMDBManager.SQL_GetRateTypesForBiltmore;
            var lKeys = new Dictionary<string, string>();
            lKeys.Add("CendynPropertyID", "CendynPropertyID");
            lKeys.Add("FieldValue", "FieldValue");
            lKeys.Add("FieldName", "FieldName");
            lKeys.Add("BuildingCode", "BuildingCode");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("PropertyCode", "PropertyCode");
            var primaryKeys = new List<string>() { "PropertyCode", "FieldValue", "FieldName" };
            var updateFields = new List<string>() { "Description", "Notes" };

            new DataFlowTask<L_Data_Dictionary, L_Data_Dictionary, L_Data_Dictionary>().runTask(sourceCon, destinationCon, destinationCon, tableName, sql, lSql, lKeys, lMapping, null, true, false, primaryKeys, updateFields);
        }

        public static void DFT_UpdateShowAlphaCodeData()
        {
            var sourceCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.L_Data_Dictionary";
            var sql = CRMDBManager.SQL_GetShowAlphaCodeAndShowName;
            var primaryKeys = new List<string>() { "PropertyCode", "FieldValue", "FieldName" };
            var updateFields = new List<string>() { "Description" };

            new DataFlowTask<L_Data_Dictionary>().runTask(sourceCon, sourceCon, tableName, sql, true, true, primaryKeys, updateFields);
        }

        public static void DFT_MoveRateTypeDataForSHGroup()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.L_Data_Dictionary";
            var sql = PMSDBManager.SQL_GetRateTypeDataForSHGroup;
            var lSql = CRMDBManager.SQL_GetRateTypesForSHGroup;
            var lKeys = new Dictionary<string, string>();
            lKeys.Add("CendynPropertyID", "CendynPropertyID");
            lKeys.Add("FieldValue", "FieldValue");
            lKeys.Add("FieldName", "FieldName");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("PropertyCode", "PropertyCode");
            var primaryKeys = new List<string>() { "PropertyCode", "FieldValue", "FieldName" };
            var updateFields = new List<string>() { "Description", "Notes" };

            new DataFlowTask<L_Data_Dictionary, L_Data_Dictionary, L_Data_Dictionary>().runTask(sourceCon, destinationCon, destinationCon, tableName, sql, lSql, lKeys, lMapping, null, true, false, primaryKeys, updateFields);
        }

        public static void DFT_MoveRoomTypeDataForSHGroup()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.L_Data_Dictionary";
            var sql = PMSDBManager.SQL_GetRoomTypeDataForSHGroup;
            var lSql = CRMDBManager.SQL_GetRoomTypesForSHGroup;
            var lKeys = new Dictionary<string, string>();
            lKeys.Add("CendynPropertyID", "CendynPropertyID");
            lKeys.Add("FieldValue", "FieldValue");
            lKeys.Add("FieldName", "FieldName");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("PropertyCode", "PropertyCode");
            var primaryKeys = new List<string>() { "PropertyCode", "FieldValue", "FieldName" };
            var updateFields = new List<string>() { "Description", "Notes" };

            new DataFlowTask<L_Data_Dictionary, L_Data_Dictionary, L_Data_Dictionary>().runTask(sourceCon, destinationCon, destinationCon, tableName, sql, lSql, lKeys, lMapping, null, true, false, primaryKeys, updateFields);
        }

        public static void DFT_MoveRateTypeDataFor12951()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.L_Data_Dictionary";
            var sql = PMSDBManager.SQL_GetRateTypeDataFor12951;
            var lSql = CRMDBManager.SQL_GetRateTypesFor12951;
            var lKeys = new Dictionary<string, string>();
            lKeys.Add("CendynPropertyID", "CendynPropertyID");
            lKeys.Add("FieldValue", "FieldValue");
            lKeys.Add("FieldName", "FieldName");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("PropertyCode", "PropertyCode");
            var primaryKeys = new List<string>() { "PropertyCode", "FieldValue", "FieldName" };
            var updateFields = new List<string>() { "Description", "Notes" };

            new DataFlowTask<L_Data_Dictionary, L_Data_Dictionary, L_Data_Dictionary>().runTask(sourceCon, destinationCon, destinationCon, tableName, sql, lSql, lKeys, lMapping, null, true, false, primaryKeys, updateFields);
        }

        public static void DFT_MoveRateTypeDataForAquaAston()
        {
            var sourceCon = PMSDBManager.GetPMSConnectionString();
            var destinationCon = CRMDBManager.GetCRMConnectionString();
            var tableName = "dbo.L_Data_Dictionary";
            var sql = PMSDBManager.SQL_GetRateTypeDataForAquaAston;
            var lSql = CRMDBManager.SQL_GetRateTypesForAquaAston;
            var lKeys = new Dictionary<string, string>();
            lKeys.Add("CendynPropertyID", "CendynPropertyID");
            lKeys.Add("FieldValue", "FieldValue");
            lKeys.Add("FieldName", "FieldName");
            var lMapping = new Dictionary<string, string>();
            lMapping.Add("PropertyCode", "PropertyCode");
            var primaryKeys = new List<string>() { "PropertyCode", "FieldValue", "FieldName" };
            var updateFields = new List<string>() { "Description", "Notes" };

            new DataFlowTask<L_Data_Dictionary, L_Data_Dictionary, L_Data_Dictionary>().runTask(sourceCon, destinationCon, destinationCon, tableName, sql, lSql, lKeys, lMapping, null, true, false, primaryKeys, updateFields);
        }
    }
}
