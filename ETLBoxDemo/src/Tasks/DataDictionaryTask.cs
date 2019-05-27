using ETLBoxDemo.Common;
using ETLBoxDemo.src.Manager;
using System;
using System.Collections.Generic;
using System.Text;

namespace ETLBoxDemo.src.Tasks
{
    public class DataDictionaryTask
    {
        public void Start()
        {
            var logResult = SSISLOGDBManager.LogPackageStart("eConciergeETL_Driver");
            var BatchLogID = logResult["BatchLogID"];
            var PackageLogID = logResult["PackageLogID"];
            var EndBatchAudit = logResult["EndBatchAudit"];

            CRMDBManager.UpdateRateTypeDictionary();

            CRMDBManager.UpdateRoomTypeDictionary();

            CRMDBManager.UpdateChannelCodeDictionary();

            CRMDBManager.UpdateRoomCodeDictionary();

            if("1".Equals(CompanySettings.BringRateTypeDescFromCenRes) && CompanySettings.CompanyID == "7375")
            {
                DataDictionaryDataFlowTaskManager.DFT_MoveRateTypeDataForBiltmore();

                DataDictionaryDataFlowTaskManager.DFT_UpdateShowAlphaCodeData();

                CRMDBManager.UpdateNotesForRateType();
            }

            if(CompanySettings.CompanyID == "8012")
            {
                CRMDBManager.UpdateTargetTableName1AndTargetFieldName1ForRateType();
            }

            if (CompanySettings.CompanyID == "12960")
            {
                DataDictionaryDataFlowTaskManager.DFT_MoveRateTypeDataForSHGroup();

                DataDictionaryDataFlowTaskManager.DFT_MoveRoomTypeDataForSHGroup();
            }

            if (CompanySettings.CompanyID == "12951")
            {
                DataDictionaryDataFlowTaskManager.DFT_MoveRateTypeDataFor12951();
            }

            if (CompanySettings.CompanyID == "8525")
            {
                DataDictionaryDataFlowTaskManager.DFT_MoveRateTypeDataForAquaAston();
            }

            SSISLOGDBManager.LogPackageEnd(PackageLogID, BatchLogID, EndBatchAudit);
        }
    }
}
