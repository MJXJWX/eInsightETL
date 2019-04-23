using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ETLBox.src.Toolbox.Database;
using System;
using System.Collections.Generic;
using System.Text;

namespace ETLBoxDemo.src.Manager
{
    public static class PMSDBManager
    {
        public static readonly string SQL_GetDataFromProfileDocument = 
            @"SELECT Id ,
                    FK_Profile ,
                    DocType ,
                    DocSource ,
                    CodeOnDocument ,
                    DocNotes ,
                    DocId_PII ,
                    NameOnDocument_PII ,
                    DocumentBody_PII ,
                    NationalityOnDocument ,
                    EffectiveDate ,
                    ExpirationDate ,
                    PII_StoredAs ,
                    PII_Algorithm ,
                    PII_Key ,
                    PII_KeyId ,
                    Issuer ,
                    IssuerAddress1 ,
                    IssuerAddress2 ,
                    IssuerCity ,
                    IssuerStateProv ,
                    IssuerPostalCode ,
                    IssuerCountry ,
                    IsPrimary ,
                    InactiveDate ,
                    DateCreated ,
                    LastUpdated FROM dbo.ProfileDocuments With(Nolock)
            WHERE (LastUpdated >= '2012-03-12 20:50:00' OR DateCreated >= '2012-03-12 20:50:00') 
            AND LastUpdated <= '2012-01-24 11:06:00' and DateCreated <= '2012-01-24 11:06:00'";

        public static List<string> GetDataFromProfileDocuments(string connectionString)
        {
            return SQLHelper.GetDbValues(connectionString, "GetDataFromProfileDocument", SQL_GetDataFromProfileDocument, null);
        }
        
    }
}
