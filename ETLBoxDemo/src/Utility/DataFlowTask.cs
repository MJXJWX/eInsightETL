using ALE.ETLBox;
using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.DataFlow;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using static ALE.ETLBox.ControlFlow.DbTask;

namespace ETLBoxDemo.src.Utility
{
    public class DataFlowTask<T>
    {
        public void runTask(string sConnString, string dConnString, string dTableName, string sql, bool isUpdate = false, bool isInsert = false, List<string> primaryKey = null, List<string> updateFields = null)
        {
            //Type of Data Flow Task
            ActionType actionType = ActionType.Upsert;
            if (isInsert && isUpdate)
            {
                actionType = ActionType.Upsert;
            }
            else if (isInsert)
            {
                actionType = ActionType.Insert;
            }
            else if (isUpdate)
            {
                actionType = ActionType.Update;
            }
            else
            {
                //return;
            }
            
            if (actionType == ActionType.Upsert && (primaryKey == null || primaryKey.Count == 0))
            {
                actionType = ActionType.Insert;
            }

            //Execute Task Flow task
            DBSource<T> dBSource = new DBSource<T>() {
                ConnString = sConnString,
                Sql = sql
            };
            RowTransformation<T, T> rowT = new RowTransformation<T, T>(
                input => input
                );
            DBDestination<T> dBDestination = new DBDestination<T>(dTableName) {
                ConnString = dConnString,
                ActionType = actionType,
                Keys = primaryKey,
                UpdateFields = updateFields
            };
            dBSource.LinkTo(rowT);
            rowT.LinkTo(dBDestination);
            dBSource.Execute();
            dBDestination.Wait();
        }
    }

    public class DataFlowTask<T, D>
    {
        public void runTask(string sConnString, string dConnString, string dTableName, string sql, Dictionary<string, string> mapping, bool isUpdate = false, bool isInsert = false, List<string> primaryKey = null, List<string> updateFields = null)
        {
            if ((typeof(T) != typeof(D)) && (mapping == null || mapping.Count == 0))
            {
                throw new KeyNotFoundException("Missing mapping of table data and destination table");
            }

            //Type of Data Flow Task
            ActionType actionType = ActionType.Upsert;
            if (isInsert && isUpdate)
            {
                actionType = ActionType.Upsert;
            }
            else if (isInsert)
            {
                actionType = ActionType.Insert;
            }
            else if (isUpdate)
            {
                actionType = ActionType.Update;
            }
            else
            {
                //return;
            }
            
            if (actionType == ActionType.Upsert && (primaryKey == null || primaryKey.Count == 0))
            {
                actionType = ActionType.Insert;
            }

            //Execute Task Flow task
            DBSource<T> dBSource = new DBSource<T>()
            {
                ConnString = sConnString,
                Sql = sql
            };
            RowTransformation<T, D> rowT = new RowTransformation<T, D>(
                input =>
                {
                    D result = (D)Activator.CreateInstance(typeof(D));
                    if(typeof(T) == typeof(D))
                    {
                        foreach (var property in typeof(D).GetProperties())
                        {
                            result.GetType().GetProperty(property.Name).SetValue(result, input.GetType().GetProperty(property.Name).GetValue(input));
                        }
                    }
                    else
                    {
                        foreach (var key in mapping.Keys)
                        {
                            result.GetType().GetProperty(mapping[key]).SetValue(result, input.GetType().GetProperty(key).GetValue(input));
                        }
                    }
                    return result;
                }
                );
            DBDestination<D> dBDestination = new DBDestination<D>(dTableName) {
                ConnString = dConnString,
                ActionType = actionType,
                Keys = primaryKey,
                UpdateFields = updateFields
            };
            dBSource.LinkTo(rowT);
            rowT.LinkTo(dBDestination);
            dBSource.Execute();
            dBDestination.Wait();
        }
    }

    public class DataFlowTask<T, D, L>
    {
        public class LookupKey
        {
            private Dictionary<string, string> LookupMapping { get; set; }
            private Dictionary<string, string> LookupKeys { get; set; }

            public LookupKey(Dictionary<string, string> lookupKeys, Dictionary<string, string> lookupMapping)
            {
                this.LookupKeys = lookupKeys;
                this.LookupMapping = lookupMapping;
            }

            public List<L> LookupData { get; set; } = new List<L>();

            public T FindKey(T resultRow)
            {
                var obj = LookupData.Where(o => {
                    foreach (var key in LookupKeys.Keys)
                    {
                        if (!(o.GetType().GetProperty(key).GetValue(o) + "").Equals(resultRow.GetType().GetProperty(LookupKeys[key]).GetValue(resultRow)+""))
                        {
                            return false;
                        }
                    }
                    return true;
                }).FirstOrDefault();
                foreach (var key in LookupMapping.Keys)
                {
                    resultRow.GetType().GetProperty(LookupMapping[key]).SetValue(resultRow, obj?.GetType().GetProperty(key).GetValue(obj)?.ToString());
                }
                return resultRow;
            }
        }

        public void runTask(string sConnString, string dConnString, string lConnString, string dTableName, string sql, string lookupSql, Dictionary<string, string> lookupKey, Dictionary<string, string> lookupMapping, Dictionary<string, string> mapping, bool isUpdate = false, bool isInsert = false, List<string> primaryKey = null, List<string> updateFields = null)
        {
            if ((typeof(T) != typeof(D)) && (mapping == null || mapping.Count == 0))
            {
                throw new KeyNotFoundException("Missing mapping of table data and destination table");
            }

            if (string.IsNullOrEmpty(lookupSql) || lookupKey == null || lookupKey.Count == 0 || lookupMapping == null || lookupMapping.Count == 0)
            {
                throw new KeyNotFoundException("Missing sql or mapping of look up data");
            }

            //Type of Data Flow Task
            ActionType actionType = ActionType.Upsert;
            if (isInsert && isUpdate)
            {
                actionType = ActionType.Upsert;
            }
            else if (isInsert)
            {
                actionType = ActionType.Insert;
            }
            else if (isUpdate)
            {
                actionType = ActionType.Update;
            }
            else
            {
                //return;
            }
            
            if (actionType != ActionType.Insert && (primaryKey == null || primaryKey.Count == 0 || updateFields == null || updateFields.Count == 0))
            {
                actionType = ActionType.Insert;
            }

            //Execute Task Flow task
            DBSource<T> dBSource = new DBSource<T>()
            {
                Sql = sql,
                ConnString = sConnString
            };
            DBSource<L> lSource = new DBSource<L>(lookupSql) { ConnString = lConnString };

            LookupKey lookupkey = new LookupKey(lookupKey, lookupMapping);
            Lookup<T, T, L> lookup = new Lookup<T, T, L>(
                lookupkey.FindKey,
                lSource,
                lookupkey.LookupData
            );

            Multicast<T> multiCast = new Multicast<T>();

            RowTransformation<T, D> rowT = new RowTransformation<T, D>(
                input =>
                {
                    D result = (D)Activator.CreateInstance(typeof(D));
                    if (typeof(T) == typeof(D))
                    {
                        foreach (var property in typeof(D).GetProperties())
                        {
                            result.GetType().GetProperty(property.Name).SetValue(result, input.GetType().GetProperty(property.Name).GetValue(input));
                        }
                    }
                    else
                    {
                        foreach (var key in mapping.Keys)
                        {
                            result.GetType().GetProperty(mapping[key]).SetValue(result, input.GetType().GetProperty(key).GetValue(input));
                        }
                    }
                    return result;
                }
                );
            DBDestination<D> dBDestination = new DBDestination<D>(dTableName)
            {
                ActionType = actionType,
                ConnString = dConnString,
                Keys = primaryKey,
                UpdateFields = updateFields
            };
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(sConnString));
            dBSource.LinkTo(lookup);
            lookup.LinkTo(multiCast);
            multiCast.LinkTo(rowT);
            rowT.LinkTo(dBDestination);
            dBSource.Execute();
            ControlFlow.CurrentDbConnection = new SqlConnectionManager(new ConnectionString(dConnString));
            dBDestination.Wait();
        }
    }

}
