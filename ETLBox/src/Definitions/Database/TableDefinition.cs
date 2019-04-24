using ALE.ETLBox.ConnectionManager;
using ALE.ETLBox.ControlFlow;
using ALE.ETLBox.Helper;
using System.Collections.Generic;
using System.Data;
using System.Linq;

namespace ALE.ETLBox
{
    public class TableDefinition
    {
        public string Name { get; set; }
        public List<TableColumn> Columns { get; set; }
        public int? IDColumnIndex
        {
            get
            {
                TableColumn idCol = Columns.FirstOrDefault(col => col.IsIdentity);
                if (idCol != null)
                    return Columns.IndexOf(idCol);
                else
                    return null;
            }
        }

        public string AllColumnsWithoutIdentity => Columns.Where(col => !col.IsIdentity).AsString();


        public TableDefinition()
        {
            Columns = new List<TableColumn>();
        }

        public TableDefinition(string name) : this()
        {
            Name = name;
        }

        public TableDefinition(string name, List<TableColumn> columns) : this(name)
        {
            Columns = columns;
        }

        public void CreateTable()
        {
            CreateTableTask.Create(this);
        }

        internal static TableDefinition GetDefinitionFromTableName(string tableName, IConnectionManager connection)
        {
            TableDefinition result = new TableDefinition(tableName);
            TableColumn curCol = null;
            var readMetaSql = new SqlTask($"Read column meta data for table {tableName}",
             $@" select cols.name, tpes.name as colname, cols.is_nullable, cols.is_identity
  from sys.columns cols
  inner join sys.tables tbl
   on cols.object_id = tbl.object_id
  inner join sys.schemas sc
   on tbl.schema_id = sc.schema_id
   inner join sys.systypes tpes
    on tpes.xtype = cols.system_type_id
  where sc.name + '.' + tbl.name  =  '{tableName}'
  and tbl.type = 'U'
  and tpes.name <> 'sysname'"
            , () => { curCol = new TableColumn(); }
            , () => { result.Columns.Add(curCol); }
            , reader => {
                curCol.Name = DbTask.GetValueFromReader(reader, "name") + "";
                curCol.DataType = DbTask.GetValueFromReader(reader, "colname") + "";
                curCol.AllowNulls = bool.Parse(DbTask.GetValueFromReader(reader, "is_nullable") + "");
                curCol.IsIdentity = bool.Parse(DbTask.GetValueFromReader(reader, "is_identity") + "");
              }
             )
            {
                DisableLogging = true,
                DisableExtension = true,
                ConnectionManager = connection
            };
            readMetaSql.ExecuteReader();
            return result;
        }


    }
}
