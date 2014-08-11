using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace SqlBulkMerge
{
    public class SqlServerBulkUpsert
    {
        private SqlConnection conn;
        private SqlTransaction trans;
        private string tableName;
        private string tempTableName;
        private string[] keyColumns;
        private string[] deleteKeyColumns;

        public SqlServerBulkUpsert(SqlConnection conn, SqlTransaction trans, string schema, string tableName, string[] keyColumns, string[] deleteKeyColumns)
        {
            this.conn = conn;
            this.trans = trans;
            this.tempTableName = string.Format("#{0}", tableName);
            this.tableName = string.Format("{0}.{1}", schema, tableName);
            this.keyColumns = keyColumns;
            this.deleteKeyColumns = deleteKeyColumns;

            if (this.conn.State == ConnectionState.Closed)
                this.conn.Open();
        }

        public void DoWith(IDataReader dr)
        {
            DataTable tableSchema = GetTableSchema();
            this.CreateTempTable(tableSchema);
            this.BulkInsertTo(dr);
            this.MergeTempTableWithTargetTable(tableSchema);
            this.DropTempTable();
        }

        public void BulkInsertTo(IDataReader dr)
        {
            DataTable dt = this.BuildDataTable();

            dt.Rows.Clear();

            while (dr.Read())
            {
                DataRow row = dt.NewRow();
                foreach (DataColumn column in dt.Columns)
                {
                    object value = dr.GetValue(dr.GetOrdinal(column.ColumnName));
                    if (value == DBNull.Value && !column.AllowDBNull)
                    {
                        if (column.DataType.Equals(typeof(String)))
                            value = "";
                        else
                            value = Activator.CreateInstance(column.DataType);
                    }
                    row[column] = value;
                }
                dt.Rows.Add(row);

                if (dt.Rows.Count == 50000)
                {
                    using (SqlBulkCopy bulk = new SqlBulkCopy(this.conn, SqlBulkCopyOptions.TableLock, this.trans))
                    {
                        bulk.DestinationTableName = this.tempTableName;
                        bulk.WriteToServer(dt);
                    }
                    dt.Rows.Clear();
                }
            }

            if (dt.Rows.Count > 0)
            {
                using (SqlBulkCopy bulk = new SqlBulkCopy(this.conn, SqlBulkCopyOptions.TableLock, this.trans))
                {
                    bulk.DestinationTableName = this.tempTableName;
                    bulk.WriteToServer(dt);
                }
            }
        }

        public void DoWith<T>(IEnumerable<T> items, Action<T, DataRow> itemToRow)
        {
            DataTable tableSchema = GetTableSchema();
            this.CreateTempTable(tableSchema);
            this.BulkInsertTo(items, itemToRow);
            this.MergeTempTableWithTargetTable(tableSchema);
            this.DropTempTable();
        }

        private void BulkInsertTo<T>(IEnumerable<T> items, Action<T, DataRow> itemToRow)
        {
            DataTable dt = this.BuildDataTable();

            dt.Rows.Clear();

            foreach (T item in items)
            {
                DataRow row = dt.NewRow();
                itemToRow(item, row);
                dt.Rows.Add(row);

                if (dt.Rows.Count == 50000)
                {
                    using (SqlBulkCopy bulk = new SqlBulkCopy(this.conn, SqlBulkCopyOptions.TableLock, this.trans))
                    {
                        bulk.BulkCopyTimeout = 600;
                        bulk.DestinationTableName = this.tempTableName;
                        bulk.WriteToServer(dt);
                    }
                    dt.Rows.Clear();
                }
            }

            if (dt.Rows.Count > 0)
            {
                using (SqlBulkCopy bulk = new SqlBulkCopy(this.conn, SqlBulkCopyOptions.TableLock, this.trans))
                {
                    bulk.BulkCopyTimeout = 600;
                    bulk.DestinationTableName = this.tempTableName;
                    bulk.WriteToServer(dt);
                }
            }
        }

        private void MergeTempTableWithTargetTable(DataTable schema)
        {
            string[] columns = schema.Rows.OfType<DataRow>()
                                          .Where(x => Convert.ToBoolean(x["IsIdentity"]) == false)
                                          .Select(x => x["ColumnName"].ToString()).ToArray();

            string[] keyColumnsCondition = new string[keyColumns.Length];
            for (int i = 0; i < keyColumnsCondition.Length; i++)
                keyColumnsCondition[i] = string.Format("target.{0} = source.{0}", keyColumns[i]);

            string[] allColumns = columns.ToArray();

            string[] columnsToUpdate = allColumns.Where(c => !keyColumns.Contains(c)).ToArray();
            string[] columnUpdateString = new string[columnsToUpdate.Length];
            for (int i = 0; i < columnUpdateString.Length; i++)
                columnUpdateString[i] = string.Format("target.{0} = source.{0}", columnsToUpdate[i]);

            string updateWhenStmt = string.Format(" WHEN MATCHED THEN UPDATE SET {0}", string.Join(",", columnUpdateString));
            string insertWhenStmt = string.Format(" WHEN NOT MATCHED BY TARGET THEN INSERT ({0}) VALUES ({1})",
                                        string.Join(",", allColumns),
                                        string.Join(",", allColumns.Select(x => "source." + x)));

            string sql = string.Format(
                         @"MERGE {0} AS target
                           USING {1} AS source
                           ON {2}",
                           this.tableName,
                           this.tempTableName,
                           string.Join(" AND ", keyColumnsCondition));

            if (columnUpdateString.Count() > 0)
                sql += updateWhenStmt;
            if (allColumns.Select(x => "source." + x).Count() > 0)
                sql += insertWhenStmt;

            if (deleteKeyColumns.Length == 0)
            {
                sql += " WHEN NOT MATCHED BY source THEN DELETE";
            }
            else
            {
                string[] columnDeleteString = new string[deleteKeyColumns.Length];
                for (int i = 0; i < deleteKeyColumns.Length; i++)
                    columnDeleteString[i] = string.Format("{0} <> target.{0}", deleteKeyColumns[i]);
                sql += string.Format(" WHEN NOT MATCHED BY source AND {0} THEN DELETE", string.Join(" AND ", columnDeleteString));
            }

            sql += ";";

            using (SqlCommand cmd = new SqlCommand(sql, this.conn, this.trans))
            {
                cmd.CommandTimeout = Convert.ToInt32(TimeSpan.FromMinutes(30).TotalSeconds);
                cmd.ExecuteNonQuery();
            }
        }

        private void CreateTempTable(DataTable schema)
        {
            this.DropTempTable();
            string sql = this.CreateTemporaryTableSql(schema);
            using (SqlCommand cmd = new SqlCommand(sql, this.conn, this.trans))
                cmd.ExecuteNonQuery();
        }

        private void DropTempTable()
        {
            string sql = string.Format(@"IF OBJECT_ID('tempdb..{0}') IS NOT NULL
                                         BEGIN
                                              DROP TABLE {0}
                                         END", this.tempTableName);
            using (SqlCommand cmd = new SqlCommand(sql, this.conn, this.trans))
                cmd.ExecuteNonQuery();
        }

        private DataTable BuildDataTable()
        {
            DataTable dt = new DataTable();
            using (SqlCommand cmd = new SqlCommand(string.Format("SELECT TOP 0 * FROM {0}", this.tableName), this.conn, this.trans))
            {
                using (SqlDataAdapter dr = new SqlDataAdapter(cmd))
                {
                    dr.FillSchema(dt, SchemaType.Source);
                }
            }
            return dt;
        }

        private DataTable GetTableSchema()
        {
            DataTable dt = new DataTable();
            using (SqlCommand cmd = new SqlCommand(string.Format("SELECT TOP 0 * FROM {0}", this.tableName), this.conn, this.trans))
            {
                using (SqlDataReader dr = cmd.ExecuteReader(CommandBehavior.SchemaOnly))
                {
                    dt = dr.GetSchemaTable();
                }
            }
            return dt;
        }

        private string CreateTemporaryTableSql(DataTable schema)
        {
            string[] sqlColumns = new string[schema.Rows.Count];
            for (int i = 0; i < sqlColumns.Length; i++)
                sqlColumns[i] = string.Format("[{0}] {1}", schema.Rows[i]["ColumnName"], SQLGetType(schema.Rows[i]));

            return string.Format("CREATE TABLE [{0}] ({1})", this.tempTableName, string.Join(",", sqlColumns));
        }

        private static string SQLGetType(object type, int columnSize, int numericPrecision, int numericScale)
        {
            switch (type.ToString())
            {
                case "System.String":
                    return "VARCHAR(" + ((columnSize == -1) ? 255 : columnSize) + ") COLLATE Latin1_General_CI_AS";

                case "System.Decimal":
                case "System.Double":
                case "System.Single":
                    return string.Format("NUMERIC ({0},{1})", numericPrecision, numericScale);

                case "System.Int64":
                    return "BIGINT";

                case "System.Int16":
                case "System.Int32":
                    return "INT";

                case "System.DateTime":
                    return "DATETIME";

                case "System.Boolean":
                    return "BIT";

                default:
                    throw new Exception(type.ToString() + " not implemented.");
            }
        }

        private static string SQLGetType(DataRow schemaRow)
        {
            return SQLGetType(schemaRow["DataType"],
                                int.Parse(schemaRow["ColumnSize"].ToString()),
                                int.Parse(schemaRow["NumericPrecision"].ToString()),
                                int.Parse(schemaRow["NumericScale"].ToString()));
        }

        private static string SQLGetType(DataColumn column)
        {
            return SQLGetType(column.DataType, column.MaxLength, 18, 0);
        }
    }
}
