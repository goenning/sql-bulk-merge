using SqlBulkMerge;
using System.Collections.Generic;

namespace System.Data.SqlClient
{
    public static class SqlConnectionExtension
    {
        public static MergeResults Merge<T>(this SqlConnection conn, string targetTable, T item, Action<T, DataRow> itemToRow, string[] keyColumns)
        {
            return conn.Merge(targetTable, new List<T> { item }, itemToRow, keyColumns, new string[0]);
        }

        public static MergeResults Merge<T>(this SqlConnection conn, string targetTable, T item, Action<T, DataRow> itemToRow, string[] keyColumns, string[] deleteKeyColumns)
        {
            return conn.Merge(targetTable, new List<T> { item }, itemToRow, keyColumns, deleteKeyColumns);
        }

        public static MergeResults Merge<T>(this SqlConnection conn, string targetTable, IEnumerable<T> items, Action<T, DataRow> itemToRow, string[] keyColumns)
        {
            var bulker = new SqlServerBulkUpsert(conn, null, targetTable, keyColumns, new string[0]);
            return bulker.DoWith(items, itemToRow);
        }

        public static MergeResults Merge<T>(this SqlConnection conn, string targetTable, IEnumerable<T> items, Action<T, DataRow> itemToRow, string[] keyColumns, string[] deleteKeyColumns)
        {
            var bulker = new SqlServerBulkUpsert(conn, null, targetTable, keyColumns, deleteKeyColumns);
            return bulker.DoWith(items, itemToRow);
        }
    }
}
