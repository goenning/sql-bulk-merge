using System;
using System.Configuration;
using System.Data.SqlClient;
using Xunit;

namespace SqlBulkMerge.Test
{
    public abstract class BaseBulkTest : IDisposable
    {
        protected SqlConnection connection;
        public BaseBulkTest()
        {
            string connectionString = ConfigurationManager.ConnectionStrings["SQLServer"].ConnectionString;
            this.connection = new SqlConnection(connectionString);
            this.connection.Open();
        }

        public void Dispose()
        {
            this.connection.Dispose();
        }

        public int TableCount(string table)
        {
            using (SqlCommand cmd = new SqlCommand(string.Format("select count(*) from {0}", table), this.connection))
            {
                return (int)cmd.ExecuteScalar();
            }
        }

        public int QueryCount(string sql)
        {
            using (SqlCommand cmd = new SqlCommand(sql, this.connection))
            {
                return (int)cmd.ExecuteScalar();
            }
        }

        public object GetSingleValue(string sql)
        {
            using (SqlCommand cmd = new SqlCommand(sql, this.connection))
            {
                return cmd.ExecuteScalar();
            }
        }
    }
}
