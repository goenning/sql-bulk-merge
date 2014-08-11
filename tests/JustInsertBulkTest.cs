using SqlBulkMerge.Test.Models;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using Xunit;

namespace SqlBulkMerge.Test
{
    public class JustInsertBulkTest : BaseBulkTest
    {
        private List<Country> allCountries = new List<Country>()
        {
            new Country(){ Code = "BRA", Currency = "BRL", Name= "Brazil", Population = 202977000 },
            new Country(){ Code = "USA", Currency = "USD", Name= "United States of America", Population = 318537000 },
            new Country(){ Code = "CRO", Currency = "HRK", Name= "Croatia", Population = 4284889 }            
        };

        [Fact]
        public void InsertAllCountries()
        {
            string connectionString = ConfigurationManager.ConnectionStrings["SQLServer"].ConnectionString;
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                new SqlServerBulkUpsert(conn, null, "dbo", "countries", new string[] { "code" }, new string[] { "code" }).DoWith<Country>(allCountries, (r, row) =>
                {
                    row["code"] = r.Code;
                    row["currency"] = r.Currency;
                    row["name"] = r.Name;
                    row["population"] = r.Population;
                });
            }
        }

    }
}
