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


        private List<Country> onlyBrazilWithDifferentName = new List<Country>()
        {
            new Country(){ Code = "BRA", Currency = "BRL", Name= "Brazilian", Population = 202977000 },
        };

        private void InsertAllCountries(SqlConnection conn)
        {
            new SqlServerBulkUpsert(conn, null, "dbo", "countries", new string[] { "code" }, new string[] { "code" }).DoWith<Country>(allCountries, (r, row) =>
            {
                row["code"] = r.Code;
                row["currency"] = r.Currency;
                row["name"] = r.Name;
                row["population"] = r.Population;
            });
        }

        [Fact]
        public void OnFullMerge_ShouldMatchNumberOfLines()
        {
            string connectionString = ConfigurationManager.ConnectionStrings["SQLServer"].ConnectionString;
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                InsertAllCountries(conn);

                using (SqlCommand cmd = new SqlCommand("select count(*) from countries", conn))
                {
                    int rows = (int)cmd.ExecuteScalar();
                    Assert.Equal(allCountries.Count, rows);
                }
            }
        }

        [Fact]
        public void ShouldChangeOnlyBrazilName()
        {
            string connectionString = ConfigurationManager.ConnectionStrings["SQLServer"].ConnectionString;
            using (SqlConnection conn = new SqlConnection(connectionString))
            {
                conn.Open();
                InsertAllCountries(conn);

                new SqlServerBulkUpsert(conn, null, "dbo", "countries", new string[] { "code" }, new string[] { "code" }).DoWith<Country>(onlyBrazilWithDifferentName, (r, row) =>
                {
                    row["code"] = r.Code;
                    row["currency"] = r.Currency;
                    row["name"] = r.Name;
                    row["population"] = r.Population;
                });

                using (SqlCommand cmd = new SqlCommand("select count(*) from countries", conn))
                {
                    int rows = (int)cmd.ExecuteScalar();
                    Assert.Equal(allCountries.Count, rows);
                }

                using (SqlCommand cmd = new SqlCommand("select name from countries where code = 'BRA'", conn))
                {
                    string name = (string)cmd.ExecuteScalar();
                    Assert.Equal("Brazilian", name);
                }
            }
        }

    }
}
