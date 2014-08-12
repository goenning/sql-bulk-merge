using SqlBulkMerge.Test.Models;
using System.Collections.Generic;
using System.Configuration;
using System.Data.SqlClient;
using Xunit;

namespace SqlBulkMerge.Test
{
    public class SimpleBulkTest : BaseBulkTest
    {
        private Country brazil = new Country("BRA", "Brazil", "BRL", 202977000);
        private Country usa = new Country("USA", "United States of America", "USD", 318537000);
        private Country croatia = new Country("CRO", "Croatia", "HRK", 4284889);

        private void InsertUSA()
        {
            this.connection.Merge("dbo.countries", usa, (r, row) =>
            {
                row["code"] = r.Code;
                row["currency"] = r.Currency;
                row["name"] = r.Name;
                row["population"] = r.Population;
            }, new string[] { "code" }, new string[] { "code" });
        }

        private void InsertCroatia()
        {
            this.connection.Merge("dbo.countries", croatia, (r, row) =>
            {
                row["code"] = r.Code;
                row["currency"] = r.Currency;
                row["name"] = r.Name;
                row["population"] = r.Population;
            }, new string[] { "code" }, new string[] { "code" });
        }

        private void InsertBrazil()
        {
            this.connection.Merge("dbo.countries", brazil, (r, row) =>
            {
                row["code"] = r.Code;
                row["currency"] = r.Currency;
                row["name"] = r.Name;
                row["population"] = r.Population;
            }, new string[] { "code" }, new string[] { "code" });
        }

        private void InsertAllCountries()
        {
            this.connection.Merge("dbo.countries", new Country[] { brazil, usa, croatia }, (r, row) =>
            {
                row["code"] = r.Code;
                row["currency"] = r.Currency;
                row["name"] = r.Name;
                row["population"] = r.Population;
            }, new string[] { "code" }, new string[] { "code" });
        }

        [Fact]
        public void FullMerge_ShouldMatchNumberOfLines()
        {
            InsertAllCountries();

            int rows = this.TableCount("countries");
            Assert.Equal(3, rows);
        }

        [Fact]
        public void TwoIncrementalMerges_ShouldMatchNumberOfLines()
        {
            InsertBrazil();
            InsertCroatia();
            InsertUSA();

            int rows = this.TableCount("countries");
            Assert.Equal(3, rows);
        }

        [Fact]
        public void ShouldChangeOnlyBrazilName()
        {
            InsertAllCountries();

            this.connection.Merge("dbo.countries", new Country[] { new Country("BRA", "Brazilian", "BRL", 202977000), }, (r, row) =>
            {
                row["code"] = r.Code;
                row["currency"] = r.Currency;
                row["name"] = r.Name;
                row["population"] = r.Population;
            }, new string[] { "code" }, new string[] { "code" });

            int rows = this.TableCount("countries");
            Assert.Equal(3, rows);

            object name = this.GetSingleValue("select name from dbo.countries where code = 'BRA'");
            Assert.Equal("Brazilian", name);
        }

        [Fact]
        public void ShouldRemoveOnlyBrazil()
        {
            InsertAllCountries();

            this.connection.Merge("dbo.countries", new Country[] { croatia, usa }, (r, row) =>
            {
                row["code"] = r.Code;
                row["currency"] = r.Currency;
                row["name"] = r.Name;
                row["population"] = r.Population;
            }, new string[] { "code" });

            int rows = this.TableCount("countries");
            Assert.Equal(2, rows);

            rows = this.QueryCount("select count(*) from dbo.countries where code = 'BRA'");
            Assert.Equal(0, rows);
        }

        [Fact]
        public void ShouldNotRemoveAny()
        {
            InsertAllCountries();

            this.connection.Merge("dbo.countries", new Country[] { croatia, usa }, (r, row) =>
            {
                row["code"] = r.Code;
                row["currency"] = r.Currency;
                row["name"] = r.Name;
                row["population"] = r.Population;
            }, new string[] { "code" }, new string[] { "code" });

            int rows = this.TableCount("countries");
            Assert.Equal(3, rows);
        }

        [Fact]
        public void ShouldRemoveAllWhenMergeIsEmpty()
        {
            InsertAllCountries();

            this.connection.Merge("dbo.countries", new Country[0], (r, row) =>
            {
                row["code"] = r.Code;
                row["currency"] = r.Currency;
                row["name"] = r.Name;
                row["population"] = r.Population;
            }, new string[] { "code" });

            int rows = this.TableCount("countries");
            Assert.Equal(0, rows);
        }
    }
}
