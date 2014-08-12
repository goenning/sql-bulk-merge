using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkMerge.Test.Models
{
    public class Country
    {
        public string Code { get; set; }
        public string Name { get; set; }
        public string Currency { get; set; }
        public int? Population { get; set; }

        public Country(string code, string name, string currency, int? population)
        {
            this.Code = code;
            this.Name = name;
            this.Currency = currency;
            this.Population = population;
        }
    }
}
