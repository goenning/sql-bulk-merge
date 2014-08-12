using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SqlBulkMerge
{
    public class MergeResults
    {
        public int RowsInserted { get; private set; }
        public int RowsUpdated { get; private set; }
        public int RowsDeleted { get; private set; }

        public MergeResults(int rowsInserted, int rowsUpdated, int rowsDeleted)
        {
            this.RowsInserted = rowsInserted;
            this.RowsUpdated = rowsUpdated;
            this.RowsDeleted = rowsDeleted;
        }
    }
}
