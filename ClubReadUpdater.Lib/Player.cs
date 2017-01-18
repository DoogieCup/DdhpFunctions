using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace ClubReadUpdater.Lib
{
    public class Player : TableEntity
    {
        private Guid _id;

        public Guid Id
        {
            get { return _id; }
            set
            {
                RowKey = value.ToString();
                PartitionKey = value.ToString().Substring(0, 1);
                _id = value;
            }
        }

        public string Name { get; set; }

        public Guid CurrentAflClubId { get; set; }

        public bool Active { get; set; }
        public string FootywireName { get; set; }

        [IgnoreProperty]
        public int LegacyId { get; set; }
    }
}