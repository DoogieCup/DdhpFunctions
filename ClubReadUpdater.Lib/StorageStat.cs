using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace ClubReadUpdater.Lib
{
    public class StorageStat : TableEntity
    {
        private Guid _playerId;
        private int _round;

        public Guid PlayerId
        {
            get { return _playerId; }
            set
            {
                RowKey = value.ToString();
                _playerId = value;
            }
        }

        public int Round
        {
            get { return _round; }
            set
            {
                PartitionKey = value.ToString();
                _round = value;
            }
        }

        public int Goals { get; set; }
        public int Behinds { get; set; }
        public int Disposals { get; set; }
        public int Marks { get; set; }
        public int Hitouts { get; set; }
        public int Tackles { get; set; }
        public int Kicks { get; set; }
        public int Handballs { get; set; }
        public int GoalAssists { get; set; }
        // ReSharper disable once InconsistentNaming
        public int Inside50s { get; set; }
        public int FreesFor { get; set; }
        public int FreesAgainst { get; set; }
        // ReSharper disable once InconsistentNaming
        public Guid AflClubId { get; set; }
    }
}