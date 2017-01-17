using System;
using System.Collections.Generic;
using System.Linq;

namespace ClubReadUpdater.Lib
{
    public class ClubSeason : ComplexEntity
    {
        public ClubSeason()
        {
        }

        public ClubSeason(int year, Club club, IDictionary<Guid, List<StorageStat>> stats)
        {
            Id = club.Id;
            CoachName = club.CoachName;
            ClubName = club.ClubName;
            Email = club.Email;
            Year = year;
            Contracts =
                club.Contracts.Where(q => q.FromRound <= Int32.Parse($"{year}24") && q.ToRound >= Int32.Parse($"{year}01"))
                    .ToList();

            foreach (var contract in Contracts)
            {
                if (!stats.ContainsKey(contract.PlayerId))
                {
                    continue;
                }

                contract.Stats = stats[contract.PlayerId].Select(stat => (ReadStat)stat);
            }
        }

        private string _clubName { get; set; }
        public Guid Id { get; set; }

        public string CoachName { get; set; }
        public string ClubName
        {
            get
            {
                return _clubName;
            }
            set
            {
                _clubName = value;
                RowKey = value.ToString();
            }
        }
        public string Email { get; set; }
        public int Year
        {
            get
            {
                return Int32.Parse(PartitionKey);
            }
            set
            {
                PartitionKey = value.ToString();
            }
        }

        [Serialize]
        public IEnumerable<Contract> Contracts { get; set; }

        public int Version { get; set; }
    }
}