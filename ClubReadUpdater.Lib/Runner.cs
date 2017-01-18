using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace ClubReadUpdater.Lib
{
    public class Runner
    {
        private static Action<string> _log;

        public Runner(Action<string> log)
        {
            _log = log;
        }

        public async Task Run(string myQueueItem,
            IQueryable<DdhpEvent> clubEvents,
            IQueryable<Player> players,
            IQueryable<StorageStat> stats,
            CloudTable clubWriter)
        {
            Guid id = Guid.Parse(myQueueItem);

            var events = clubEvents.Where(q => q.PartitionKey == id.ToString()).ToList();
            if (!events.Any()) { throw new Exception("I haven't any events!"); }
            _log($"Club events count: {events.Count}");

            var entity = Club.LoadFromEvents(clubEvents.Where(q => q.PartitionKey == id.ToString()));
            _log($"Club Name: {entity.ClubName} Id: {entity.Id}");

            foreach (var contract in entity.Contracts)
            {
                var player = players.Where(q => q.Id == contract.PlayerId).ToList();

                if (!player.Any())
                {
                    _log($"Cannot find player for id {contract.PlayerId}");
                    continue;
                }

                contract.Player = player.Single();
            }

            var years = entity.Contracts.Select(q => q.FromRound / 100).ToList();
            years.AddRange(entity.Contracts.Select(q => q.ToRound / 100));

            var distinctYears = years.Distinct();

            var tasks = new List<Task>(distinctYears.Count());

            foreach (var year in distinctYears)
            {
                var yearStats = new List<StorageStat>(500);

                Stopwatch timer = new Stopwatch();
                timer.Start();

                for (int i = 0; i < 24; i++)
                {
                    yearStats.AddRange(stats.Where(stat => stat.PartitionKey == $"{year}{i.ToString("00")}"));
                }

                var playerStats = yearStats.GroupBy(stat => stat.PlayerId).ToDictionary(q => q.Key, q => q.ToList());
                timer.Stop();
                _log($"{year} Loaded stats in {timer.ElapsedMilliseconds} ms");

                var clubSeason = new ClubSeason(year, entity, playerStats);

                var upsert = TableOperation.InsertOrReplace(clubSeason);
                tasks.Add(clubWriter.ExecuteAsync(upsert));

                _log($"Wrote {year}");
            }

            await Task.WhenAll(tasks);
        }
    }
}
