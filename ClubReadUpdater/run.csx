#r "Newtonsoft.Json"
#r "Microsoft.WindowsAzure.Storage"
#r "ClubReadUpdater.Lib.dll"
using System;
using ClubReadUpdater.Lib;

public async static

Task Run(string myQueueItem, 
    IQueryable<DdhpEvent> clubEvents, 
    IQueryable<Player> players,
    IQueryable<StorageStat> stats,
    CloudTable clubWriter, 
    TraceWriter log)
{
    _log =log;

    var runner = new Runner((message) => log.Info(message));

    await runner.Run(myQueueItem,
        clubEvents,
        players,
        stats,
        clubWriter);
}