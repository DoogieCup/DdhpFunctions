#r "Newtonsoft.Json"
#r "Microsoft.WindowsAzure.Storage"
using System;
using System.Diagnostics.Contracts;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Runtime.Remoting.Metadata.W3cXsd2001;

public async static Task Run(string myQueueItem, 
    IQueryable<DdhpEvent> clubEvents, 
    IQueryable<Player> players,
    CloudTable clubWriter, 
    TraceWriter log)
{
    _log = log;

    Guid id = Guid.Parse(myQueueItem);

    var events = clubEvents.Where(q => q.PartitionKey == id.ToString()).ToList();
    log.Info($"Club events count: {events.Count}");

    var entity = Club.LoadFromEvents(clubEvents.Where(q => q.PartitionKey == id.ToString()));
    log.Info($"Club Name: {entity.ClubName} Id: {entity.Id}");

    var contracts = entity._contracts;

    foreach (var contract in contracts)
    {
        var player = players.Where(q => q.Id == contract.PlayerId).ToList();

        if (!player.Any())
        {
            log.Info($"Cannot find player for id {contract.PlayerId}");
            continue;
        }

        contract.SetPlayer(player.Single());
    }

    var years = entity.ContractsRead.Select(q => q.FromRound / 100).ToList();
    years.AddRange(entity.ContractsRead.Select(q => q.ToRound / 100));

    var distinctYears = years.Distinct();

    var tasks = new List<Task>(distinctYears.Count());

    foreach (var year in distinctYears)
    {
        var clubSeason = new ClubSeason(year, entity);

        var upsert = TableOperation.InsertOrReplace(clubSeason);
        tasks.Add(clubWriter.ExecuteAsync(upsert));
    }

    await Task.WhenAll(tasks);
}

private static TraceWriter _log;

public class ClubSeason : TableEntity
{
    public ClubSeason() { }

    public ClubSeason(int year, Club club)
    {
        Id = club.Id;
        CoachName = club.CoachName;
        ClubName = club.ClubName;
        Email = club.Email;
        Year = year;
        _contracts = club.ContractsRead.Where(q => q.FromRound <= int.Parse($"{year}24") && q.ToRound >= int.Parse($"{year}01")).ToList();
    }

    private string _clubName{get;set;}
    public Guid Id{get;set;}

    public string CoachName { get; set; }
    public string ClubName
    {
        get
        {
            return _clubName;
        }
        set{
            _clubName = value;
            RowKey = value.ToString();
        }
    }
    public string Email { get; set; }
    public int Year
    {
        get
        {
            return int.Parse(PartitionKey);
        }
        set
        {
            PartitionKey = value.ToString();
        }
    }

    public List<Contract> _contracts = new List<Contract>();

    public string Contracts
    {
        get { return JsonConvert.SerializeObject(_contracts); }
        set { _contracts = (List<Contract>)JsonConvert.DeserializeObject<List<Contract>>(value); }
    }

    public int Version { get; set; }
}

public class Club : TableEntity
{
    private Guid _id;
    public Guid Id
    {
        get { return _id; }
        set
        {
            _id = value;
            RowKey = value.ToString();
        }
    }

    public string CoachName { get; set; }
    public string ClubName { get; set; }
    public string Email { get; set; }

    public List<Contract> _contracts = new List<Contract>();

    public IEnumerable<Contract> ContractsRead => _contracts;

    private Club()
    {
        Version = -1;
        PartitionKey = "ALL_CLUBS";
    }

    public string Contracts
    {
        get { return JsonConvert.SerializeObject(_contracts); }
        set { _contracts = (List<Contract>)JsonConvert.DeserializeObject<List<Contract>>(value); }
    }

    public int Version { get; set; }

    protected void ReplayEvent(DdhpEvent e)
    {
        ClubEvent type;
        if (!Enum.TryParse(e.EventType, true, out type))
        {
            throw new Exception($"Could not identify ClubEvent type {e.EventType}");
        }

        switch (type)
        {
            case ClubEvent.ClubCreated:
                _log.Info($"Processing club created event {e.Payload}");
                var castEvent = GetPayload<ClubCreatedEvent>(e);
                Id = Guid.Parse(e.PartitionKey);
                Email = castEvent.Email;
                CoachName = castEvent.CoachName;
                ClubName = castEvent.ClubName;
                return;
            case ClubEvent.ContractImported:
                var contractImportedEvent = GetPayload<ContractImportedEvent>(e);
                _log.Info($"Processing ContractImported event {e.Payload}");
                _contracts.Add(new Contract(contractImportedEvent.PlayerId,
                    contractImportedEvent.FromRound,
                    contractImportedEvent.ToRound,
                    contractImportedEvent.DraftPick));
                return;
        }
    }

    public static Club LoadFromEvents(IEnumerable<DdhpEvent> events)
    {
        var entity = new Club();

        foreach (var e in events.OrderBy(q => q.RowKey))
        {
            entity.ReplayEvent(e);

            var version = int.Parse(e.RowKey);
            if (version != entity.Version + 1)
            {
                throw new Exception($"Events out of order. At version {entity.Version} but received event {version}");
            }
            entity.Version = version;
        }

        return entity;
    }

    private T GetPayload<T>(DdhpEvent e)
    {
        return (T)JsonConvert.DeserializeObject<T>(e.Payload);
    }

    private enum ClubEvent
    {
        ClubCreated,
        ContractImported
    }
}

public class Contract
{
    public Contract(Guid playerId,
        int fromRound,
        int toRound,
        int draftPick)
    {
        PlayerId = playerId;
        FromRound = fromRound;
        ToRound = toRound;
        DraftPick = draftPick;
    }
    public Guid PlayerId { get; set; }
    public int FromRound { get; set; }
    public int ToRound { get; set; }
    public int DraftPick { get; set; }
    public string Player { get; set; }

    public void SetPlayer(Player player)
    {
        Player = JsonConvert.SerializeObject(player);
    }
}

public class DdhpEvent : TableEntity
{
    public string EventType { get; set; }
    public string Payload { get; set; }
}

public class ClubCreatedEvent
{
    public ClubCreatedEvent(Guid id,
        string clubName,
        string coachName,
        string email)
    {
        Id = id;
        ClubName = clubName;
        CoachName = coachName;
        Email = email;
    }
    public ClubCreatedEvent()
    {

    }

    public Guid Id { get; set; }
    public string ClubName { get; set; }
    public string CoachName { get; set; }
    public string Email { get; set; }
}

public class ContractImportedEvent
{
    public ContractImportedEvent(Guid playerId,
        int fromRound,
        int toRound,
        int draftPick)
    {
        PlayerId = playerId;
        FromRound = fromRound;
        ToRound = toRound;
        DraftPick = draftPick;
    }
    public Guid PlayerId { get; set; }
    public int FromRound { get; set; }
    public int ToRound { get; set; }
    public int DraftPick { get; set; }
}

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