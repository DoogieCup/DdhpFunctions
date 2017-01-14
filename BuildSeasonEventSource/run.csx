#r "Newtonsoft.Json"
#r "Microsoft.WindowsAzure.Storage"
using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Linq;
using Newtonsoft.Json;

private static IAsyncCollector<Event> _seasonWriter;
private static IQueryable<Fixture> _fixtures;

public async static Task Run(string input,
    IQueryable<Round> rounds,
    IQueryable<Fixture> fixtures,
    TraceWriter log,
    IAsyncCollector<Event> seasonWriter)
{
    log.Info("Starting run");
    _seasonWriter = seasonWriter;
    _fixtures = fixtures;

    int year = 2008;
    var currentRounds = rounds.Where(round => round.Year == year).ToList().OrderBy(round => round.Id);

    do
    {
        log.Info($"{year} Found {currentRounds.Count()} rounds");

        int version = 0;

        await CreateSeason(year, version++);
        foreach (var round in currentRounds)
        {
            version = await AddRound(year, round, version);
        }

        year++;
        currentRounds = rounds.Where(round => round.Year == year).ToList().OrderBy(round => round.Id);
    }
    while (currentRounds.Any());
}

public static async Task<int> AddFixture(int year, Fixture fixture, int version)
{
    var addition = new FixtureAddedEvent
    {
        HomeClub = fixture.Home,
        AwayClub = fixture.Away
    };

    var addEvent = new Event(year,
        version++,
        "fixtureAdded",
        addition);

    await _seasonWriter.AddAsync(addEvent);

    return version;
}

private static async Task<int> AddRound(int year, Round round, int version)
{
    var addition = new RoundAddedEvent{
        Round = round.RoundNumber,
        NormalRound = round.NormalRound
    };
    var additionEvent = new Event(year, 
        version++, 
        "roundAdded",
        addition);

    await _seasonWriter.AddAsync(additionEvent);

    if (round.RoundComplete)
    {
        var completeEvent = new Event(year,
            version++,
            "roundCompleted",
            new object());

        await _seasonWriter.AddAsync(completeEvent);
    }

    foreach (var fixture in _fixtures.Where(fixture =>fixture.PartitionKey == $"{round.Id}"))
    {
        version = await AddFixture(year, fixture, version);
    }

    return version;
}

private static async Task CreateSeason(int year, int version)
{
    var creation = new SeasonCreatedEvent
    {
        Id = year
    };

    var creationEvent = new Event(year,
        version,
        "seasonCreated",
        creation
        );

    await _seasonWriter.AddAsync(creationEvent);
}

public class Event : TableEntity
{
    public Event(int entityId,
        int entityVersion,
        string eventType,
        object payload)
    {
        RowKey = entityVersion.ToString("0000000000");
        PartitionKey = entityId.ToString();
        EventType = eventType;
        SetPayload(payload);
    }

    public string EventType{get;set;}
    public string Payload{get;set;}
    public T GetPayload<T>()
    {
        return (T)JsonConvert.DeserializeObject<T>(Payload);
    }

    public void SetPayload(object payload)
    {
        Payload = JsonConvert.SerializeObject(payload);
    }
}

public class FixtureAddedEvent
{
    public Guid HomeClub{get;set;}
    public Guid AwayClub{get;set;}
}

public class RoundAddedEvent
{
    public int Round{get;set;}
    public bool NormalRound{get;set;}
}

public class SeasonCreatedEvent
{
    public int Id{get;set;}
}

public class Round : TableEntity
{
    private int _year;
    private int _roundNumber;

    public int Id { get; set; }

    public int Year
    {
        get { return _year; }
        set
        {
            PartitionKey = value.ToString();
            _year = value;
        }
    }

    public int RoundNumber
    {
        get { return _roundNumber; }
        set
        {
            RowKey = value.ToString();
            _roundNumber = value;
        }
    }

    public bool RoundComplete { get; set; }
    public bool NormalRound { get; set; }
}

public class Stat : TableEntity
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

public class Fixture : TableEntity
{
    private int _roundId;
    private Guid _home;
    private Guid _away;

    public int RoundId
    {
        get { return _roundId; }
        set
        {
            PartitionKey = value.ToString();
            _roundId = value;
        }
    }

    public Guid Home
    {
        get { return _home; }
        set
        {
            SetRowKey();
            _home = value;
        }
    }

    public Guid Away
    {
        get { return _away; }
        set
        {
            SetRowKey();
            _away = value;
        }
    }

    private void SetRowKey()
    {
        RowKey = $"{Home}-{Away}";
    }
}

public class PlayedTeam : TableEntity
{
    private Guid _clubId;
    private int _round;
    public Guid Id { get; set; }

    public Guid ClubId
    {
        get { return _clubId; }
        set
        {
            RowKey = value.ToString();
            _clubId = value;
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

    public string TeamJson
    {
        get { return JsonConvert.SerializeObject(Team); }
        set { Team = (IEnumerable<PlayedTeam.TeamPlayer>)JsonConvert.DeserializeObject<IEnumerable<PlayedTeam.TeamPlayer>>(value); }
    }

    public int Score { get; set; }

    [IgnoreProperty]
    public IEnumerable<PlayedTeam.TeamPlayer> Team { get; set; }

    public class TeamPlayer
    {
        public Guid PlayerId { get; set; }
        public char PickedPosition { get; set; }
        public char PlayedPosition { get; set; }
        public Stat Stat { get; set; }
        public int Score { get; set; }
    }
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