#r "Newtonsoft.Json"
#r "Microsoft.WindowsAzure.Storage"
using System;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.Linq;
using Newtonsoft.Json;

private static IAsyncCollector<Event> _seasonWriter;
private static IQueryable<Fixture> _fixtures;
private static IQueryable<PickedTeam> _pickedTeams;
private static IQueryable<StorageStat> _stats;
private static IQueryable<AflClub> _aflClubs;
private static TraceWriter _logger;

public async static Task Run(string input,
    IQueryable<Round> rounds,
    IQueryable<Fixture> fixtures,
    IQueryable<PickedTeam> pickedTeams,
    IQueryable<StorageStat> stats,
    IQueryable<AflClub> aflClubs,
    TraceWriter log,
    IAsyncCollector<Event> seasonWriter)
{
    log.Info("Starting run");
    _logger = log;
    _seasonWriter = seasonWriter;
    _fixtures = fixtures;
    _pickedTeams = pickedTeams;
    _stats = stats;
    _aflClubs = aflClubs;

    int year = 2008;
    var currentRounds = rounds.Where(round => round.Year == year).ToList().OrderBy(round => round.Id);

    var aflClubIds = aflClubs.Select(q => q.Id);

    do
    {
        log.Info($"{year} Found {currentRounds.Count()} rounds");

        int version = 0;

        await CreateSeason(year, version++);
        foreach (var round in currentRounds)
        {
            version = await AddRound(year, round, version);

            version = await ImportStats(round, version);
        }

        year++;
        currentRounds = rounds.Where(round => round.Year == year).ToList().OrderBy(round => round.Id);
    }
    while (currentRounds.Any());
}

private static async Task<int> ImportStats(Round round, int version)
{
    _logger.Info($"Processing stats for {round.Id}");

    var stats = _stats.Where(q => q.PartitionKey == round.Id.ToString()).ToList().GroupBy(q => q.AflClubId);

    _logger.Info($"Found {stats.Count()} stats");

    foreach (var statGroup in stats)
    {
        var matchStats = new StatsSubmittedEvent
        {
            RoundNumber = round.RoundNumber,
            AflClubId = statGroup.Key,
            Stats = statGroup.Select(q => (Stat)q)
        };

        var statEvent = new Event(round.Year,
            version++,
            "statsImported",
            matchStats);

        await SaveEvent(statEvent);
    }

    return version;
}

private static async Task SaveEvent(Event e)
{
    try
    {
        await _seasonWriter.AddAsync(e);
    }
    catch (Exception ex)
    {
        _logger.Info($"Error while saving event {e.PartitionKey} {e.RowKey}\n{e.Payload}\n{ex.ToString()}");
        throw;
    }
}

public static async Task<int> AddFixture(Round round, Fixture fixture, int version)
{
    var addition = new FixtureAddedEvent
    {
        RoundNumber = GetRoundNumber(fixture.RoundId),
        HomeClub = fixture.Home,
        AwayClub = fixture.Away
    };

    var year = round.Year;

    var addEvent = new Event(year,
        version++,
        "fixtureAdded",
        addition);

    await SaveEvent(addEvent);

    PickedTeam[] pickedTeams = _pickedTeams.Where(q =>q.PartitionKey == fixture.RoundId.ToString()).ToList().Where(q => q.ClubId == fixture.Home || q.ClubId == fixture.Away).ToArray();

    if (pickedTeams.Length != 2)
    {
        _logger.Info($"Got an unexpected number of teams in fixture {fixture.PartitionKey} {fixture.RowKey}: {pickedTeams.Length}");

        // We've got some bad data for a couple of rounds. It's ok to bail on these during import.
        if (round.Year == 2010 && (new[]{19, 22}).Contains(round.RoundNumber))
        {
            return version;
        }
    }

    version = await AddTeam(round, version, pickedTeams[0]);
    version = await AddTeam(round, version, pickedTeams[1]);

    return version;
}

private static async Task<int> AddTeam(Round round, int version, PickedTeam team)
{
    var teamEvent = new PickedTeamSubmittedEvent
    {
        RoundNumber = round.RoundNumber,
        ClubId = team.ClubId,
        Team = team
    };

    var pickedEvent = new Event(round.Year,
        version++,
        "teamSubmitted",
        teamEvent);

    await SaveEvent(pickedEvent);

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

    await SaveEvent(additionEvent);

    if (round.RoundComplete)
    {
        var completeEvent = new Event(year,
            version++,
            "roundCompleted",
            new RoundCompletedEvent{RoundNumber = round.RoundNumber});

        await SaveEvent(completeEvent);
    }

    foreach (var fixture in _fixtures.Where(fixture =>fixture.PartitionKey == $"{round.Id}"))
    {
        version = await AddFixture(round, fixture, version);
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

    await SaveEvent(creationEvent);
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

private static int GetRoundNumber(int roundId)
{
    if (roundId.ToString().Length != 6)
    {
        _logger.Info($"Unexpected length of roundId {roundId}");
    }
        
    return int.Parse(roundId.ToString().Substring(4, 2));
}

public class StatsSubmittedEvent
{
    public int RoundNumber{get;set;}
    public Guid AflClubId{get;set;}
    public IEnumerable<Stat> Stats{get;set;}
}

public class PickedTeamSubmittedEvent
{
    public int RoundNumber{get;set;}
    public Guid ClubId{get;set;}
    public PickedTeam Team{get;set;}
}

public class RoundCompletedEvent
{
    public int RoundNumber{get;set;}
}

public class FixtureAddedEvent
{
    public int RoundNumber{get;set;}
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
            _home = value;
            SetRowKey();
        }
    }

    public Guid Away
    {
        get { return _away; }
        set
        {
            _away = value;
            SetRowKey();
        }
    }

    private void SetRowKey()
    {
        RowKey = $"{Home}-{Away}";
    }
}

public class PickedTeam : TableEntity
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
        set { Team = (IEnumerable<TeamPlayer>) JsonConvert.DeserializeObject<IEnumerable<TeamPlayer>>(value); }
    }

    [IgnoreProperty]
    public IEnumerable<TeamPlayer> Team { get; set; }

    public class TeamPlayer
    {
        public Guid PlayerId { get; set; }
        public char PickedPosition { get; set; }
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

public class AflClub : TableEntity
{
    private Guid _id;

    public AflClub()
    {
        PartitionKey = "AFL_CLUB";
    }

    public Guid Id
    {
        get { return _id; }
        set
        {
            RowKey = value.ToString();
            _id = value;
        }
    }

    public string Name { get; set; }
    public string ShortName { get; set; }
}

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

public struct Stat
{
    public Guid PlayerId{get;set;}
    public int Goals { get; set; }
    public int Behinds { get; set; }
    public int Disposals { get; set; }
    public int Marks { get; set; }
    public int Hitouts { get; set; }
    public int Tackles { get; set; }
    public int Kicks { get; set; }
    public int Handballs { get; set; }
    public int GoalAssists { get; set; }
    public int Inside50s { get; set; }
    public int FreesFor { get; set; }
    public int FreesAgainst { get; set; }
    public Guid AflClubId { get; set; }

    public static implicit operator Stat(string stat)
    {
        return JsonConvert.DeserializeObject<Stat>(stat);
    }

    public static implicit operator Stat(StorageStat stat)
    {
        var json = JsonConvert.SerializeObject(stat);
        return JsonConvert.DeserializeObject<Stat>(json);
    }

    public override string ToString()
    {
        return JsonConvert.SerializeObject(this);
    }
}