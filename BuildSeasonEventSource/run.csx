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
            round = round.RoundNumber,
            aflClubId = statGroup.Key,
            stats = statGroup.Select(q => (Stat)q)
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
        round = GetRoundNumber(fixture.RoundId),
        homeClubId = fixture.Home,
        awayClubId = fixture.Away
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
    var teamEvent = new TeamSubmittedEvent
    {
        round = round.RoundNumber,
        clubId = team.ClubId,
        team = team
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
        round = round.RoundNumber,
        normalRound = round.NormalRound
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
            new RoundCompletedEvent{round = round.RoundNumber});

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
        year = year
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
    public int round{get;set;}
    public Guid aflClubId{get;set;}
    public IEnumerable<Stat> stats{get;set;}
}

public class TeamSubmittedEvent
{
    public int round{get;set;}
    public Guid clubId{get;set;}
    public PickedTeam team{get;set;}
}

public class RoundCompletedEvent
{
    public int round{get;set;}
}

public class FixtureAddedEvent
{
    public int round{get;set;}
    public Guid homeClubId{get;set;}
    public Guid awayClubId{get;set;}
}

public class RoundAddedEvent
{
    public int round{get;set;}
    public bool normalRound{get;set;}
}

public class SeasonCreatedEvent
{
    public int year{get;set;}
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
    [JsonIgnore]
    public Guid Id { get; set; }

    [JsonIgnore]
    public Guid ClubId
    {
        get { return _clubId; }
        set
        {
            RowKey = value.ToString();
            _clubId = value;
        }
    }

    [JsonIgnore]
    public int Round
    {
        get { return _round; }
        set
        {
            PartitionKey = value.ToString();
            _round = value;
        }
    }

    [JsonIgnore]
    public string TeamJson
    {
        get { return JsonConvert.SerializeObject(PickedPositions.Select(q => (StorageTeamPlayer)q)); }
        set { PickedPositions =((IEnumerable<StorageTeamPlayer>) JsonConvert.DeserializeObject<IEnumerable<StorageTeamPlayer>>(value)).Select(q => (TeamPlayer)q); }
    }

    [JsonProperty("pickedPositions")]
    public IEnumerable<TeamPlayer> PickedPositions { get; set; }

    public class StorageTeamPlayer
    {
        public Guid PlayerId { get; set; }
        public char PickedPosition { get; set; }

        public static implicit operator StorageTeamPlayer(TeamPlayer player)
        {
            return new StorageTeamPlayer
            {
                PlayerId = player.PlayerId,
                PickedPosition = player.PickedPosition
            };
        }
    }

    public class TeamPlayer
    {
        [JsonProperty("playerId")]
        public Guid PlayerId { get; set; }
        [JsonProperty("position")]    
        public char PickedPosition { get; set; }

        public static implicit operator TeamPlayer(StorageTeamPlayer player)
        {
            return new TeamPlayer
            {
                PlayerId = player.PlayerId,
                PickedPosition = player.PickedPosition
            };
        }
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

    [JsonProperty("goals")]
    public int Goals { get; set; }
    [JsonProperty("behinds")]
    public int Behinds { get; set; }
    [JsonProperty("disposals")]
    public int Disposals { get; set; }
    [JsonProperty("marks")]
    public int Marks { get; set; }
    [JsonProperty("hitouts")]
    public int Hitouts { get; set; }
    [JsonProperty("tackles")]
    public int Tackles { get; set; }
    [JsonProperty("kicks")]
    public int Kicks { get; set; }
    [JsonProperty("handballs")]
    public int Handballs { get; set; }
    [JsonProperty("goalAssists")]
    public int GoalAssists { get; set; }
    [JsonProperty("inside50s")]
    public int Inside50s { get; set; }
    [JsonProperty("freesFor")]
    public int FreesFor { get; set; }
    [JsonProperty("freesAgainst")]
    public int FreesAgainst { get; set; }
    [JsonProperty("aflClubId")]
    public Guid AflClubId { get; set; }
}

public struct Stat
{
    public Guid playerId{get;set;}
    public int goals { get; set; }
    public int behinds { get; set; }
    public int disposals { get; set; }
    public int marks { get; set; }
    public int hitouts { get; set; }
    public int tackles { get; set; }
    public int kicks { get; set; }
    public int handballs { get; set; }
    public int goalAssists { get; set; }
    public int inside50s { get; set; }
    public int freesFor { get; set; }
    public int freesAgainst { get; set; }
    public Guid aflClubId { get; set; }

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