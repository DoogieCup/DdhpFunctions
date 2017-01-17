using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace ClubReadUpdater.Lib
{
    public class Runner
    {
        private static Action<string> _log;

        public Runner(Action<string> log)
        {
            _log = log;
        }

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
                    club.Contracts.Where(q => q.FromRound <= int.Parse($"{year}24") && q.ToRound >= int.Parse($"{year}01"))
                        .ToList();

                foreach (var contract in Contracts)
                {
                    if (!stats.ContainsKey(contract.PlayerId))
                    {
                        continue;
                    }

                    try
                    {
                        contract.Stats = stats[contract.PlayerId].Select(stat => (ReadStat)stat);
                    }
                    catch (Exception ex)
                    {
                        _log($"Error casting StorageStat to ReadStat:\n{ex.ToString()}");
                        throw;
                    }
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
                    return int.Parse(PartitionKey);
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

        public class Club : ComplexEntity
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

            private Club()
            {
                Version = -1;
                PartitionKey = "ALL_CLUBS";
            }

            public IEnumerable<Contract> Contracts
            {
                get { return _contracts; }
                set { _contracts = value.ToList(); }
            }

            private List<Contract> _contracts = new List<Contract>();

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
                        _log($"Processing club created event {e.Payload}");
                        var castEvent = GetPayload<ClubCreatedEvent>(e);
                        Id = Guid.Parse(e.PartitionKey);
                        Email = castEvent.Email;
                        CoachName = castEvent.CoachName;
                        ClubName = castEvent.ClubName;
                        return;
                    case ClubEvent.ContractImported:
                        var contractImportedEvent = GetPayload<ContractImportedEvent>(e);
                        _log($"Processing ContractImported event {e.Payload}");
                        _contracts.Add(new Contract(contractImportedEvent.PlayerId,
                            contractImportedEvent.FromRound,
                            contractImportedEvent.ToRound,
                            contractImportedEvent.DraftPick));
                        return;
                }
            }

            public Club LoadFromEvents(IEnumerable<DdhpEvent> events)
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
            public ReadPlayer Player { get; set; }
            public IEnumerable<ReadStat> Stats { get; set; }
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

        public class ReadPlayer
        {
            public Guid Id { get; set; }

            public string Name { get; set; }

            public Guid CurrentAflClubId { get; set; }

            public bool Active { get; set; }
            public string FootywireName { get; set; }

            public static implicit operator ReadPlayer(Player player)
            {
                return new ReadPlayer
                {
                    Id = player.Id,
                    Name = player.Name,
                    CurrentAflClubId = player.CurrentAflClubId,
                    Active = player.Active,
                    FootywireName = player.FootywireName
                };
            }
        }

        public abstract class ComplexEntity : TableEntity
        {
            public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
            {
                base.ReadEntity(properties, operationContext);

                foreach (var property in SerializedProperties)
                {
                    var value = properties[property.Name].StringValue;
                    Type propertyType = property.PropertyType;
                    property.SetValue(this, JsonConvert.DeserializeObject(value, propertyType));
                }
            }

            public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
            {
                var dictionary = base.WriteEntity(operationContext);

                foreach (var propertyInfo in SerializedProperties)
                {
                    dictionary.Add(propertyInfo.Name, new EntityProperty(JsonConvert.SerializeObject(propertyInfo.GetValue(this))));
                }

                return dictionary;
            }

            private IEnumerable<PropertyInfo> SerializedProperties
            {
                get
                {
                    foreach (var propertyInfo in GetType().GetProperties())
                    {
                        var serializeAttribute = propertyInfo.GetCustomAttribute<SerializeAttribute>();
                        if (serializeAttribute != null)
                        {
                            yield return propertyInfo;
                        }
                    }
                }
            }
        }

        public class SerializeAttribute : Attribute
        {

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

        public struct ReadStat
        {
            [JsonProperty("rn")]
            public int RoundNumber { get; set; }
            [JsonProperty("f")]
            public int Forward { get; set; }
            [JsonProperty("m")]
            public int Midfield { get; set; }
            [JsonProperty("r")]
            public int Ruck { get; set; }
            [JsonProperty("t")]
            public int Tackle { get; set; }

            public static implicit operator ReadStat(StorageStat stat)
            {
                var f = stat.Goals * 6 + stat.Behinds;
                var m = stat.Disposals;
                var r = stat.Hitouts + stat.Marks;
                var t = stat.Tackles * 6;

                return new ReadStat
                {
                    Forward = f,
                    Midfield = m,
                    Ruck = r,
                    Tackle = t
                };
            }
        }
    }
}
