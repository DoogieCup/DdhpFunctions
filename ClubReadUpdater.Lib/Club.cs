using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;

namespace ClubReadUpdater.Lib
{
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
                    var castEvent = GetPayload<ClubCreatedEvent>(e);
                    Id = Guid.Parse(e.PartitionKey);
                    Email = castEvent.Email;
                    CoachName = castEvent.CoachName;
                    ClubName = castEvent.ClubName;
                    return;
                case ClubEvent.ContractImported:
                    var contractImportedEvent = GetPayload<ContractImportedEvent>(e);
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

                var version = Int32.Parse(e.RowKey);
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
}