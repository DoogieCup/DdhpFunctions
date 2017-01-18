using System;

namespace ClubReadUpdater.Lib
{
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
}