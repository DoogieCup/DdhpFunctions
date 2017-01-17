using System;
using System.Collections.Generic;

namespace ClubReadUpdater.Lib
{
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
}