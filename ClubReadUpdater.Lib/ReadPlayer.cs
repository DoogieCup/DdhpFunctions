using System;

namespace ClubReadUpdater.Lib
{
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
}