using System;

namespace ClubReadUpdater.Lib
{
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
}