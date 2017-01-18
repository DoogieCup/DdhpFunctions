using Newtonsoft.Json;

namespace ClubReadUpdater.Lib
{
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