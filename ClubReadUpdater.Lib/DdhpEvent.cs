using Microsoft.WindowsAzure.Storage.Table;

namespace ClubReadUpdater.Lib
{
    public class DdhpEvent : TableEntity
    {
        public string EventType { get; set; }
        public string Payload { get; set; }
    }
}