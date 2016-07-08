
namespace ArchiveStatusCheckPlugin
{
    public class clsIngestStatusInfo
    {
        public int StatusNum { get; private set; }
        public string StatusURI { get; set; }
        public string Subfolder { get; set; }

        public byte IngestStepsCompletedOld { get; set; }
        public byte IngestStepsCompletedNew { get; set; }

        public int EUS_InstrumentID;
        public string EUS_ProposalID;
        public int EUS_UploaderID;

        /// <summary>
        /// Constructor
        /// </summary>
        public clsIngestStatusInfo(int statusNum) : this(statusNum, string.Empty)
        {
        }

        public clsIngestStatusInfo(int statusNum, string statusURI)
        {
            StatusNum = statusNum;
            StatusURI = statusURI;
            Subfolder = string.Empty;
            IngestStepsCompletedOld = 0;
            IngestStepsCompletedNew = 0;

            EUS_InstrumentID = 0;
            EUS_ProposalID = string.Empty;
            EUS_UploaderID = 0;

        }
    }
}
