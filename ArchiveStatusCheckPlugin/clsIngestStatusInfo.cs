
namespace ArchiveStatusCheckPlugin
{
    public class clsIngestStatusInfo
    {
        public int StatusNum { get; private set; }
        public string StatusURI { get; set; }
        public string Subfolder { get; set; }

        public int TransactionId { get; set; }

        public byte IngestStepsCompletedOld { get; set; }
        public byte IngestStepsCompletedNew { get; set; }

        public int EUS_InstrumentID { get; set; }
        public string EUS_ProposalID { get; set; }
        public int EUS_UploaderID { get; set; }
        public int ExistingErrorCode { get; set; }

        /// <summary>
        /// Constructor that takes statusNum
        /// </summary>
        public clsIngestStatusInfo(int statusNum) : this(statusNum, string.Empty)
        {
        }

        /// <summary>
        /// Constructor that takes statusNum and statusURI
        /// </summary>
        public clsIngestStatusInfo(int statusNum, string statusURI)
        {
            StatusNum = statusNum;
            StatusURI = statusURI;
            Subfolder = string.Empty;
            TransactionId = 0;

            IngestStepsCompletedOld = 0;
            IngestStepsCompletedNew = 0;

            EUS_InstrumentID = 0;
            EUS_ProposalID = string.Empty;
            EUS_UploaderID = 0;

        }
    }
}
