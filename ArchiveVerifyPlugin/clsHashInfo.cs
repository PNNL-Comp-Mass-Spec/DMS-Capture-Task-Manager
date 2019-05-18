
namespace ArchiveVerifyPlugin
{
    public class clsHashInfo
    {
        private string mHashCode;
        private string mMyEMSLFileID;

        /// <summary>
        /// MD5 or SHA-1 Hash
        /// </summary>
        public string HashCode
        {
            get => mHashCode;
            set => mHashCode = value ?? string.Empty;
        }

        public string MyEMSLFileID
        {
            get => mMyEMSLFileID;
            set => mMyEMSLFileID = value ?? string.Empty;
        }

        /// <summary>
        /// Constructor
        /// </summary>
        public clsHashInfo() :
            this(string.Empty, string.Empty)
        { }

        public clsHashInfo(string hashCode, string myEmslFileID)
        {
            Clear();
            HashCode = hashCode;
            MyEMSLFileID = myEmslFileID;
        }

        public void Clear()
        {
            HashCode = string.Empty;
            MyEMSLFileID = string.Empty;
        }

        public bool IsMatch(clsHashInfo comparisonValue)
        {
            return string.Equals(HashCode, comparisonValue.HashCode) &&
                   string.Equals(MyEMSLFileID, comparisonValue.MyEMSLFileID);
        }

        public override string ToString()
        {
            string description;
            if (string.IsNullOrEmpty(HashCode))
                description = "#No Hash#";
            else
                description = HashCode;

            if (!string.IsNullOrEmpty(MyEMSLFileID))
                description += ", ID=" + MyEMSLFileID;

            return description;
        }

    }
}
