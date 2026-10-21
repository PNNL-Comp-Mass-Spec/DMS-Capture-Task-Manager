namespace DatasetIntegrityPlugin
{
    internal class AcquisitionMessageInfo
    {
        public double ElutionTimeMin { get; set; } = 0;

        public double ElutionTimeMax { get; set; } = 0;

        public string MessageType { get; set; } = string.Empty;

        public int ObservationCount { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="elutionTime">First elution time associated with this acquisition message</param>
        public AcquisitionMessageInfo(string messageType, double elutionTime)
        {
            MessageType = messageType;
            ElutionTimeMin = elutionTime;
            ElutionTimeMax = elutionTime;
            ObservationCount = 1;
        }
    }
}
