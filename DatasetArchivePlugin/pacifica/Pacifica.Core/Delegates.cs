namespace Pacifica.Core
{
    public delegate void StatusUpdateEventHandler(string bundleIdentifier,
        int percentCompleted, long totalBytesSent,
        long totalBytesToSend, string averageUploadSpeed);

    public delegate void TaskCompletedEventHandler(string bundleIdentifier, string serverResponse);
		public delegate void DataVerifiedHandler(bool successfulVerification);
}