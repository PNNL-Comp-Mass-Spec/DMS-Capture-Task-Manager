using System;

namespace Pacifica.Core
{
    public class MessageEventArgs : EventArgs
    {
        public readonly string CallingFunction;
        public readonly string Message;

        public MessageEventArgs(string callingFunction, string message)
        {
            CallingFunction = callingFunction;
            Message = message;
        }
    }

    public class ProgressEventArgs : EventArgs
    {
        /// <summary>
        /// Value between 0 and 100
        /// </summary>
        public readonly double PercentComplete;
        public readonly string CurrentTask;

        public ProgressEventArgs(double percentComplete, string currentTask)
        {
            PercentComplete = percentComplete;
            CurrentTask = currentTask;
        }
    }

    public class StatusEventArgs : EventArgs
    {

        /// <summary>
        /// Value between 0 and 100
        /// </summary>
        public readonly double PercentCompleted;

        public readonly long TotalBytesSent;

        public readonly long TotalBytesToSend;

        public readonly string StatusMessage;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="percentCompleted">Percent complete, value between 0 and 100</param>
        /// <param name="totalBytesSent">Total bytes sent</param>
        /// <param name="totalBytesToSend">Total bytes to send</param>
        /// <param name="statusMessage">Status message</param>
        public StatusEventArgs(double percentCompleted, long totalBytesSent, long totalBytesToSend, string statusMessage)
        {
            PercentCompleted = percentCompleted;
            TotalBytesSent = totalBytesSent;
            TotalBytesToSend = totalBytesToSend;
            StatusMessage = statusMessage;
        }
    }

    public class UploadCompletedEventArgs : EventArgs
    {
        public readonly string ServerResponse;

        public UploadCompletedEventArgs(string serverResponse)
        {
            ServerResponse = serverResponse;
        }
    }

}