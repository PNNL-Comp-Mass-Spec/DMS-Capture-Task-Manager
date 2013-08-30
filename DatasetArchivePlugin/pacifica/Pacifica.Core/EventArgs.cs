using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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

		public readonly string BundleIdentifier;

		/// <summary>
		/// Value between 0 and 100
		/// </summary>
		public readonly double PercentCompleted;

		public readonly long TotalBytesSent;
		public readonly long TotalBytesToSend;
		public readonly string StatusMessage;

		public StatusEventArgs(string bundleIdentifier, double percentCompleted, long totalBytesSent, long totalBytesToSend, string statusMessage)
		{
			BundleIdentifier = bundleIdentifier;
			PercentCompleted = percentCompleted;
			TotalBytesSent = totalBytesSent;
			TotalBytesToSend = totalBytesToSend;
			StatusMessage = statusMessage;
		}
	}

	public class UploadCompletedEventArgs : EventArgs
	{
		public readonly string BundleIdentifier;
		public readonly string ServerResponse;

		public UploadCompletedEventArgs(string bundleIdentifier, string serverResponse)
		{
			BundleIdentifier = bundleIdentifier;
			ServerResponse = serverResponse;
		}
	}

}