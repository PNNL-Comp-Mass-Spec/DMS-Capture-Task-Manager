using System;

namespace DatasetArchivePlugin
{
	public class MyEMSLUploadEventArgs : EventArgs
	{
		public readonly int fileCountNew;
		public readonly int fileCountUpdated;
		public readonly Int64 bytes;
		public readonly double uploadTimeSeconds;
		public readonly string statusURI;
		public readonly int errorCode;

		public MyEMSLUploadEventArgs(int iFileCountNew, int iFileCountUpdated, Int64 iBytes, double dUploadTimeSeconds, string sStatusURI, int iErrorCode)
		{
			fileCountNew = iFileCountNew;
			fileCountUpdated = iFileCountUpdated;
			bytes = iBytes;
			uploadTimeSeconds = dUploadTimeSeconds;
			statusURI = sStatusURI;
			errorCode = iErrorCode;
		}
	}

	public delegate void MyEMSLUploadEventHandler(object sender, MyEMSLUploadEventArgs e);

}
