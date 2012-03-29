using System;
using System.Collections.Generic;

namespace DatasetArchivePlugin
{
	public class MyEMSLUploadEventArgs : EventArgs
	{
		public readonly int fileCountNew;
		public readonly int fileCountUpdated;
		public readonly Int64 bytes;
		public readonly double uploadTimeSeconds;
		public readonly string statusURI;
		public readonly string contentURI;
		public readonly Int16 errorCode;

		public MyEMSLUploadEventArgs(int iFileCountNew, int iFileCountUpdated, Int64 iBytes, double dUploadTimeSeconds, string sStatusURI, string sContentURI, Int16 iErrorCode)
		{
			fileCountNew = iFileCountNew;
			fileCountUpdated = iFileCountUpdated;
			bytes = iBytes;
			uploadTimeSeconds = dUploadTimeSeconds;
			statusURI = sStatusURI;
			contentURI = sContentURI;
			errorCode = iErrorCode;
		}
	}

	public delegate void MyEMSLUploadEventHandler(object sender, MyEMSLUploadEventArgs e);

}
