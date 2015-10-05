using System;

namespace DatasetArchivePlugin
{
	public class MyEMSLUploadEventArgs : EventArgs
	{
		public readonly int FileCountNew;
		public readonly int FileCountUpdated;
		public readonly Int64 BytesUploaded;
		public readonly double UploadTimeSeconds;
		public readonly string StatusURI;
		public readonly int ErrorCode;
	    public readonly bool UsedTestInstance;

		public MyEMSLUploadEventArgs(
            int iFileCountNew, 
            int iFileCountUpdated, 
            Int64 iBytes, 
            double dUploadTimeSeconds, 
            string sStatusURI, 
            int iErrorCode,
            bool usedTestInstance)
		{
			FileCountNew = iFileCountNew;
			FileCountUpdated = iFileCountUpdated;
			BytesUploaded = iBytes;
			UploadTimeSeconds = dUploadTimeSeconds;
			StatusURI = sStatusURI;
			ErrorCode = iErrorCode;
		    UsedTestInstance = usedTestInstance;
		}
	}

	public delegate void MyEMSLUploadEventHandler(object sender, MyEMSLUploadEventArgs e);

}
