using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Pacifica.Core;

namespace Pacifica.DMS_Metadata
{
	public class MyEMSLUploader
	{
		public const string RECURSIVE_UPLOAD = "MyEMSL_Recurse";
		
		/* August 2013: To be deleted
		 *
		System.ComponentModel.BackgroundWorker backgrounder;
		System.ComponentModel.BackgroundWorker statusBackgrounder;
		 */

		DMSMetadataObject _mdContainer;
		Upload myEMSLUpload;

		public MyEMSLUploader() {
			StatusURI = string.Empty;
			FileCountNew = 0;
			FileCountUpdated = 0;
			Bytes = 0;
			ErrorCode = string.Empty;

			/* August 2013: To be deleted
			 *
			backgrounder = new System.ComponentModel.BackgroundWorker();
			statusBackgrounder = new System.ComponentModel.BackgroundWorker();
			 */

			this.myEMSLUpload = new Upload();

			// Attach the events			
			this.myEMSLUpload.DebugEvent +=new MessageEventHandler(myEMSLUpload_DebugEvent);
			this.myEMSLUpload.ErrorEvent +=new MessageEventHandler(myEMSLUpload_ErrorEvent);
			this.myEMSLUpload.StatusUpdate +=new StatusUpdateEventHandler(myEMSLUpload_StatusUpdate);
			this.myEMSLUpload.UploadCompleted +=new UploadCompletedEventHandler(myEMSLUpload_UploadCompleted);

		}

		#region "Properties"


		public string StatusURI {
			get;
			private set;
		}


		public int FileCountNew {
			get;
			private set;
		}

		public int FileCountUpdated {
			get;
			private set;
		}

		public long Bytes {
			get;
			private set;
		}

		public string ErrorCode {
			get;
			private set;
		}

		#endregion

		public void StartUpload(Dictionary<string, string> taskParams, Dictionary<string, string> mgrParams, out string statusURL)
		{

			statusURL = string.Empty;

			// Instantiate the metadata object
			this._mdContainer = new DMSMetadataObject();
			this._mdContainer.ProgressEvent += new DMSMetadataObject.ProgressEventHandler(_mdContainer_ProgressEvent);

			// Look for files to upload, compute a Sha-1 hash for each, and compare those hashes to existing files in MyEMSL
			this._mdContainer.SetupMetadata(taskParams, mgrParams);

			Pacifica.Core.Configuration.LocalTempDirectory = mgrParams["workdir"];
			this.FileCountUpdated = this._mdContainer.TotalFileCountUpdated;
			this.FileCountNew = this._mdContainer.TotalFileCountNew;
			this.Bytes = this._mdContainer.TotalFileSizeToUpload;

			/*
			 * August 2013: To be deleted
			 * 
			 * This call is updating the locally cached cookie container
			 * Instead, obtain the cookies using an Auth call
			 *
			Boolean isLoginRequired = Pacifica.Core.Configuration.AuthInstance.LoginRequired;
			 */

			this.myEMSLUpload.StartUpload(this._mdContainer.MetadataObject, out statusURL);

			if (!string.IsNullOrEmpty(statusURL))
				this.StatusURI = statusURL + "/xml";

			/* August 2013: To be deleted 
			 * No longer needed since we're creating the tar file on-the-fly
			 */
			// Delete the cached tar file
			string bundleName = this._mdContainer.BundleName;
			FileInfo bundleObject = new FileInfo(Path.Combine(Pacifica.Core.Configuration.LocalTempDirectory, bundleName));
			if (bundleObject.Exists)
			{
				bundleObject.Delete();
			}

		}

		#region "Events and Event Handlers"

		public event DebugEventHandler DebugEvent;
		public event DebugEventHandler ErrorEvent;

		public event StatusUpdateEventHandler StatusUpdate;
		public event UploadCompletedEventHandler UploadCompleted;
		
		void myEMSLUpload_DebugEvent(object sender, MessageEventArgs e)
		{
			if (DebugEvent != null)
				DebugEvent(this, e);
		}

		void myEMSLUpload_ErrorEvent(object sender, MessageEventArgs e)
		{
			if (ErrorEvent != null)
				ErrorEvent(this, e);
		}

		void myEMSLUpload_StatusUpdate(object sender, StatusEventArgs e)
		{
			if (StatusUpdate != null)
			{
				// Multiplying by 0.25 because we're assuming 25% of the time is required for _mdContainer to compute the Sha-1 hashes of files to be uploaded while 75% of the time is required to create and upload the .tar file				
				double percentCompleteOverall = 25 + e.PercentCompleted * 0.75;
				StatusUpdate(this, new StatusEventArgs(e.BundleIdentifier, percentCompleteOverall, e.TotalBytesSent, e.TotalBytesToSend, e.StatusMessage));
			}
		}

		void myEMSLUpload_UploadCompleted(object sender, UploadCompletedEventArgs e)
		{
			if (UploadCompleted != null)
			{
				UploadCompleted(this, e);
			}
		}
					
		void _mdContainer_ProgressEvent(object sender, ProgressEventArgs e)
		{
			if (StatusUpdate != null)
			{
				// Multiplying by 0.25 because we're assuming 25% of the time is required for _mdContainer to compute the Sha-1 hashes of files to be uploaded while 75% of the time is required to create and upload the .tar file
				double percentCompleteOverall = 0 + e.PercentComplete * 0.25;
				StatusUpdate(this, new StatusEventArgs("", percentCompleteOverall, 0, _mdContainer.TotalFileSizeToUpload, ""));
			}
			
		}

		#endregion


	}
}
