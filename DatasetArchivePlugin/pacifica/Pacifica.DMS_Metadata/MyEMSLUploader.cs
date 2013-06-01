using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Pacifica.Core;

namespace Pacifica.DMS_Metadata
{
	public class MyEMSLUploader
	{
		public const string RECURSIVE_UPLOAD = "MyEMSL_Recurse";

		System.ComponentModel.BackgroundWorker backgrounder;
		System.ComponentModel.BackgroundWorker statusBackgrounder;
		DMSMetadataObject _mdContainer;
		IUpload myEMSLUpload;

		public MyEMSLUploader() {
			StatusURI = string.Empty;
			DirectoryLookupPath = string.Empty;
			FileCountNew = 0;
			FileCountUpdated = 0;
			Bytes = 0;
			ErrorCode = string.Empty;
			backgrounder = new System.ComponentModel.BackgroundWorker();
			statusBackgrounder = new System.ComponentModel.BackgroundWorker();
			this.myEMSLUpload = new Upload(ref backgrounder);

			// Attach the events			
			this.myEMSLUpload.DebugEvent += new DebugEventHandler(myEMSLUpload_DebugEvent);
			this.myEMSLUpload.ErrorEvent += new DebugEventHandler(myEMSLUpload_ErrorEvent);
			this.myEMSLUpload.StatusUpdate += new StatusUpdateEventHandler(myEMSLUpload_StatusUpdate);
			this.myEMSLUpload.TaskCompleted += new TaskCompletedEventHandler(myEMSLUpload_TaskCompleted);
			this.myEMSLUpload.DataReceivedAndVerified += new DataVerifiedHandler(myEMSLUpload_DataReceivedAndVerified);
		}

		#region "Properties"


		public string StatusURI {
			get;
			private set;
		}

		public string DirectoryLookupPath {
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

		#region "Event Handlers"

		public event DebugEventHandler DebugEvent;

		void myEMSLUpload_DebugEvent(string callingFunction, string currentTask) {
			if (DebugEvent != null)
			{
				DebugEvent(callingFunction, currentTask);
			}
		}

		public event DebugEventHandler ErrorEvent;

		void myEMSLUpload_ErrorEvent(string callingFunction, string errorMessage)
		{
			if (ErrorEvent != null)
			{
				ErrorEvent(callingFunction, errorMessage);
			}
		}

		public event StatusUpdateEventHandler StatusUpdate;

		void myEMSLUpload_StatusUpdate(string bundleIdentifier, int percentCompleted, long totalBytesSent, long totalBytesToSend, string averageUploadSpeed) {
			if (StatusUpdate != null)
			{
				StatusUpdate(bundleIdentifier, percentCompleted, totalBytesSent, totalBytesToSend, averageUploadSpeed);
			}
		}

		public event TaskCompletedEventHandler TaskCompleted;

		void myEMSLUpload_TaskCompleted(string bundleIdentifier, string serverResponse) {

			if(this._mdContainer.newFilesObject.Count > 0) {
				this.StatusURI = serverResponse + "/xml";
				this.DirectoryLookupPath = this._mdContainer.serverSearchString;
				myEMSLUpload.BeginUploadMonitoring(serverResponse, this._mdContainer.serverSearchString, this._mdContainer.newFilesObject);
			}

			if (TaskCompleted != null)
			{
				TaskCompleted(bundleIdentifier, serverResponse);
			}
		}

		public event DataVerifiedHandler DataReceivedAndVerified;

		void myEMSLUpload_DataReceivedAndVerified(bool successfulVerification, string errorMessage)
		{
			if (successfulVerification)
			{
				//delete the cached zip/tar file
				string bundleName = this._mdContainer.bundleName;
				System.IO.FileInfo bundleObject = new System.IO.FileInfo(System.IO.Path.Combine(Pacifica.Core.Configuration.LocalTempDirectory, bundleName));
				if (bundleObject.Exists)
				{
					bundleObject.Delete();
				}
			}

			if (DataReceivedAndVerified != null)
			{
				DataReceivedAndVerified(successfulVerification, errorMessage);
			}
		}

		#endregion

		public void StartUpload(System.Collections.Generic.Dictionary<string, string> taskParamsDict, System.Collections.Generic.Dictionary<string, string> mgrParamsDict) {

			//generate the metadata object
			this._mdContainer = new DMSMetadataObject(taskParamsDict, mgrParamsDict, null);
			Pacifica.Core.Configuration.LocalTempDirectory = mgrParamsDict["workdir"];
			this.FileCountUpdated = this._mdContainer.totalFileCountUpdated;
			this.FileCountNew = this._mdContainer.totalFileCountNew;
			this.Bytes = this._mdContainer.totalFileSizeToUpload;
			Dictionary<string, object> md = this._mdContainer.metadataObject;

			Boolean isLoginRequired = Pacifica.Core.Configuration.AuthInstance.LoginRequired;
			this.myEMSLUpload.ProcessMetadata(md);		
		}

	}
}
