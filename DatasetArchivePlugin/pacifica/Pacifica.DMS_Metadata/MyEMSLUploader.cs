using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Pacifica.Core;

namespace Pacifica.DMS_Metadata
{
	public class MyEMSLUploader
	{
		System.ComponentModel.BackgroundWorker backgrounder;
		System.ComponentModel.BackgroundWorker statusBackgrounder;
		DMSMetadataObject _mdContainer;
		IUpload myEMSLUpload;

		public MyEMSLUploader() {
			backgrounder = new System.ComponentModel.BackgroundWorker();
			statusBackgrounder = new System.ComponentModel.BackgroundWorker();
			this.myEMSLUpload = new Upload(ref backgrounder);
			this.myEMSLUpload.TaskCompleted += new TaskCompletedEventHandler(myEMSLUpload_TaskCompleted);
		}

		void myEMSLUpload_TaskCompleted(string bundleIdentifier, string serverResponse) {

			myEMSLUpload.DataReceivedAndVerified += new DataVerifiedHandler(myEMSLUpload_DataReceivedAndVerified);
			if(this._mdContainer.newFilesObject.Count > 0) {
				myEMSLUpload.BeginUploadMonitoring(serverResponse, this._mdContainer.serverSearchString, this._mdContainer.newFilesObject);
			}
		}

		public void StartUpload(System.Collections.Generic.Dictionary<string, string> taskParamsDict, System.Collections.Generic.Dictionary<string, string> mgrParamsDict) {

			//generate the metadata object
			this._mdContainer = new DMSMetadataObject(taskParamsDict, mgrParamsDict, null);
			Dictionary<string, object> md = this._mdContainer.metadataObject;

			Boolean isLoginRequired = Pacifica.Core.Configuration.AuthInstance.LoginRequired;
			this.myEMSLUpload.ProcessMetadata(md);		
		}

		void myEMSLUpload_DataReceivedAndVerified(bool successfulVerification) {
			if(successfulVerification) {
				//delete the cached zip/tar file
				string bundleName = this._mdContainer.bundleName;
				System.IO.FileInfo bundleObject = new System.IO.FileInfo(System.IO.Path.Combine(System.IO.Path.GetTempPath(), bundleName));
				if(bundleObject.Exists) {
					bundleObject.Delete();
				}
			}
		}
	}
}
