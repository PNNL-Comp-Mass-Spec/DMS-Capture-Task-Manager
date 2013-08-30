using System;
using CaptureTaskManager;
using Pacifica.Core;
using System.Collections.Generic;
using System.Net;

namespace ArchiveVerifyPlugin
{
	public class clsPluginMain : clsToolRunnerBase
	{
		//*********************************************************************************************************
		// Main class for plugin
		//**********************************************************************************************************

		#region "Constants and Enums"
	
		#endregion

		#region "Class-wide variables"
		clsToolReturnData mRetData = new clsToolReturnData();

		protected double mPercentComplete;
		protected DateTime mLastProgressUpdateTime = DateTime.UtcNow;

		#endregion

		#region "Constructors"
		public clsPluginMain()
			: base()
		{

		}

		#endregion

		#region "Methods"
		/// <summary>
		/// Runs the Archive Verify step tool
		/// </summary>
		/// <returns>Class with completionCode, completionMessage, evaluationCode, and evaluationMessage</returns>
		public override clsToolReturnData RunTool()
		{
			string msg;
			
			msg = "Starting ArchiveVerifyPlugin.clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			// Perform base class operations, if any
			mRetData = base.RunTool();
			if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
				return mRetData;
		
			if (m_DebugLevel >= 5)
			{
				msg = "Verifying files in MyEMSL for dataset '" + m_Dataset + "'";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
			}

			// Set this to Success for now
			mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;

			// Examine the MyEMSL ingest status page
			bool success = CheckUploadStatus();

			if (success)
				success = VisibleInElasticSearch();
			
			if (success)
			{
				// Everything was good
				if (m_DebugLevel >= 4)
				{
					msg = "MyEMSL verification successful for dataset " + m_Dataset;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				}
				mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
			}
			else
			{
				// There was a problem	
				if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
					mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
			}
			
			msg = "Completed clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			return mRetData;

		}	// End sub

		protected bool CheckUploadStatus()
		{

			// Monitor the upload status for 60 seconds
			// If still not complete after 60 seconds, then this manager will return completionCode CLOSEOUT_NOT_READY=2 
			// which will tell the DMS_Capture DB to reset the task to state 2 and bump up the Next_Try value by 5 minutes
			const int MAX_WAIT_TIME_SECONDS = 60;

			DateTime dtStartTime = DateTime.UtcNow;

			string statusURI = m_TaskParams.GetParam("MyEMSL_Status_URI", "");

			if (string.IsNullOrEmpty(statusURI))
			{
				string msg = "MyEMSL_Status_URI is empty; cannot verify upload status";
				mRetData.CloseoutMsg = msg;
				mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return false;
			}

			// Call the testauth service to obtain a cookie for this session
			string authURL = Configuration.TestAuthUri;
			Auth auth = new Auth(new Uri(authURL));

			CookieContainer cookieJar = null;
			if (!auth.GetAuthCookies(out cookieJar))
			{
				string msg = "Auto-login to " + Configuration.TestAuthUri + " failed authentication";
				mRetData.CloseoutMsg = "Failed to obtain MyEMSL session cookie";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return false;
			}

			int exceptionCount = 0;
			while (DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds < MAX_WAIT_TIME_SECONDS)
			{
				
				try
				{
					int timeoutSeconds = 5;
					HttpStatusCode responseStatusCode;

					string xmlServerResponse = EasyHttp.Send(statusURI, out responseStatusCode, timeoutSeconds);
					
					bool abortNow;
					string dataReceivedMessage;

					if (this.WasDataReceived(xmlServerResponse, out abortNow, out dataReceivedMessage))
					{
						Utilities.Logout(cookieJar);
						return true;
					}

					if (abortNow)
					{
						mRetData.CloseoutMsg = dataReceivedMessage;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, dataReceivedMessage);
						Utilities.Logout(cookieJar);
						return false;
					}

				}
				catch (Exception ex)
				{
					exceptionCount++;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception checking upload status", ex);
					if (exceptionCount >= 3)
						break;
				}

				exceptionCount = 0;
			
				// Sleep for 5 seconds
				System.Threading.Thread.Sleep(5000);

			}

			return false;
		}

		
			
		protected bool CompareToFilesOnDisk(List<MyEMSLReader.ArchivedFileInfo>lstArchivedFiles)
		{

			// TODO: Compare the files in lstArchivedFiles to those actually on disk

			return false;

		}

		protected Boolean CreateMD5StageFile(List<MyEMSLReader.ArchivedFileInfo> lstArchivedFiles)
		{
			return false;

		}

		protected Boolean VisibleInElasticSearch()
		{
			var reader = new MyEMSLReader.Reader();
			reader.IncludeAllRevisions = false;

			// Attach events
			reader.ErrorEvent += new MyEMSLReader.MyEMSLBase.MessageEventHandler(reader_ErrorEvent);
			reader.MessageEvent += new MyEMSLReader.MyEMSLBase.MessageEventHandler(reader_MessageEvent);
			reader.ProgressEvent += new MyEMSLReader.MyEMSLBase.ProgressEventHandler(reader_ProgressEvent);

			string subDir = m_TaskParams.GetParam("Output_Folder_Name", "");

			var lstArchivedFiles = reader.FindFilesByDatasetID(m_DatasetID, subDir);

			if (lstArchivedFiles.Count == 0)
			{
				string msg = "Elastic search did not find any files for Dataset_ID " + m_DatasetID;
					if (!string.IsNullOrEmpty(subDir))
						msg += " and subdirectory " + subDir;

				mRetData.CloseoutMsg = msg;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return false;
			}


			bool success = CompareToFilesOnDisk(lstArchivedFiles);

			if (success)
				success = CreateMD5StageFile(lstArchivedFiles);

			return success;

		}

		protected bool WasDataReceived(string xmlServerResponse, out bool abortNow, out string dataReceivedMessage)
		{
			Boolean success = false;
			abortNow = false;
			dataReceivedMessage = string.Empty;

			try
			{
				System.Xml.XmlDocument xmlDoc = new System.Xml.XmlDocument();
				xmlDoc.LoadXml(xmlServerResponse);

				// Example XML:
				//
				// <?xml version="1.0"?>
				// <myemsl>
				// 	<status username='70000'>
				// 		<transaction id='111177' />
				// 		<step id='0' message='completed' status='SUCCESS' />
				// 		<step id='1' message='completed' status='SUCCESS' />
				// 		<step id='2' message='completed' status='SUCCESS' />
				// 		<step id='3' message='completed' status='SUCCESS' />
				// 		<step id='4' message='completed' status='SUCCESS' />
				// 		<step id='5' message='completed' status='SUCCESS' />
				// 		<step id='6' message='verified' status='SUCCESS' />
				// 	</status>
				// </myemsl>
				// 
				// Step IDs correspond to:
				// 0: Submitted
				// 1: Received
				// 2: Processing
				// 3: Verified
				// 4: Stored
				// 5: Available   (status will be "ERROR" if user doesn't have upload permissions for a proposal; 
				//                 for example https://a4.my.emsl.pnl.gov/myemsl/cgi-bin/status/1042281/xml shows message 
				//                 "You(47943) do not have upload permissions to proposal 17797"
				//                 for user svc-dms on May 3, 2012)
				// 6: Archived    (status will be "UNKNOWN" if not yet verified)

				// Check the "available" entry (ID=5) to make sure everything came through ok

				string query = string.Format("//*[@id='{0}']", 5);
				System.Xml.XmlNode statusElement = xmlDoc.SelectSingleNode(query);

				string message = statusElement.Attributes["message"].Value;
				string status = statusElement.Attributes["status"].Value;

				if (status.ToLower() == "success" && message.ToLower() == "completed")
				{
					dataReceivedMessage = "Data is available";
					success = true;
				}

				if (message.Contains("do not have upload permissions"))
				{
					dataReceivedMessage = "Permissions error: " + message;
					abortNow = true;
				}

			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in WasDataReceived", ex);
			}

			return success;
		}

		#endregion

		#region "Event Handlers"
		void reader_ErrorEvent(object sender, MyEMSLReader.MessageEventArgs e)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "MyEMSLReader: " + e.Message);
		}

		void reader_MessageEvent(object sender, MyEMSLReader.MessageEventArgs e)
		{
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, e.Message);
		}

		void reader_ProgressEvent(object sender, MyEMSLReader.ProgressEventArgs e)
		{
			string msg = "Percent complete: " + e.PercentComplete.ToString("0.0") + "%";

			if (e.PercentComplete > mPercentComplete || DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 30)
			{				
				if (DateTime.UtcNow.Subtract(mLastProgressUpdateTime).TotalSeconds >= 1)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
					mPercentComplete = e.PercentComplete;
					mLastProgressUpdateTime = DateTime.UtcNow;
				}
			}
		}


		#endregion

	}	// End class

}	// End namespace
