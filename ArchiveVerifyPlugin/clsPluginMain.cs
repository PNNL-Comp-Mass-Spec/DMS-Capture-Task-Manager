using System;
using CaptureTaskManager;
using System.Collections.Generic;

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
		/// <returns>Enum indicating success or failure</returns>
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

			// ToDo: Perform work here
			bool success = false;

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
				msg = "Problem verifying data in MyEMSL for dataset " + m_Dataset + ". See local log for details";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.WARN, msg);
				mRetData.EvalCode = EnumEvalCode.EVAL_CODE_FAILED;
				mRetData.EvalMsg = msg;
				mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
			}

			
			msg = "Completed clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			return mRetData;

		}	// End sub



		protected bool CheckUploadStatus(string statusURI, out string errorMessage)
		{

		
			errorMessage = string.Empty;

			// Monitor the upload status for 60 seconds
			// If still not complete after 60 seconds, then this manager will tell the DMS_Capture DB to bump the Next_Try value by 15 minutes and reset the state to 2
			const int MAX_WAIT_TIME_SECONDS = 60;

			DateTime dtStartTime = DateTime.UtcNow;

			// Call the testauth service to obtain a cookie for this session
			string authURL = MyEMSLReader.MyEMSLBase.MYEMSL_URI_BASE + "testauth";
			Auth auth = new Auth(new Uri(authURL));

			if (cookieJar == null)
			{
				if (!auth.GetAuthCookies(out cookieJar))
				{
					ReportError("Auto-login to ingest.my.emsl.pnl.gov failed authentication");
					return string.Empty;
				}
			}


			while (DateTime.UtcNow.Subtract(dtStartTime).TotalSeconds < MAX_WAIT_TIME_SECONDS)
			{
				if (currentLoopDelaySec > 10)
				{
					RaiseDebugEvent("UploadMonitorLoop", "Waiting " + currentLoopDelaySec + " seconds");
				}

				System.Threading.Thread.Sleep(currentLoopDelaySec * 1000);

				try
				{
					int timeoutSeconds = 5;
					HttpStatusCode responseStatusCode;

					xmlServerResponse = EasyHttp.Send(statusURI, out responseStatusCode, timeoutSeconds);
					if (this.WasDataReceived(xmlServerResponse, out abortNow, out dataReceivedMessage))
					{

						string logoutURL = Configuration.ServerUri + "/myemsl/logout";
						timeoutSeconds = 5;
						string response = EasyHttp.Send(logoutURL, mCookieJar, out responseStatusCode, timeoutSeconds);

						return true;
					}

					if (abortNow)
					{
						errorMessage = string.Copy(dataReceivedMessage);
						return false;
					}

				}
				catch (Exception ex)
				{
					RaiseErrorEvent("UploadMonitorLoop", ex.Message);
				}

				if (iterations == 1)
					RaiseDebugEvent("UploadMonitorLoop", "Data not yet ready; see " + statusURI);

				iterations++;
				if (currentLoopDelaySec < maxLoopDelaySec)
				{
					currentLoopDelaySec *= 2;
					if (currentLoopDelaySec > maxLoopDelaySec)
						currentLoopDelaySec = maxLoopDelaySec;
				}

			}

			RaiseErrorEvent("UploadMonitorLoop", "Data not received after waiting " + System.DateTime.UtcNow.Subtract(dtStartTime).TotalMinutes.ToString("0.0") + " minutes");

			//e.Result = false;
			return false;
		}

		private Boolean WasDataReceived(string xmlServerResponse, out bool abortNow, out string dataReceivedMessage)
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
					dataReceivedMessage = "Aborting upload due to permissions error: " + message;
					abortNow = true;
				}

			}
			catch (Exception ex)
			{
				RaiseErrorEvent("WasDataReceived", ex.Message);
			}

			return success;
		}


		void myEMSLUpload_DataReceivedAndVerified(bool successfulVerification, string errorMessage)
		{
			string msg;
			if (successfulVerification)
			{
				msg = "  ... DataReceivedAndVerified success = true";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				m_MyEmslUploadSuccess = true;
			}
			else
			{
				msg = "  ... DataReceivedAndVerified success = false: " + errorMessage;
				if (errorMessage.Contains("do not have upload permissions"))
				{
					m_WarningMsg = AppendToString(m_WarningMsg, errorMessage);
					m_MyEmslUploadPermissionsError = true;
				}

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				m_MyEmslUploadSuccess = false;
			}
		}


		#endregion

	}	// End class

}	// End namespace
