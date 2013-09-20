using System;
using CaptureTaskManager;
using Pacifica.Core;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.IO;
using System.Data.SqlClient;

namespace ArchiveStatusCheckPlugin
{
	public class clsPluginMain : clsToolRunnerBase
	{
		//*********************************************************************************************************
		// Main class for plugin
		//**********************************************************************************************************

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
		/// Runs the Archive Status Check step tool
		/// </summary>
		/// <returns>Class with completionCode, completionMessage, evaluationCode, and evaluationMessage</returns>
		public override clsToolReturnData RunTool()
		{
			string msg;

			msg = "Starting ArchiveStatusCheckPlugin.clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			// Perform base class operations, if any
			mRetData = base.RunTool();
			if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
				return mRetData;

			if (m_DebugLevel >= 5)
			{
				msg = "Verifying status of files in MyEMSL for dataset '" + m_Dataset + "'";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
			}

			// Set this to Success for now
			mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;

			// Examine the MyEMSL ingest status page
			bool success = CheckArchiveStatus();

			if (success)
			{
				// Everything was good
				if (m_DebugLevel >= 4)
				{
					msg = "MyEMSL status verification successful for dataset " + m_Dataset;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
				}
				mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;

				mRetData.EvalCode = EnumEvalCode.EVAL_CODE_SUCCESS;
					
			}
			else
			{
				// Files are not yet verified
				// Return completionCode CLOSEOUT_NOT_READY=2 
				// which will tell the DMS_Capture DB to reset the task to state 2 and bump up the Next_Try value by 60 minutes
				if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_SUCCESS)
					mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
			}

			msg = "Completed clsPluginMain.RunTool()";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

			return mRetData;

		}	// End sub

		protected bool CheckArchiveStatus()
		{

			// Examine the upload status for any uploads for this dataset, filtering on job number to ignore jobs created after this job
			
			// First obtain a list of status URIs to check
			int retryCount = 2;
			var lstURIs = GetStatusURIs(retryCount);
			var lstUnverifiedURIs = new List<string>();

			if (lstURIs.Count == 0)
			{
				string msg = "Could not find any MyEMSL_Status_URIs; cannot verify archive status";
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
			int verifiedCount = 0;

			foreach (var statusURI in lstURIs)
			{

				try
				{
					int timeoutSeconds = 5;
					HttpStatusCode responseStatusCode;

					string xmlServerResponse = EasyHttp.Send(statusURI, out responseStatusCode, timeoutSeconds);

					bool abortNow;
					string dataVerificationMessage;

					if (this.WasDataVerified(xmlServerResponse, out abortNow, out dataVerificationMessage))
					{
						verifiedCount++;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, dataVerificationMessage);
						continue;
					}

					if (abortNow)
					{
						mRetData.CloseoutMsg = dataVerificationMessage;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, dataVerificationMessage);
						Utilities.Logout(cookieJar);
						return false;
					}

					lstUnverifiedURIs.Add(statusURI);

				}
				catch (Exception ex)
				{
					exceptionCount++;
					if (exceptionCount < 3)
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception verifying archive status: " + ex.Message);
					}
					else
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception verifying archive status: ", ex);
						break;
					}
				}

				exceptionCount = 0;

			}

			Utilities.Logout(cookieJar);

			if (verifiedCount == lstURIs.Count)
				return true;
			else
			{
				string msg;
				if (verifiedCount == 0)
				{
					msg = "MyEMSL archive status not yet verified; see " + lstUnverifiedURIs.First();
				}
				else 
					msg = "MyEMSL archive status partially verified (success count = " + verifiedCount + ", unverified count = " + lstUnverifiedURIs.Count() + "); first not verified: " + lstUnverifiedURIs.First();

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
				return false;
			}

		}

		protected List<string> GetStatusURIs(int retryCount)
		{
			var lstURIs = new List<string>();

			// This Connection String points to the DMS_Capture database
			string connectionString = m_MgrParams.GetParam("connectionstring");

			// First look for a specific Status_URI for this job			
			string statusURI = m_TaskParams.GetParam("MyEMSL_Status_URI", "");

			if (!string.IsNullOrEmpty(statusURI))
			{
				lstURIs.Add(statusURI);
				return lstURIs;
			}

			try
			{
			
				string sql = "SELECT Status_URI FROM V_MyEMSL_Uploads WHERE Dataset_ID = " + m_DatasetID + " AND Job <= " + m_Job + " AND ISNULL(StatusNum, 0) > 0";

				while (retryCount > 0)
				{
					try
					{
						using (var cnDB = new SqlConnection(connectionString))
						{
							cnDB.Open();

							var cmd = new SqlCommand(sql, cnDB);
							var reader = cmd.ExecuteReader();

							while (reader.Read())
							{
								
								if (!Convert.IsDBNull(reader.GetValue(0)))
								{
									string value = (string)reader.GetValue(0);
									if (!string.IsNullOrEmpty(value))
										lstURIs.Add(value);
								}
								
							}
						}
						break;
					}
					catch (Exception ex)
					{
						retryCount -= 1;
						string msg = "ArchiveStatusCheckPlugin, GetStatusURIs; Exception querying database: " + ex.Message + "; ConnectionString: " + connectionString;
						msg += ", RetryCount = " + retryCount.ToString();
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

						//Delay for 5 second before trying again
						System.Threading.Thread.Sleep(5000);
					}
				}
			}
			catch (Exception ex)
			{
				string msg = "Exception connecting to database: " + ex.Message + "; ConnectionString: " + connectionString;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
			}

			return lstURIs;
		}

		protected bool WasDataVerified(string xmlServerResponse, out bool abortNow, out string dataVerificationMessage)
		{
			bool success = false;
			abortNow = false;
			dataVerificationMessage = string.Empty;

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

				// Check the "Archived" entry (ID=6) to make sure the data has been fully verified

				string query = string.Format("//*[@id='{0}']", 6);
				System.Xml.XmlNode statusElement = xmlDoc.SelectSingleNode(query);

				string message = statusElement.Attributes["message"].Value;
				string status = statusElement.Attributes["status"].Value;

				if (status.ToLower() == "success" && message.ToLower() == "verified")
				{
					dataVerificationMessage = "Data is verified";
					success = true;
				}

				if (message.Contains("do not have upload permissions"))
				{
					dataVerificationMessage = "Permissions error: " + message;
					abortNow = true;
				}

			}
			catch (Exception ex)
			{
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception in WasDataVerified", ex);
				return false;
			}

			return success;
		}

		#endregion


	}	// End class

}	// End namespace
