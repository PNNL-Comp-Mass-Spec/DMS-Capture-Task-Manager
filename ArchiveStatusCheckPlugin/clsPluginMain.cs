using System;
using CaptureTaskManager;
using Pacifica.Core;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Data.SqlClient;
using System.Text.RegularExpressions;

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
			string msg = "Starting ArchiveStatusCheckPlugin.clsPluginMain.RunTool()";
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
			bool success = false;

			try
			{
				// Examine the MyEMSL ingest status page
				success = CheckArchiveStatus();
			}
			catch (Exception ex)
			{
				msg = "Exception checking archive status";
				mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				mRetData.CloseoutMsg = msg + ": " + ex.Message;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
			}
			

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
			// Keys are StatusNum integers, values are StatusURI strings
			const int retryCount = 2;
			var dctURIs = GetStatusURIs(retryCount);

			var dctVerifiedURIs = new Dictionary<int, string>();
			var lstUnverifiedURIs = new List<string>();
			string msg;

			if (dctURIs.Count == 0)
			{
				msg = "Could not find any MyEMSL_Status_URIs; cannot verify archive status";
				mRetData.CloseoutMsg = msg;
				mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg + " for job " + m_Job);
				return false;
			}

			// Call the testauth service to obtain a cookie for this session
			string authURL = Configuration.TestAuthUri;
			var auth = new Auth(new Uri(authURL));

			CookieContainer cookieJar;
			if (!auth.GetAuthCookies(out cookieJar))
			{
				msg = "Auto-login to " + Configuration.TestAuthUri + " failed authentication";
				mRetData.CloseoutMsg = "Failed to obtain MyEMSL session cookie";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return false;
			}

			int exceptionCount = 0;
			var statusChecker = new MyEMSLStatusCheck();

            statusChecker.ErrorEvent += statusChecker_ErrorEvent;
			foreach (var statusInfo in dctURIs)
			{
				string statusURI = statusInfo.Value;

				try
				{
                    string xmlServerResponse;
                    bool ingestSuccess = base.GetMyEMSLIngestStatus(m_Job, statusChecker, statusURI, cookieJar, ref mRetData, out xmlServerResponse);

                    if (!ingestSuccess)
                    {
                        lstUnverifiedURIs.Add(statusInfo.Value);

                        if (mRetData.EvalCode == EnumEvalCode.EVAL_CODE_FAILURE_DO_NOT_RETRY)
                            break;
                        
                        continue;
                    }

                    string statusMessage;
                    string errorMessage;
                    bool success = statusChecker.IngestStepCompleted(
                        xmlServerResponse,
					    MyEMSLStatusCheck.StatusStep.Archived,
					    out statusMessage, 
                        out errorMessage);

				    if (success)
				    {
                        dctVerifiedURIs.Add(statusInfo.Key, statusInfo.Value);
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, statusMessage + ", job " + m_Job + ", " + dctVerifiedURIs.Values);
                        continue;				
				    }

                    // Look for critical errors in statusMessage
                    if (!string.IsNullOrEmpty(errorMessage))
                    {
                        if (statusChecker.IsCriticalError(errorMessage))
                        {
                            mRetData.CloseoutMsg = errorMessage;
                            mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, errorMessage + ", job " + m_Job);
                        }
                    }

				}
				catch (Exception ex)
				{
					exceptionCount++;
					if (exceptionCount < 3)
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Exception verifying archive status for job " + m_Job + ": " + ex.Message);
					}
					else
					{
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception verifying archive status for job " + m_Job + ": ", ex);
                        break;
					}
				}

				lstUnverifiedURIs.Add(statusInfo.Value);
				exceptionCount = 0;

			}

			Utilities.Logout(cookieJar);

			if (dctVerifiedURIs.Count > 0)
			{ 
				// Update the Verified flag in T_MyEMSL_Uploads
				UpdateVerifiedURIs(dctVerifiedURIs);
			}

			if (dctVerifiedURIs.Count == dctURIs.Count)
				return true;
			
			string firstUnverified = "??";
			if (lstUnverifiedURIs.Count > 0)
				firstUnverified = lstUnverifiedURIs.First();

			if (dctVerifiedURIs.Count == 0)
			{
				msg = "MyEMSL archive status not yet verified; see " + firstUnverified;
			}
			else
				msg = "MyEMSL archive status partially verified (success count = " + dctVerifiedURIs.Count + ", unverified count = " + lstUnverifiedURIs.Count() + "); first not verified: " + firstUnverified;

            if (mRetData.EvalCode != EnumEvalCode.EVAL_CODE_FAILURE_DO_NOT_RETRY || string.IsNullOrEmpty(mRetData.CloseoutMsg))
    			mRetData.CloseoutMsg = msg;

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
			return false;
		}

		protected Dictionary<int, string> GetStatusURIs(int retryCount)
		{
			var dctURIs = new Dictionary<int, string>();

			// This Connection String points to the DMS_Capture database
			string connectionString = m_MgrParams.GetParam("connectionstring");

			// First look for a specific Status_URI for this job			
			string statusURI = m_TaskParams.GetParam("MyEMSL_Status_URI", "");

			if (!string.IsNullOrEmpty(statusURI))
			{
				// Parse out the StatusID from the URI
				var reGetStatusNum = new Regex(@"(\d+)/xml", RegexOptions.IgnoreCase);
				var reMatch = reGetStatusNum.Match(statusURI);
				if (!reMatch.Success)
					throw new Exception("Could not find Status ID in StatusURI: " + statusURI);

				int statusNum;
				int.TryParse(reMatch.Groups[1].Value, out statusNum);
				
				if (statusNum <= 0)
					throw new Exception("Status ID is 0 in StatusURI: " + statusURI);

				dctURIs.Add(statusNum, statusURI);
				return dctURIs;
			}

			try
			{

				string sql = "SELECT StatusNum, Status_URI FROM V_MyEMSL_Uploads WHERE Dataset_ID = " + m_DatasetID + " AND Job <= " + m_Job + " AND ISNULL(StatusNum, 0) > 0 AND ErrorCode NOT IN (-1, 101)";

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
								int statusNum = reader.GetInt32(0);						

								if (!Convert.IsDBNull(reader.GetValue(1)))
								{
									var value = (string)reader.GetValue(1);
									if (!string.IsNullOrEmpty(value))
										dctURIs.Add(statusNum, value);
								}								
							}
						}
						break;
					}
					catch (Exception ex)
					{
						retryCount -= 1;
						string msg = "ArchiveStatusCheckPlugin, GetStatusURIs; Exception querying database: " + ex.Message + "; ConnectionString: " + connectionString;
						msg += ", RetryCount = " + retryCount;
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

			return dctURIs;
		}

        void statusChecker_ErrorEvent(object sender, MessageEventArgs e)
        {
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, e.Message);
        }

		protected bool UpdateVerifiedURIs(Dictionary<int, string> dctVerifiedURIs)
		{
			const string SP_NAME = "SetMyEMSLUploadVerified";

			try
			{
				string statusNums = string.Join(",", (from item in dctVerifiedURIs select item.Key));

				var cmd = new SqlCommand(SP_NAME)
				{
					CommandType = System.Data.CommandType.StoredProcedure
				};

				cmd.Parameters.Add("@Return", System.Data.SqlDbType.Int);
				cmd.Parameters["@Return"].Direction = System.Data.ParameterDirection.ReturnValue;

				cmd.Parameters.Add("@DatasetID", System.Data.SqlDbType.Int);
				cmd.Parameters["@DatasetID"].Direction = System.Data.ParameterDirection.Input;
				cmd.Parameters["@DatasetID"].Value = m_DatasetID;

				cmd.Parameters.Add("@StatusNumList", System.Data.SqlDbType.VarChar, 1024);
				cmd.Parameters["@StatusNumList"].Direction = System.Data.ParameterDirection.Input;
				cmd.Parameters["@StatusNumList"].Value = statusNums;

				cmd.Parameters.Add("@message", System.Data.SqlDbType.VarChar, 512);
				cmd.Parameters["@message"].Direction = System.Data.ParameterDirection.Output;

				m_ExecuteSP.TimeoutSeconds = 20;
				var resCode = m_ExecuteSP.ExecuteSP(cmd, 2);

				if (resCode == 0)
					return true;
				
				var msg = "Error " + resCode + " calling stored procedure " + SP_NAME;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return false;
			}
			catch (Exception ex)
			{
				const string msg = "Exceptiong calling stored procedure " + SP_NAME;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
				return false;
			}

		}
	
		#endregion


	}	// End class

}	// End namespace
