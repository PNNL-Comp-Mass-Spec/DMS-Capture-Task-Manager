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
				mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
				mRetData.CloseoutMsg = "Exception checking archive status: " + ex.Message;
				msg = "Exception checking archive status for job " + m_Job;
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

			const int retryCount = 2;

            // Keys in dctStatusSubfolders are StatusNum integers, values are the subfolder that was uploaded (blank means the entire dataset)
            Dictionary<int, string> dctStatusSubfolders;

            // Keys in dctURIs are StatusNum integers, values are StatusURI strings
            var dctURIs = GetStatusURIs(retryCount, out dctStatusSubfolders);

            // Keys in this dictionary are StatusNum; values are StatusURI strings
			Dictionary<int, string> dctVerifiedURIs;

            // Keys in this dictionary are StatusNum; values are critical error messages
            Dictionary<int, string> dctCriticalErrors;

            // Keys in this dictionary are StatusNum; values are StatusURI strings
            Dictionary<int, string> dctUnverifiedURIs;
		    
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
				mRetData.CloseoutMsg = "Failed to obtain MyEMSL session cookie";
				msg = "Auto-login to " + Configuration.TestAuthUri + " failed authentication for job " + m_Job;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return false;
			}

			var statusChecker = new MyEMSLStatusCheck();

            statusChecker.ErrorEvent += statusChecker_ErrorEvent;

            // Check the status of each of the URIs
		    CheckStatusURIs(statusChecker, cookieJar, dctURIs, out dctUnverifiedURIs, out dctVerifiedURIs, out dctCriticalErrors);

			Utilities.Logout(cookieJar);

			if (dctVerifiedURIs.Count > 0)
			{ 
				// Update the Verified flag in T_MyEMSL_Uploads
				UpdateVerifiedURIs(dctVerifiedURIs);
			}

		    if (dctCriticalErrors.Count > 0)
		    {
                mRetData.CloseoutMsg = dctCriticalErrors.First().Value;
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

		        foreach (var criticalError in dctCriticalErrors)
		        {
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Critical MyEMSL upload error for job " + m_Job + ", status num " + criticalError.Key + ": " + criticalError.Value);
		        }		        
            }

            if (dctUnverifiedURIs.Count > 0 && dctVerifiedURIs.Count > 0)
            {
                CompareUnverifiedAndVerifiedURIs(
                    dctUnverifiedURIs,
                    dctVerifiedURIs,
                    dctStatusSubfolders,
                    dctURIs);
            }


		    if (dctVerifiedURIs.Count == dctURIs.Count)
		    {
		        if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
		        {
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                    mRetData.CloseoutMsg = string.Empty;
		        }
		        
		        return true;
		    }

		    string firstUnverified = "??";
			if (dctUnverifiedURIs.Count > 0)
				firstUnverified = dctUnverifiedURIs.First().Value;

			if (dctVerifiedURIs.Count == 0)
			{
				msg = "MyEMSL archive status not yet verified; see " + firstUnverified;
			}
			else
				msg = "MyEMSL archive status partially verified (success count = " + dctVerifiedURIs.Count + ", unverified count = " + dctUnverifiedURIs.Count() + "); first not verified: " + firstUnverified;

            if (mRetData.EvalCode != EnumEvalCode.EVAL_CODE_FAILURE_DO_NOT_RETRY || string.IsNullOrEmpty(mRetData.CloseoutMsg))
    			mRetData.CloseoutMsg = msg;

			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
			return false;
		}

        protected void CheckStatusURIs(
            MyEMSLStatusCheck statusChecker, 
            CookieContainer cookieJar,
            Dictionary<int, string> dctURIs, 
            out Dictionary<int, string> dctUnverifiedURIs,
            out Dictionary<int, string> dctVerifiedURIs,
            out Dictionary<int, string> dctCriticalErrors)
        {
            int exceptionCount = 0;

            dctUnverifiedURIs = new Dictionary<int, string>();
            dctVerifiedURIs = new Dictionary<int, string>();
            dctCriticalErrors = new Dictionary<int, string>();

            foreach (var statusInfo in dctURIs)
            {
                int statusNum = statusInfo.Key;
                string statusURI = statusInfo.Value;

                try
                {
                    string xmlServerResponse;
                    bool ingestSuccess = base.GetMyEMSLIngestStatus(m_Job, statusChecker, statusURI, cookieJar, ref mRetData, out xmlServerResponse);

                    if (!ingestSuccess)
                    {
                        dctUnverifiedURIs.Add(statusNum, statusURI);
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
                        dctVerifiedURIs.Add(statusNum, statusURI);
                        clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Successful MyEMSL upload for job " + m_Job + ", status num " + statusNum + ": " + statusURI);
                        continue;
                    }

                    // Look for critical errors in statusMessage
                    if (!string.IsNullOrEmpty(errorMessage))
                    {
                        if (statusChecker.IsCriticalError(errorMessage))
                        {
                            if (!dctCriticalErrors.ContainsKey(statusNum))
                            {
                                dctCriticalErrors.Add(statusNum, errorMessage);
                            }
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

                if (!dctUnverifiedURIs.ContainsKey(statusNum))
                {
                    dctUnverifiedURIs.Add(statusNum, statusURI);
                }

                exceptionCount = 0;

            }
        }

            /// <summary>
        /// Step through the unverified URIs to see if the same subfolder was subsequently successfully uploaded 
        /// (could be a blank subfolder, meaning the instrument data and all jobs) 
        /// </summary>
        /// <param name="dctUnverifiedURIs">Unverified URIs</param>
        /// <param name="dctVerifiedURIs">Verified URIs</param>
        /// <param name="dctStatusSubfolders">Subfolder name for each StatusNum</param>
        /// <param name="dctURIs">Status URI for each StatusNum</param>
        protected void CompareUnverifiedAndVerifiedURIs(
            Dictionary<int, string> dctUnverifiedURIs, 
            Dictionary<int, string> dctVerifiedURIs, 
            Dictionary<int, string> dctStatusSubfolders,
            Dictionary<int, string> dctURIs)
	    {
            var lstStatusNumsToIgnore = new List<int>();

            foreach (var unverifiedEntry in dctUnverifiedURIs)
            {
                int unverifiedStatusNum = unverifiedEntry.Key;
                string unverifiedSubfolder;

                if (!dctStatusSubfolders.TryGetValue(unverifiedStatusNum, out unverifiedSubfolder))
                    continue;

                // Find StatusNums that had the same subfolder
                // Note: cannot require that identical matches have a larger StatusNum because sometimes 
                // extremely large status values (like 1168231360) are assigned to failed uploads
                var lstIdenticalStatusNums = (from item in dctStatusSubfolders
                                              where item.Key != unverifiedStatusNum &&
                                                    item.Value == unverifiedSubfolder
                                              select item.Key).ToList();

                if (lstIdenticalStatusNums.Count == 0)
                    continue;

                // Check if any of the identical entries has been successfully verified
                foreach (var identicalStatusNum in lstIdenticalStatusNums)
                {
                    if (dctVerifiedURIs.ContainsKey(identicalStatusNum))
                    {
                        lstStatusNumsToIgnore.Add(unverifiedStatusNum);
                        break;
                    }
                }

            }

            if (lstStatusNumsToIgnore.Count > 0)
            {
                // Found some URIs that we can ignore
                foreach (var statusNumToRemove in lstStatusNumsToIgnore)
                {
                    dctUnverifiedURIs.Remove(statusNumToRemove);
                    dctURIs.Remove(statusNumToRemove);
                }

                // Set the ErrorCode to 101 in T_MyEMSL_Uploads
                UpdateSupersededURIs(lstStatusNumsToIgnore);
            }

	    }

	    protected Dictionary<int, string> GetStatusURIs(int retryCount, out Dictionary<int, string> dctStatusSubfolders)
		{
            // Keys in this dictionary are StatusNum integers; values are Status URIs (e.g. https://a4.my.emsl.pnl.gov/myemsl/cgi-bin/status/2623335/xml)
			var dctURIs = new Dictionary<int, string>();

            // Keys in this dictionary are StausNum integers; values are the Subfolder for the given archive task (a blank subfolder means all dataset files and subfolders)
            dctStatusSubfolders = new Dictionary<int, string>();

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

                // Note that GetStatusURIsAndSubfolders requires that the column order be StatusNum, Status_URI, Subfolder
                string sql = "SELECT StatusNum, Status_URI, Subfolder " +
                             "FROM V_MyEMSL_Uploads " +
                             "WHERE Dataset_ID = " + m_DatasetID + " AND " +
                                   "StatusNum = " + statusNum;

                GetStatusURIsAndSubfolders(sql, retryCount, dctURIs, dctStatusSubfolders);

				return dctURIs;
			}

			try
			{
                // Note that GetStatusURIsAndSubfolders requires that the column order be StatusNum, Status_URI, Subfolder
				string sql = "SELECT StatusNum, Status_URI, Subfolder " +
				             "FROM V_MyEMSL_Uploads " +
				             "WHERE Dataset_ID = " + m_DatasetID + " AND " +
				                   "Job <= " + m_Job + " AND " +
				                   "ISNULL(StatusNum, 0) > 0 AND " +
				                   "ErrorCode NOT IN (-1, 101)";

                GetStatusURIsAndSubfolders(sql, retryCount, dctURIs, dctStatusSubfolders);

			}
			catch (Exception ex)
			{
				string msg = "Exception connecting to database for job " + m_Job + ": " + ex.Message;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
			}

			return dctURIs;
		}

        protected void GetStatusURIsAndSubfolders(string sql, int retryCount, Dictionary<int, string> dctURIs, Dictionary<int, string> dctStatusSubfolders)
	    {
            // This Connection String points to the DMS_Capture database
            string connectionString = m_MgrParams.GetParam("connectionstring");

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

                            if (Convert.IsDBNull(reader.GetValue(1)))
                            {
                                continue;
                            }

                            var uri = (string)reader.GetValue(1);
                            if (string.IsNullOrEmpty(uri))
                            {
                                continue;
                            }

                            if (dctURIs.ContainsKey(statusNum))
                            {
                                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, "Job " + m_Job + " has more than one upload task with StatusID " + statusNum);
                                continue;
                            }

                            dctURIs.Add(statusNum, uri);

                            var subFolder = (string)reader.GetValue(2);
                            dctStatusSubfolders.Add(statusNum, subFolder);
                        }
                    }
                    break;
                }
                catch (Exception ex)
                {
                    retryCount -= 1;
                    string msg = "ArchiveStatusCheckPlugin, GetStatusURIs; Exception querying database for job " + m_Job + ": " + ex.Message + "; ConnectionString: " + connectionString;
                    msg += ", RetryCount = " + retryCount;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

                    //Delay for 5 second before trying again
                    System.Threading.Thread.Sleep(5000);
                }
            }

	    }

	    void statusChecker_ErrorEvent(object sender, MessageEventArgs e)
        {
            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Status checker error for job " + m_Job + ": " + e.Message);
        }

	    protected bool UpdateSupersededURIs(List<int> lstStatusNumsToIgnore)
        {
            const string SP_NAME = "SetMyEMSLUploadSupersededIfFailed";

            try
            {
                string statusNums = string.Join(",", lstStatusNumsToIgnore);

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

                var msg = "Error " + resCode + " calling stored procedure " + SP_NAME + ", job " + m_Job;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                return false;
            }
            catch (Exception ex)
            {
                var msg = "Exception calling stored procedure " + SP_NAME + ", job " + m_Job;
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
                return false;
            }

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
				
				var msg = "Error " + resCode + " calling stored procedure " + SP_NAME + ", job " + m_Job;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
				return false;
			}
			catch (Exception ex)
			{
                var msg = "Exception calling stored procedure " + SP_NAME + ", job " + m_Job;
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
				return false;
			}

		}
	
		#endregion


	}	// End class

}	// End namespace
