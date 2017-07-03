using System;
using CaptureTaskManager;
using Pacifica.Core;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Net;
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

        #region "Methods"

        /// <summary>
        /// Runs the Archive Status Check step tool
        /// </summary>
        /// <returns>Class with completionCode, completionMessage, evaluationCode, and evaluationMessage</returns>
        public override clsToolReturnData RunTool()
        {
            var msg = "Starting ArchiveStatusCheckPlugin.clsPluginMain.RunTool()";
            LogDebug(msg);

            // Perform base class operations, if any
            mRetData = base.RunTool();
            if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
                return mRetData;

            if (m_DebugLevel >= 5)
            {
                msg = "Verifying status of files in MyEMSL for dataset '" + m_Dataset + "'";
                LogMessage(msg);
            }

            // Set this to Success for now
            mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            var success = false;

            try
            {
                // Examine the MyEMSL ingest status page
                success = CheckArchiveStatus();
            }
            catch (Exception ex)
            {
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;       // Possibly instead use CLOSEOUT_NOT_READY
                mRetData.CloseoutMsg = "Exception checking archive status (ArchiveStatusCheckPlugin): " + ex.Message;
                msg = "Exception checking archive status for job " + m_Job;
                LogError(msg, ex);
            }


            if (success)
            {
                // Everything was good
                if (m_DebugLevel >= 4)
                {
                    msg = "MyEMSL status verification successful for dataset " + m_Dataset;
                    LogMessage(msg);
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
            LogDebug(msg);

            return mRetData;

        }

        private bool CheckArchiveStatus()
        {

            // Examine the upload status for any uploads for this dataset, filtering on job number to ignore jobs created after this job

            // First obtain a list of status URIs to check

            // Keys in dctStatusData are StatusNum integers, values are instances of class clsIngestStatusInfo
            var dctStatusData = GetStatusURIs();

            // Keys in this dictionary are StatusNum; values are critical error messages
            Dictionary<int, string> dctCriticalErrors;

            string msg;

            if (dctStatusData.Count == 0)
            {
                msg = "Could not find any MyEMSL_Status_URIs; cannot verify archive status. " +
                      "If all entries for Dataset " + m_DatasetID + " have ErrorCode -1 or 101 this job step should be manually skipped";

                mRetData.CloseoutMsg = msg;
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                LogError(msg + " for job " + m_Job);
                return false;
            }

            var statusChecker = new MyEMSLStatusCheck();

            statusChecker.ErrorEvent += statusChecker_ErrorEvent;

            // Check the status of each of the URIs
            // Keys in dctUnverifiedURIs and dctVerifiedURIs are StatusNum; values are StatusURI strings

            CheckStatusURIs(statusChecker, dctStatusData,
                out var dctUnverifiedURIs, out var dctVerifiedURIs, out dctCriticalErrors);

            if (dctVerifiedURIs.Count > 0)
            {
                // Update the Verified flag in T_MyEMSL_Uploads
                UpdateVerifiedURIs(dctVerifiedURIs, dctStatusData);
            }

            if (dctCriticalErrors.Count > 0)
            {
                mRetData.CloseoutMsg = dctCriticalErrors.First().Value;
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

                foreach (var criticalError in dctCriticalErrors)
                {
                    LogError("Critical MyEMSL upload error for job " + m_Job + ", status num " + criticalError.Key + ": " + criticalError.Value);
                }
            }

            if (dctUnverifiedURIs.Count > 0 && dctVerifiedURIs.Count > 0)
            {
                CompareUnverifiedAndVerifiedURIs(
                    dctUnverifiedURIs,
                    dctVerifiedURIs,
                    dctStatusData);
            }


            if (dctVerifiedURIs.Count == dctStatusData.Count)
            {
                if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
                {
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                    mRetData.CloseoutMsg = string.Empty;
                }

                return true;
            }

            var firstUnverified = "??";
            if (dctUnverifiedURIs.Count > 0)
            {
                firstUnverified = dctUnverifiedURIs.First().Value;

                // Update Ingest_Steps_Completed in the database for StatusNums that now have more steps completed than tracked by the database
                var statusNumsToUpdate = new List<int>();
                foreach (var statusNum in dctUnverifiedURIs.Keys)
                {
                    clsIngestStatusInfo statusInfo;
                    if (dctStatusData.TryGetValue(statusNum, out statusInfo))
                    {
                        if (statusInfo.IngestStepsCompletedNew > statusInfo.IngestStepsCompletedOld)
                            statusNumsToUpdate.Add(statusNum);
                    }
                }

                if (statusNumsToUpdate.Count > 0)
                {
                    UpdateIngestStepsCompletedInDB(statusNumsToUpdate, dctStatusData);
                }

            }

            if (dctVerifiedURIs.Count == 0)
            {
                msg = "MyEMSL archive status not yet verified; see " + firstUnverified;
            }
            else
                msg = "MyEMSL archive status partially verified (success count = " + dctVerifiedURIs.Count + ", unverified count = " + dctUnverifiedURIs.Count + "); first not verified: " + firstUnverified;

            if (mRetData.EvalCode != EnumEvalCode.EVAL_CODE_FAILURE_DO_NOT_RETRY || string.IsNullOrEmpty(mRetData.CloseoutMsg))
                mRetData.CloseoutMsg = msg;

            LogDebug(msg);
            return true;
        }

        /// <summary>
        /// Validates that MyEMSL knows about each of the items in dctStatusData
        /// </summary>
        /// <param name="statusChecker"></param>
        /// <param name="dctStatusData"></param>
        /// <param name="dctUnverifiedURIs">Number of URIs that were unknown</param>
        /// <param name="dctVerifiedURIs">Number of URIs that properly resolved (not all steps are necessarily complete yet)</param>
        /// <param name="dctCriticalErrors"></param>
        private void CheckStatusURIs(
            MyEMSLStatusCheck statusChecker,
            Dictionary<int, clsIngestStatusInfo> dctStatusData,
            out Dictionary<int, string> dctUnverifiedURIs,
            out Dictionary<int, string> dctVerifiedURIs,
            out Dictionary<int, string> dctCriticalErrors)
        {
            var exceptionCount = 0;

            dctUnverifiedURIs = new Dictionary<int, string>();
            dctVerifiedURIs = new Dictionary<int, string>();
            dctCriticalErrors = new Dictionary<int, string>();

            foreach (var statusDataItem in dctStatusData)
            {
                var statusNum = statusDataItem.Key;
                var statusInfo = statusDataItem.Value;

                try
                {
                    var ingestSuccess = GetMyEMSLIngestStatus(
                        m_Job, statusChecker, statusInfo.StatusURI,
                        statusInfo.EUS_InstrumentID, statusInfo.EUS_ProposalID, statusInfo.EUS_UploaderID,
                        mRetData, out var serverResponse, out int percentComplete);

                    // Convert the percent complete value (between 0 and 100) to a number between 0 and 7
                    // since historically there were 7 steps to the ingest process
                    var ingestStepsCompleted = statusChecker.IngestStepCompletionCount(percentComplete);

                    statusInfo.IngestStepsCompletedNew = ingestStepsCompleted;

                    if (!ingestSuccess)
                    {
                        dctUnverifiedURIs.Add(statusNum, statusInfo.StatusURI);
                        continue;
                    }

                    // We no longer track transaction ID
                    // statusInfo.TransactionId = statusChecker.IngestStepTransactionId(xmlServerResponse);

                    dctVerifiedURIs.Add(statusNum, statusInfo.StatusURI);
                    LogDebug("Successful MyEMSL upload for job " + m_Job + ", status num " + statusNum + ": " + statusInfo.StatusURI);
                    continue;

                }
                catch (Exception ex)
                {
                    exceptionCount++;
                    if (exceptionCount < 3)
                    {
                        LogWarning("Exception verifying archive status for job " + m_Job + ": " + ex.Message);
                    }
                    else
                    {
                        LogError("Exception verifying archive status for job " + m_Job + ": ", ex);
                        break;
                    }
                }

                if (!dctUnverifiedURIs.ContainsKey(statusNum))
                {
                    dctUnverifiedURIs.Add(statusNum, statusInfo.StatusURI);
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
        /// <param name="dctStatusData">Status Info for each StatusNum</param>
        /// <remarks>Will remove superseded (yet unverified) entries from dctUnverifiedURIs and dctStatusData</remarks>
        private void CompareUnverifiedAndVerifiedURIs(
            IDictionary<int, string> dctUnverifiedURIs,
            IReadOnlyDictionary<int, string> dctVerifiedURIs,
            Dictionary<int, clsIngestStatusInfo> dctStatusData)
        {
            var lstStatusNumsToIgnore = new List<int>();

            foreach (var unverifiedEntry in dctUnverifiedURIs)
            {
                var unverifiedStatusNum = unverifiedEntry.Key;

                if (!dctStatusData.TryGetValue(unverifiedStatusNum, out clsIngestStatusInfo unverifiedStatusInfo))
                    continue;

                var unverifiedSubfolder = unverifiedStatusInfo.Subfolder;

                // Find StatusNums that had the same subfolder
                // Note: cannot require that identical matches have a larger StatusNum because sometimes
                // extremely large status values (like 1168231360) are assigned to failed uploads
                var lstIdenticalStatusNums = (from item in dctStatusData
                                              where item.Key != unverifiedStatusNum &&
                                                    item.Value.Subfolder == unverifiedSubfolder
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

                // Set the ErrorCode to 101 in T_MyEMSL_Uploads
                UpdateSupersededURIs(lstStatusNumsToIgnore, dctStatusData);

                // Update the dictionaries
                foreach (var statusNumToRemove in lstStatusNumsToIgnore)
                {
                    dctUnverifiedURIs.Remove(statusNumToRemove);
                    dctStatusData.Remove(statusNumToRemove);
                }

            }

        }

        /// <summary>
        /// For the given list of status numbers, looks up the maximum value for IngestStepsCompleted in dctStatusData
        /// </summary>
        /// <param name="statusNums"></param>
        /// <param name="dctStatusData"></param>
        /// <returns></returns>
        private byte GetMaxIngestStepCompleted(IEnumerable<int> statusNums, IReadOnlyDictionary<int, clsIngestStatusInfo> dctStatusData)
        {
            byte ingestStepsCompleted = 0;
            foreach (var statusNum in statusNums)
            {
                if (dctStatusData.TryGetValue(statusNum, out clsIngestStatusInfo statusInfo))
                {
                    ingestStepsCompleted = Math.Max(ingestStepsCompleted, statusInfo.IngestStepsCompletedNew);
                }
            }

            return ingestStepsCompleted;
        }

        private Dictionary<int, clsIngestStatusInfo> GetStatusURIs(int retryCount = 2)
        {
            // Keys in this dictionary are StatusNum integers
            var dctStatusData = new Dictionary<int, clsIngestStatusInfo>();

            // First look for a specific Status_URI for this joB
            // Only DatasetArchive or ArchiveUpdate jobs will have this job parameter
            // MyEMSLVerify will not have this parameter
            var statusURI = m_TaskParams.GetParam("MyEMSL_Status_URI", string.Empty);

            // Note that GetStatusURIsAndSubfolders requires that the column order be StatusNum, Status_URI, Subfolder, Ingest_Steps_Completed, EUS_InstrumentID, EUS_ProposalID, EUS_UploaderID, ErrorCode
            var sql =
                " SELECT StatusNum, Status_URI, Subfolder, " +
                       " IsNull(Ingest_Steps_Completed, 0) AS Ingest_Steps_Completed, " +
                       " EUS_InstrumentID, EUS_ProposalID, EUS_UploaderID, ErrorCode" +
                " FROM V_MyEMSL_Uploads " +
                " WHERE Dataset_ID = " + m_DatasetID;

            if (!string.IsNullOrEmpty(statusURI))
            {
                var statusNum = MyEMSLStatusCheck.GetStatusNumFromURI(statusURI);

                dctStatusData.Add(statusNum, new clsIngestStatusInfo(statusNum, statusURI));

                sql += " AND StatusNum = " + statusNum +
                       " ORDER BY Entry_ID";

                GetStatusURIsAndSubfolders(sql, dctStatusData, retryCount);

                if (dctStatusData.First().Value.ExistingErrorCode == -1 ||
                    dctStatusData.First().Value.ExistingErrorCode == 101)
                {
                    // The verification of this step has already been manually skipped (an admin set the ErrorCode to -1 or 101)
                    // Return an empty dictioary
                    return new Dictionary<int, clsIngestStatusInfo>();
                }

                return dctStatusData;
            }

            try
            {
                sql += " AND Job <= " + m_Job +
                       " AND ISNULL(StatusNum, 0) > 0 " +
                       " AND ErrorCode NOT IN (-1, 101)" +
                       " ORDER BY Entry_ID";

                GetStatusURIsAndSubfolders(sql, dctStatusData, retryCount);

            }
            catch (Exception ex)
            {
                var msg = "Exception connecting to database for job " + m_Job + ": " + ex.Message;
                LogError(msg);
            }

            return dctStatusData;
        }

        /// <summary>
        /// Run a query against V_MyEMSL_Uploads
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="dctStatusData"></param>
        /// <param name="retryCount"></param>
        private void GetStatusURIsAndSubfolders(string sql, IDictionary<int, clsIngestStatusInfo> dctStatusData, int retryCount = 2)
        {
            // This Connection String points to the DMS_Capture database
            var connectionString = m_MgrParams.GetParam("connectionstring");

            while (retryCount >= 0)
            {
                try
                {
                    using (var cnDB = new SqlConnection(connectionString))
                    {
                        cnDB.Open();

                        var cmd = new SqlCommand(sql, cnDB);
                        var reader = cmd.ExecuteReader();

                        // Expected fields:
                        // StatusNum, Status_URI, Subfolder, Ingest_Steps_Completed, EUS_InstrumentID, EUS_ProposalID, EUS_UploaderID, ErrorCode
                        while (reader.Read())
                        {
                            var statusNum = reader.GetInt32(0);

                            if (Convert.IsDBNull(reader.GetValue(1)))
                            {
                                continue;
                            }

                            var uri = (string)reader.GetValue(1);
                            if (string.IsNullOrEmpty(uri))
                            {
                                continue;
                            }

                            var subFolder = (string)reader.GetValue(2);
                            var ingestStepsCompleted = (byte)reader.GetValue(3);

                            var eusInstrumentID = clsConversion.GetDbValue(reader, 4, 0);
                            var eusProposalID = clsConversion.GetDbValue(reader, 5, string.Empty);
                            var eusUploaderID = clsConversion.GetDbValue(reader, 6, 0);
                            var errorCode = clsConversion.GetDbValue(reader, 7, 0);

                            if (!dctStatusData.TryGetValue(statusNum, out clsIngestStatusInfo statusInfo))
                            {
                                statusInfo = new clsIngestStatusInfo(statusNum, uri);
                                dctStatusData.Add(statusNum, statusInfo);
                            }

                            statusInfo.Subfolder = subFolder;
                            statusInfo.IngestStepsCompletedOld = ingestStepsCompleted;
                            statusInfo.EUS_InstrumentID = eusInstrumentID;
                            statusInfo.EUS_ProposalID = eusProposalID;
                            statusInfo.EUS_UploaderID = eusUploaderID;
                            statusInfo.ExistingErrorCode = errorCode;

                        }
                    }
                    return;

                }
                catch (Exception ex)
                {
                    retryCount -= 1;

                    var msg = string.Format("GetStatusURIs; Exception querying database for job {0}: {1}; " +
                                            "ConnectionString: {2}, RetryCount = {3}",
                                            m_Job, ex.Message, connectionString, retryCount);
                    LogError(msg);

                    // Delay for 5 seconds before trying again
                    if (retryCount >= 0)
                        System.Threading.Thread.Sleep(5000);
                }

            } // while

        }

        void statusChecker_ErrorEvent(object sender, MessageEventArgs e)
        {
            LogError("Status checker error for job " + m_Job + ": " + e.Message);
        }

        /// <summary>
        /// Update Ingest_Steps_Completed and Error_Code in T_MyEMSL_Uploads for all tasks in dctStatusData
        /// </summary>
        /// <param name="statusNumsToUpdate"></param>
        /// <param name="dctStatusData"></param>
        /// <returns></returns>
        private bool UpdateIngestStepsCompletedInDB(
            IEnumerable<int> statusNumsToUpdate,
            IReadOnlyDictionary<int, clsIngestStatusInfo> dctStatusData)
        {
            try
            {
                var spError = false;

                foreach (var statusNum in statusNumsToUpdate)
                {

                    if (!dctStatusData.TryGetValue(statusNum, out clsIngestStatusInfo statusInfo))
                    {
                        continue;
                    }

                    var success = UpdateIngestStepsCompletedOneTask(
                        statusNum,
                        statusInfo.IngestStepsCompletedNew,
                        statusInfo.TransactionId);

                    if (!success)
                        spError = true;

                }

                if (spError)
                {
                    // One or more calls to the stored procedure failed
                    return false;
                }

                return true;

            }
            catch (Exception ex)
            {
                var msg = "Exception calling stored procedure SetMyEMSLUploadSupersededIfFailed, job " + m_Job;
                LogError(msg, ex);
                return false;
            }

        }

        private bool UpdateSupersededURIs(
            IReadOnlyCollection<int> lstStatusNumsToIgnore,
            IReadOnlyDictionary<int, clsIngestStatusInfo> dctStatusData)
        {
            const string SP_NAME = "SetMyEMSLUploadSupersededIfFailed";

            try
            {
                var statusNums = string.Join(",", lstStatusNumsToIgnore);

                var ingestStepsCompleted = GetMaxIngestStepCompleted(lstStatusNumsToIgnore, dctStatusData);

                var spCmd = new SqlCommand(SP_NAME)
                {
                    CommandType = CommandType.StoredProcedure
                };

                spCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;

                spCmd.Parameters.Add("@DatasetID", SqlDbType.Int).Value = m_DatasetID;


                spCmd.Parameters.Add("@statusNumList", SqlDbType.VarChar, 1024).Value = statusNums;

                spCmd.Parameters.Add("@ingestStepsCompleted", SqlDbType.TinyInt).Value = ingestStepsCompleted;

                spCmd.Parameters.Add("@message", SqlDbType.VarChar, 512).Direction = ParameterDirection.Output;

                var resCode = CaptureDBProcedureExecutor.ExecuteSP(spCmd, 2);

                if (resCode == 0)
                    return true;

                var msg = "Error " + resCode + " calling stored procedure " + SP_NAME + ", job " + m_Job;
                LogError(msg);
                return false;
            }
            catch (Exception ex)
            {
                var msg = "Exception calling stored procedure " + SP_NAME + ", job " + m_Job;
                LogError(msg, ex);
                return false;
            }

        }

        private bool UpdateVerifiedURIs(
            Dictionary<int, string> dctVerifiedURIs,
            IReadOnlyDictionary<int, clsIngestStatusInfo> dctStatusData)
        {
            const string SP_NAME = "SetMyEMSLUploadVerified";

            try
            {
                var verifiedStatusNums = dctVerifiedURIs.Keys;

                var statusNums = string.Join(",", verifiedStatusNums);

                var ingestStepsCompleted = GetMaxIngestStepCompleted(verifiedStatusNums, dctStatusData);

                var spCmd = new SqlCommand(SP_NAME)
                {
                    CommandType = CommandType.StoredProcedure
                };

                spCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;

                spCmd.Parameters.Add("@datasetID", SqlDbType.Int).Value = m_DatasetID;

                spCmd.Parameters.Add("@statusNumList", SqlDbType.VarChar, 1024).Value = statusNums;

                spCmd.Parameters.Add("@ingestStepsCompleted", SqlDbType.TinyInt).Value = ingestStepsCompleted;

                spCmd.Parameters.Add("@message", SqlDbType.VarChar, 512).Direction = ParameterDirection.Output;

                var resCode = CaptureDBProcedureExecutor.ExecuteSP(spCmd, 2);

                if (resCode == 0)
                    return true;

                var msg = "Error " + resCode + " calling stored procedure " + SP_NAME + ", job " + m_Job;
                LogError(msg);
                return false;
            }
            catch (Exception ex)
            {
                var msg = "Exception calling stored procedure " + SP_NAME + ", job " + m_Job;
                LogError(msg, ex);
                return false;
            }

        }

        #endregion


    }

}
