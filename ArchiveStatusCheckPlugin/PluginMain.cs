using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using CaptureTaskManager;
using Pacifica.Core;
using PRISMDatabaseUtils;

namespace ArchiveStatusCheckPlugin
{
    /// <summary>
    /// Archive status check plugin
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class PluginMain : ToolRunnerBase
    {
        // Ignore Spelling: num, subfolder, Status_Nums, UploaderID

        private ToolReturnData mRetData = new();

        /// <summary>
        /// Runs the Archive Status Check step tool
        /// </summary>
        /// <returns>Class with completionCode, completionMessage, evaluationCode, and evaluationMessage</returns>
        public override ToolReturnData RunTool()
        {
            LogDebug("Starting ArchiveStatusCheckPlugin.PluginMain.RunTool()");

            // Perform base class operations, if any
            mRetData = base.RunTool();
            if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
            {
                return mRetData;
            }

            if (mDebugLevel >= 5)
            {
                LogMessage("Verifying status of files in MyEMSL for dataset " + mDataset);
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
                LogError("Exception checking archive status for job " + mJob, ex);
            }

            if (success)
            {
                // Everything was good
                if (mDebugLevel >= 4)
                {
                    LogMessage("MyEMSL status verification successful for dataset " + mDataset);
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
                {
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_NOT_READY;
                }
            }

            LogDebug("Completed PluginMain.RunTool()");

            return mRetData;
        }

        private bool CheckArchiveStatus()
        {
            // Examine the upload status for any uploads for this dataset, filtering on job number to ignore jobs created after this job
            // First obtain a list of status URIs to check

            // Keys in statusData are Status_Num integers, values are instances of class IngestStatusInfo
            var statusData = GetStatusURIs();

            string msg;

            if (statusData.Count == 0)
            {
                msg = "Could not find any MyEMSL_Status_URIs; cannot verify archive status. " +
                      "If all entries for Dataset " + mDatasetID + " have ErrorCode -1 or 101 this job step should be manually skipped";

                mRetData.CloseoutMsg = msg;
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;
                LogError(msg + " for job " + mJob);
                return false;
            }

            var statusChecker = new MyEMSLStatusCheck();
            RegisterEvents(statusChecker);

            // Check the status of each of the URIs
            // Keys in unverifiedURIs and verifiedURIs are Status_Num; values are Status_URI strings
            // Keys in criticalErrors are Status_Num; values are critical error messages

            CheckStatusURIs(statusChecker, statusData,
                out var unverifiedURIs, out var verifiedURIs, out var criticalErrors);

            if (verifiedURIs.Count > 0)
            {
                // Update the Verified flag in T_MyEMSL_Uploads
                UpdateVerifiedURIs(verifiedURIs, statusData);
            }

            if (criticalErrors.Count > 0)
            {
                mRetData.CloseoutMsg = criticalErrors.First().Value;
                mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_FAILED;

                foreach (var criticalError in criticalErrors)
                {
                    LogError("Critical MyEMSL upload error for job " + mJob + ", status num " + criticalError.Key + ": " + criticalError.Value);
                }
            }

            if (unverifiedURIs.Count > 0 && verifiedURIs.Count > 0)
            {
                CompareUnverifiedAndVerifiedURIs(
                    unverifiedURIs,
                    verifiedURIs,
                    statusData);
            }

            if (verifiedURIs.Count == statusData.Count)
            {
                if (mRetData.CloseoutType == EnumCloseOutType.CLOSEOUT_FAILED)
                {
                    mRetData.CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                    mRetData.CloseoutMsg = string.Empty;
                }

                return true;
            }

            var firstUnverified = "??";
            if (unverifiedURIs.Count > 0)
            {
                firstUnverified = unverifiedURIs.First().Value;

                // Update Ingest_Steps_Completed in the database for Status_Nums that now have more steps completed than tracked by the database
                var statusNumsToUpdate = new List<int>();

                foreach (var statusNum in unverifiedURIs.Keys)
                {
                    if (statusData.TryGetValue(statusNum, out var statusInfo) &&
                        statusInfo.IngestStepsCompletedNew > statusInfo.IngestStepsCompletedOld)
                    {
                        statusNumsToUpdate.Add(statusNum);
                    }
                }

                if (statusNumsToUpdate.Count > 0)
                {
                    UpdateIngestStepsCompletedInDB(statusNumsToUpdate, statusData);
                }
            }

            if (verifiedURIs.Count == 0)
            {
                msg = "MyEMSL archive status not yet verified; see " + firstUnverified;
            }
            else
            {
                msg = "MyEMSL archive status partially verified (success count = " + verifiedURIs.Count + ", unverified count = " + unverifiedURIs.Count + "); first not verified: " + firstUnverified;
            }

            if (mRetData.EvalCode != EnumEvalCode.EVAL_CODE_FAILURE_DO_NOT_RETRY || string.IsNullOrEmpty(mRetData.CloseoutMsg))
            {
                mRetData.CloseoutMsg = msg;
            }

            LogDebug(msg);
            return true;
        }

        /// <summary>
        /// Validates that MyEMSL knows about each of the items in statusData
        /// </summary>
        /// <param name="statusChecker"></param>
        /// <param name="statusData"></param>
        /// <param name="unverifiedURIs">Number of URIs that were unknown</param>
        /// <param name="verifiedURIs">Number of URIs that properly resolved (not all steps are necessarily complete yet)</param>
        /// <param name="criticalErrors"></param>
        private void CheckStatusURIs(
            MyEMSLStatusCheck statusChecker,
            Dictionary<int, IngestStatusInfo> statusData,
            out Dictionary<int, string> unverifiedURIs,
            out Dictionary<int, string> verifiedURIs,
            out Dictionary<int, string> criticalErrors)
        {
            var exceptionCount = 0;

            unverifiedURIs = new Dictionary<int, string>();
            verifiedURIs = new Dictionary<int, string>();
            criticalErrors = new Dictionary<int, string>();

            foreach (var statusDataItem in statusData)
            {
                var statusNum = statusDataItem.Key;
                var statusInfo = statusDataItem.Value;

                try
                {
                    var ingestSuccess = GetMyEMSLIngestStatus(
                        mJob, statusChecker, statusInfo.StatusURI,
                        mRetData, out _, out var currentTask, out var percentComplete);

                    statusInfo.IngestStepsCompletedNew = statusChecker.DetermineIngestStepsCompleted(currentTask, percentComplete, statusInfo.IngestStepsCompletedOld);

                    if (!ingestSuccess)
                    {
                        unverifiedURIs.Add(statusNum, statusInfo.StatusURI);
                        continue;
                    }

                    // We no longer track transaction ID
                    // statusInfo.TransactionId = statusChecker.IngestStepTransactionId(xmlServerResponse);

                    verifiedURIs.Add(statusNum, statusInfo.StatusURI);
                    LogDebug("Successful MyEMSL upload for job " + mJob + ", status num " + statusNum + ": " + statusInfo.StatusURI);
                    continue;
                }
                catch (Exception ex)
                {
                    exceptionCount++;
                    if (exceptionCount < 3)
                    {
                        LogWarning("Exception verifying archive status for job " + mJob + ": " + ex.Message);
                    }
                    else
                    {
                        LogError("Exception verifying archive status for job " + mJob + ": ", ex);
                        break;
                    }
                }

                if (!unverifiedURIs.ContainsKey(statusNum))
                {
                    unverifiedURIs.Add(statusNum, statusInfo.StatusURI);
                }

                exceptionCount = 0;
            }
        }

        /// <summary>
        /// Step through the unverified URIs to see if the same subdirectory was subsequently successfully uploaded
        /// (could be a blank subdirectory, meaning the instrument data and all jobs)
        /// </summary>
        /// <remarks>Will remove superseded (yet unverified) entries from unverifiedURIs and statusData</remarks>
        /// <param name="unverifiedURIs">Unverified URIs</param>
        /// <param name="verifiedURIs">Verified URIs</param>
        /// <param name="statusData">Status Info for each Status_Num</param>
        private void CompareUnverifiedAndVerifiedURIs(
            IDictionary<int, string> unverifiedURIs,
            IReadOnlyDictionary<int, string> verifiedURIs,
            Dictionary<int, IngestStatusInfo> statusData)
        {
            var statusNumsToIgnore = new List<int>();

            foreach (var unverifiedEntry in unverifiedURIs)
            {
                var unverifiedStatusNum = unverifiedEntry.Key;

                if (!statusData.TryGetValue(unverifiedStatusNum, out var unverifiedStatusInfo))
                {
                    continue;
                }

                var unverifiedSubfolder = unverifiedStatusInfo.Subdirectory;

                // Find Status_Nums that had the same subdirectory
                // Note: cannot require that identical matches have a larger Status_Num because sometimes
                // extremely large status values (like 1168231360) are assigned to failed uploads
                var identicalStatusNums = (
                    from item in statusData
                    where item.Key != unverifiedStatusNum && item.Value.Subdirectory == unverifiedSubfolder
                    select item.Key).ToList();

                if (identicalStatusNums.Count == 0)
                {
                    continue;
                }

                // Check if any of the identical entries has been successfully verified
                foreach (var identicalStatusNum in identicalStatusNums)
                {
                    if (!verifiedURIs.ContainsKey(identicalStatusNum))
                        continue;

                    statusNumsToIgnore.Add(unverifiedStatusNum);
                    break;
                }
            }

            if (statusNumsToIgnore.Count == 0)
                return;

            // Found some URIs that we can ignore

            // Set the ErrorCode to 101 in T_MyEMSL_Uploads
            UpdateSupersededURIs(statusNumsToIgnore, statusData);

            // Update the dictionaries
            foreach (var statusNumToRemove in statusNumsToIgnore)
            {
                unverifiedURIs.Remove(statusNumToRemove);
                statusData.Remove(statusNumToRemove);
            }
        }

        /// <summary>
        /// For the given list of status numbers, looks up the maximum value for IngestStepsCompleted in statusData
        /// </summary>
        /// <param name="statusNums"></param>
        /// <param name="statusData"></param>
        /// <returns>Number of steps that have been completed</returns>
        private byte GetMaxIngestStepCompleted(IEnumerable<int> statusNums, IReadOnlyDictionary<int, IngestStatusInfo> statusData)
        {
            byte ingestStepsCompleted = 0;
            foreach (var statusNum in statusNums)
            {
                if (statusData.TryGetValue(statusNum, out var statusInfo))
                {
                    ingestStepsCompleted = Math.Max(ingestStepsCompleted, statusInfo.IngestStepsCompletedNew);
                }
            }

            return ingestStepsCompleted;
        }

        private Dictionary<int, IngestStatusInfo> GetStatusURIs(int retryCount = 2)
        {
            // Keys in this dictionary are Status_Num integers
            var statusData = new Dictionary<int, IngestStatusInfo>();

            // First look for a specific Status_URI for this job
            // Only DatasetArchive or ArchiveUpdate jobs will have this job parameter
            // MyEMSLVerify will not have this parameter
            var statusURI = mTaskParams.GetParam("MyEMSL_Status_URI", string.Empty);

            // Note that GetStatusURIsAndSubdirectories requires that the column order be
            // Status_Num, Status_URI, Subfolder, Ingest_Steps_Completed, EUS_Instrument_ID, EUS_Proposal_ID, EUS_Uploader_ID, Error_Code

            var sql = new StringBuilder();
            sql.AppendFormat(
                " SELECT status_num, status_uri, subfolder, " +
                       " Coalesce(ingest_steps_completed, 0) AS ingest_steps_completed, " +
                       " eus_instrument_id, eus_proposal_id, eus_uploader_id, error_code" +
                " FROM " + mMgrParams.DMSCaptureSchema + "V_MyEMSL_Uploads " +
                " WHERE dataset_id = {0}", mDatasetID);

            if (!string.IsNullOrEmpty(statusURI))
            {
                var statusNum = MyEMSLStatusCheck.GetStatusNumFromURI(statusURI);

                statusData.Add(statusNum, new IngestStatusInfo(statusNum, statusURI));

                sql.AppendFormat(" AND status_num = {0} ORDER BY entry_id", statusNum);

                GetStatusURIsAndSubdirectories(sql.ToString(), statusData, retryCount);

                if (statusData.First().Value.ExistingErrorCode == -1 ||
                    statusData.First().Value.ExistingErrorCode == 101)
                {
                    // The verification of this step has already been manually skipped (an admin set the ErrorCode to -1 or 101)
                    // Return an empty dictionary
                    return new Dictionary<int, IngestStatusInfo>();
                }

                return statusData;
            }

            try
            {
                sql.AppendFormat(
                    " AND job <= {0}"  +
                    " AND Coalesce(status_num, 0) > 0 " +
                    " AND error_code NOT IN (-1, 101)" +
                    " ORDER BY entry_id", mJob);

                GetStatusURIsAndSubdirectories(sql.ToString(), statusData, retryCount);
            }
            catch (Exception ex)
            {
                LogError("Exception connecting to database for job {0}: {1}", mJob, ex.Message);
            }

            return statusData;
        }

        /// <summary>
        /// Run a query against V_MyEMSL_Uploads
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="statusData"></param>
        /// <param name="retryCount"></param>
        private void GetStatusURIsAndSubdirectories(string sql, IDictionary<int, IngestStatusInfo> statusData, int retryCount = 2)
        {
            // This connection string points to the DMS_Capture database
            var connectionString = mMgrParams.GetParam("ConnectionString");

            var applicationName = string.Format("{0}_ArchiveStatus", mMgrParams.ManagerName);

            var connectionStringToUse = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, applicationName);

            var dbTools = DbToolsFactory.GetDBTools(connectionStringToUse, debugMode: mMgrParams.TraceMode);
            RegisterEvents(dbTools);

            dbTools.GetQueryResultsDataTable(sql, out var table, retryCount, 5);

            // Expected fields:
            // Status_Num, Status_URI, Subfolder, Ingest_Steps_Completed, EUS_Instrument_ID, EUS_Proposal_ID, EUS_Uploader_ID, Error_Code
            foreach (DataRow row in table.Rows)
            {
                var statusNum = row[0].CastDBVal<int>();

                var uri = row[1].CastDBVal(string.Empty);
                if (string.IsNullOrEmpty(uri))
                {
                    continue;
                }

                var subdirectory = row[2].CastDBVal<string>();
                var ingestStepsCompleted = row[3].CastDBVal<byte>();

                var eusInstrumentID = row[4].CastDBVal(0);
                var eusProjectID = row[5].CastDBVal(string.Empty);
                var eusUploaderID = row[6].CastDBVal(0);
                var errorCode = row[7].CastDBVal(0);

                if (!statusData.TryGetValue(statusNum, out var statusInfo))
                {
                    statusInfo = new IngestStatusInfo(statusNum, uri);
                    statusData.Add(statusNum, statusInfo);
                }

                statusInfo.Subdirectory = subdirectory;
                statusInfo.IngestStepsCompletedOld = ingestStepsCompleted;
                statusInfo.EUS_InstrumentID = eusInstrumentID;
                statusInfo.EUS_ProjectID = eusProjectID;
                statusInfo.EUS_UploaderID = eusUploaderID;
                statusInfo.ExistingErrorCode = errorCode;
            }
        }

        /// <summary>
        /// Update Ingest_Steps_Completed and Error_Code in T_MyEMSL_Uploads for all tasks in statusData
        /// </summary>
        /// <param name="statusNumsToUpdate"></param>
        /// <param name="statusData"></param>
        private void UpdateIngestStepsCompletedInDB(IEnumerable<int> statusNumsToUpdate, IReadOnlyDictionary<int, IngestStatusInfo> statusData)
        {
            try
            {
                var spError = false;

                foreach (var statusNum in statusNumsToUpdate)
                {
                    if (!statusData.TryGetValue(statusNum, out var statusInfo))
                    {
                        continue;
                    }

                    var success = UpdateIngestStepsCompletedOneTask(
                        statusNum,
                        statusInfo.IngestStepsCompletedNew);

                    if (!success)
                    {
                        spError = true;
                    }
                }

                if (spError)
                {
                    // One or more calls to the stored procedure failed
                }
            }
            catch (Exception ex)
            {
                LogError("Exception calling stored procedure set_myemsl_upload_superseded_if_failed, job " + mJob, ex);
            }
        }

        private void UpdateSupersededURIs(IReadOnlyCollection<int> statusNumsToIgnore, IReadOnlyDictionary<int, IngestStatusInfo> statusData)
        {
            const string SP_NAME = "set_myemsl_upload_superseded_if_failed";

            try
            {
                var statusNums = string.Join(",", statusNumsToIgnore);

                var ingestStepsCompleted = GetMaxIngestStepCompleted(statusNumsToIgnore, statusData);

                var dbTools = mCaptureDbProcedureExecutor;
                var cmd = dbTools.CreateCommand(SP_NAME, CommandType.StoredProcedure);

                dbTools.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);
                dbTools.AddParameter(cmd, "@datasetID", SqlType.Int).Value = mDatasetID;
                dbTools.AddParameter(cmd, "@statusNumList", SqlType.VarChar, 1024, statusNums);
                dbTools.AddParameter(cmd, "@ingestStepsCompleted", SqlType.TinyInt).Value = ingestStepsCompleted;
                dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.InputOutput);

                var resCode = mCaptureDbProcedureExecutor.ExecuteSP(cmd, 2);

                if (resCode == 0)
                {
                    return;
                }

                LogError("Error {0} calling stored procedure {1}, job {2}", resCode, SP_NAME, mJob);
            }
            catch (Exception ex)
            {
                LogError(ex, "Exception calling stored procedure {0}, job {1}", SP_NAME, mJob);
            }
        }

        private void UpdateVerifiedURIs(Dictionary<int, string> verifiedURIs, IReadOnlyDictionary<int, IngestStatusInfo> statusData)
        {
            const string SP_NAME = "set_myemsl_upload_verified";

            try
            {
                var verifiedStatusNums = verifiedURIs.Keys;
                var verifiedStatusURIs = verifiedURIs.Values;

                var statusNums = string.Join(", ", verifiedStatusNums);

                var statusURIs = string.Join(", ", verifiedStatusURIs);

                var ingestStepsCompleted = GetMaxIngestStepCompleted(verifiedStatusNums, statusData);

                var dbTools = mCaptureDbProcedureExecutor;
                var cmd = dbTools.CreateCommand(SP_NAME, CommandType.StoredProcedure);

                dbTools.AddParameter(cmd, "@Return", SqlType.Int, ParameterDirection.ReturnValue);
                dbTools.AddParameter(cmd, "@datasetID", SqlType.Int).Value = mDatasetID;
                dbTools.AddParameter(cmd, "@statusNumList", SqlType.VarChar, 1024, statusNums);
                dbTools.AddParameter(cmd, "@statusURIList", SqlType.VarChar, 4000, statusURIs);
                dbTools.AddParameter(cmd, "@ingestStepsCompleted", SqlType.TinyInt).Value = ingestStepsCompleted;
                dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.InputOutput);

                var resCode = mCaptureDbProcedureExecutor.ExecuteSP(cmd, 2);

                if (resCode == 0)
                {
                    return;
                }

                LogError("Error {0} calling stored procedure {1}, job {2}", resCode, SP_NAME, mJob);
            }
            catch (Exception ex)
            {
                LogError(ex, "Exception calling stored procedure {0}, job {1}", SP_NAME, mJob);
            }
        }
    }
}
