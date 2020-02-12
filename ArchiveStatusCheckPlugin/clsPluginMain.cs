using CaptureTaskManager;
using Pacifica.Core;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using PRISMDatabaseUtils;

namespace ArchiveStatusCheckPlugin
{
    /// <summary>
    /// Archive status check plugin
    /// </summary>
    // ReSharper disable once UnusedMember.Global
    public class clsPluginMain : clsToolRunnerBase
    {

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

            if (mDebugLevel >= 5)
            {
                msg = "Verifying status of files in MyEMSL for dataset '" + mDataset + "'";
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
                msg = "Exception checking archive status for job " + mJob;
                LogError(msg, ex);
            }

            if (success)
            {
                // Everything was good
                if (mDebugLevel >= 4)
                {
                    msg = "MyEMSL status verification successful for dataset " + mDataset;
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

            string msg;

            if (dctStatusData.Count == 0)
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
            // Keys in dctUnverifiedURIs and dctVerifiedURIs are StatusNum; values are StatusURI strings
            // Keys in dctCriticalErrors are StatusNum; values are critical error messages

            CheckStatusURIs(statusChecker, dctStatusData,
                out var dctUnverifiedURIs, out var dctVerifiedURIs, out var dctCriticalErrors);

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
                    LogError("Critical MyEMSL upload error for job " + mJob + ", status num " + criticalError.Key + ": " + criticalError.Value);
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
                    if (dctStatusData.TryGetValue(statusNum, out var statusInfo))
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
                        mJob, statusChecker, statusInfo.StatusURI,
                        mRetData, out _, out var currentTask, out var percentComplete);

                    var ingestStepsCompleted = statusChecker.DetermineIngestStepsCompleted(currentTask, percentComplete, statusInfo.IngestStepsCompletedOld);

                    statusInfo.IngestStepsCompletedNew = ingestStepsCompleted;

                    if (!ingestSuccess)
                    {
                        dctUnverifiedURIs.Add(statusNum, statusInfo.StatusURI);
                        continue;
                    }

                    // We no longer track transaction ID
                    // statusInfo.TransactionId = statusChecker.IngestStepTransactionId(xmlServerResponse);

                    dctVerifiedURIs.Add(statusNum, statusInfo.StatusURI);
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

                if (!dctStatusData.TryGetValue(unverifiedStatusNum, out var unverifiedStatusInfo))
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
                if (dctStatusData.TryGetValue(statusNum, out var statusInfo))
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

            // First look for a specific Status_URI for this job
            // Only DatasetArchive or ArchiveUpdate jobs will have this job parameter
            // MyEMSLVerify will not have this parameter
            var statusURI = mTaskParams.GetParam("MyEMSL_Status_URI", string.Empty);

            // Note that GetStatusURIsAndSubfolders requires that the column order be StatusNum, Status_URI, Subfolder, Ingest_Steps_Completed, EUS_InstrumentID, EUS_ProposalID, EUS_UploaderID, ErrorCode
            var sql =
                " SELECT StatusNum, Status_URI, Subfolder, " +
                       " IsNull(Ingest_Steps_Completed, 0) AS Ingest_Steps_Completed, " +
                       " EUS_InstrumentID, EUS_ProposalID, EUS_UploaderID, ErrorCode" +
                " FROM V_MyEMSL_Uploads " +
                " WHERE Dataset_ID = " + mDatasetID;

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
                    // Return an empty dictionary
                    return new Dictionary<int, clsIngestStatusInfo>();
                }

                return dctStatusData;
            }

            try
            {
                sql += " AND Job <= " + mJob +
                       " AND ISNULL(StatusNum, 0) > 0 " +
                       " AND ErrorCode NOT IN (-1, 101)" +
                       " ORDER BY Entry_ID";

                GetStatusURIsAndSubfolders(sql, dctStatusData, retryCount);

            }
            catch (Exception ex)
            {
                var msg = "Exception connecting to database for job " + mJob + ": " + ex.Message;
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
            var connectionString = mMgrParams.GetParam("ConnectionString");

            var dbTools = DbToolsFactory.GetDBTools(connectionString);
            dbTools.ErrorEvent += LogError;

            var success = dbTools.GetQueryResultsDataTable(sql, out var table, retryCount, 5);

            // Expected fields:
            // StatusNum, Status_URI, Subfolder, Ingest_Steps_Completed, EUS_InstrumentID, EUS_ProposalID, EUS_UploaderID, ErrorCode
            foreach (DataRow row in table.Rows)
            {
                var statusNum = row[0].CastDBVal<int>();

                var uri = row[1].CastDBVal(string.Empty);
                if (string.IsNullOrEmpty(uri))
                {
                    continue;
                }

                var subFolder = row[2].CastDBVal<string>();
                var ingestStepsCompleted = row[3].CastDBVal<byte>();

                var eusInstrumentID = row[4].CastDBVal(0);
                var eusProjectID = row[5].CastDBVal(string.Empty);
                var eusUploaderID = row[6].CastDBVal(0);
                var errorCode = row[7].CastDBVal(0);

                if (!dctStatusData.TryGetValue(statusNum, out var statusInfo))
                {
                    statusInfo = new clsIngestStatusInfo(statusNum, uri);
                    dctStatusData.Add(statusNum, statusInfo);
                }

                statusInfo.Subfolder = subFolder;
                statusInfo.IngestStepsCompletedOld = ingestStepsCompleted;
                statusInfo.EUS_InstrumentID = eusInstrumentID;
                statusInfo.EUS_ProjectID = eusProjectID;
                statusInfo.EUS_UploaderID = eusUploaderID;
                statusInfo.ExistingErrorCode = errorCode;
            }
        }

        /// <summary>
        /// Update Ingest_Steps_Completed and Error_Code in T_MyEMSL_Uploads for all tasks in dctStatusData
        /// </summary>
        /// <param name="statusNumsToUpdate"></param>
        /// <param name="dctStatusData"></param>
        /// <returns></returns>
        private void UpdateIngestStepsCompletedInDB(IEnumerable<int> statusNumsToUpdate, IReadOnlyDictionary<int, clsIngestStatusInfo> dctStatusData)
        {
            try
            {
                var spError = false;

                foreach (var statusNum in statusNumsToUpdate)
                {

                    if (!dctStatusData.TryGetValue(statusNum, out var statusInfo))
                    {
                        continue;
                    }

                    var success = UpdateIngestStepsCompletedOneTask(
                        statusNum,
                        statusInfo.IngestStepsCompletedNew);

                    if (!success)
                        spError = true;

                }

                if (spError)
                {
                    // One or more calls to the stored procedure failed
                }

            }
            catch (Exception ex)
            {
                var msg = "Exception calling stored procedure SetMyEMSLUploadSupersededIfFailed, job " + mJob;
                LogError(msg, ex);
            }

        }

        private void UpdateSupersededURIs(IReadOnlyCollection<int> lstStatusNumsToIgnore, IReadOnlyDictionary<int, clsIngestStatusInfo> dctStatusData)
        {
            const string SP_NAME = "SetMyEMSLUploadSupersededIfFailed";

            try
            {
                var statusNums = string.Join(",", lstStatusNumsToIgnore);

                var ingestStepsCompleted = GetMaxIngestStepCompleted(lstStatusNumsToIgnore, dctStatusData);

                var dbTools = mCaptureDbProcedureExecutor;
                var cmd = dbTools.CreateCommand(SP_NAME, CommandType.StoredProcedure);

                dbTools.AddParameter(cmd, "@Return", SqlType.Int, direction: ParameterDirection.ReturnValue);
                dbTools.AddParameter(cmd, "@DatasetID", SqlType.Int, value: mDatasetID);
                dbTools.AddParameter(cmd, "@statusNumList", SqlType.VarChar, 1024, statusNums);
                dbTools.AddParameter(cmd, "@ingestStepsCompleted", SqlType.TinyInt, value: ingestStepsCompleted);
                dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, direction: ParameterDirection.Output);

                var resCode = mCaptureDbProcedureExecutor.ExecuteSP(cmd, 2);

                if (resCode == 0)
                    return;

                var msg = "Error " + resCode + " calling stored procedure " + SP_NAME + ", job " + mJob;
                LogError(msg);
            }
            catch (Exception ex)
            {
                var msg = "Exception calling stored procedure " + SP_NAME + ", job " + mJob;
                LogError(msg, ex);
            }

        }

        private void UpdateVerifiedURIs(Dictionary<int, string> dctVerifiedURIs, IReadOnlyDictionary<int, clsIngestStatusInfo> dctStatusData)
        {
            const string SP_NAME = "SetMyEMSLUploadVerified";

            try
            {
                var verifiedStatusNums = dctVerifiedURIs.Keys;
                var verifiedStatusURIs = dctVerifiedURIs.Values;

                var statusNums = string.Join(", ", verifiedStatusNums);

                var statusURIs = string.Join(", ", verifiedStatusURIs);

                var ingestStepsCompleted = GetMaxIngestStepCompleted(verifiedStatusNums, dctStatusData);

                var dbTools = mCaptureDbProcedureExecutor;
                var cmd = dbTools.CreateCommand(SP_NAME, CommandType.StoredProcedure);

                dbTools.AddParameter(cmd, "@Return", SqlType.Int, direction: ParameterDirection.ReturnValue);
                dbTools.AddParameter(cmd, "@datasetID", SqlType.Int, value: mDatasetID);
                dbTools.AddParameter(cmd, "@StatusNumList", SqlType.VarChar, 1024, statusNums);
                dbTools.AddParameter(cmd, "@statusURIList", SqlType.VarChar, 4000, statusURIs);
                dbTools.AddParameter(cmd, "@ingestStepsCompleted", SqlType.TinyInt, value: ingestStepsCompleted);
                dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, direction: ParameterDirection.Output);

                var resCode = mCaptureDbProcedureExecutor.ExecuteSP(cmd, 2);

                if (resCode == 0)
                    return;

                var msg = "Error " + resCode + " calling stored procedure " + SP_NAME + ", job " + mJob;
                LogError(msg);
            }
            catch (Exception ex)
            {
                var msg = "Exception calling stored procedure " + SP_NAME + ", job " + mJob;
                LogError(msg, ex);
            }

        }

        #endregion

    }

}
