//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/15/2009
//*********************************************************************************************************

using PRISM;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;

namespace CaptureTaskManager
{
    /// <summary>
    /// Contacts the database to retrieve a task or mark a task as complete (or failed)
    /// </summary>
    class clsCaptureTask : clsDbTask, ITaskParams
    {

        #region "Constants"

        private const string SP_NAME_SET_COMPLETE = "SetStepTaskComplete";
        private const string SP_NAME_REPORT_IDLE = "ReportManagerIdle";
        private const string SP_NAME_REQUEST_TASK = "RequestStepTask";

        #endregion

        #region "Properties"

        /// <summary>
        /// When true, show additional messages at the console
        /// </summary>
        public bool TraceMode { get; set; }

        #endregion

        #region "Constructor"

        /// <summary>
        /// Class constructor
        /// </summary>
        /// <param name="mgrParams">Manager params for use by class</param>
        public clsCaptureTask(IMgrParams mgrParams)
            : base(mgrParams)
        {
            mJobParams.Clear();
        }

        #endregion

        #region "Methods"

        /// <summary>
        /// Gets a job task parameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <returns>Parameter value if found, otherwise empty string</returns>
        public string GetParam(string name)
        {
            return GetParam(name, string.Empty);
        }

        /// <summary>
        /// Gets a job task parameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
        /// <returns>Parameter value if found, otherwise empty string</returns>
        public string GetParam(string name, string valueIfMissing)
        {
            if (mJobParams.TryGetValue(name, out var itemValue))
            {
                return itemValue ?? string.Empty;
            }

            return valueIfMissing ?? string.Empty;
        }

        /// <summary>
        /// Gets a boolean job task parameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
        /// <returns>
        /// True if the parameter value is "true" or is a non-zero integer
        /// False if the parameter value is "false" or is zero
        /// Otherwise returns valueIfMissing
        /// </returns>
        public bool GetParam(string name, bool valueIfMissing)
        {
            if (!mJobParams.TryGetValue(name, out var valueText))
                return valueIfMissing;

            if (string.IsNullOrWhiteSpace(valueText))
                return valueIfMissing;

            if (bool.TryParse(valueText, out var value))
                return value;

            if (int.TryParse(valueText, out var integerValue))
            {
                return integerValue != 0;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Gets a job task parameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
        /// <returns>Parameter value if found, otherwise empty string</returns>
        public float GetParam(string name, float valueIfMissing)
        {
            if (mJobParams.TryGetValue(name, out var valueText))
            {
                if (float.TryParse(valueText, out var value))
                    return value;

                return valueIfMissing;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Gets a job task parameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
        /// <returns>Parameter value if found, otherwise empty string</returns>
        public int GetParam(string name, int valueIfMissing)
        {
            if (mJobParams.TryGetValue(name, out var valueText))
            {
                if (int.TryParse(valueText, out var value))
                    return value;

                return valueIfMissing;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Check for the existence of a job task parameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <returns>True if the parameter is defined, false if not</returns>
        public bool HasParam(string name)
        {
            return mJobParams.ContainsKey(name);
        }

        /// <summary>
        /// Adds (or updates) a parameter
        /// </summary>
        /// <param name="paramName">Name of parameter</param>
        /// <param name="paramValue">Value for parameter</param>
        /// <returns>RUE for success, FALSE for error</returns>
        public bool AddAdditionalParameter(string paramName, string paramValue)
        {
            try
            {
                if (mJobParams.ContainsKey(paramName))
                    mJobParams[paramName] = paramValue;
                else
                    mJobParams.Add(paramName, paramValue);

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception adding parameter: " + paramName + ", Value: " + paramValue, ex);
                return false;
            }
        }

        /// <summary>
        /// Stores a parameter
        /// </summary>
        /// <param name="keyName">Parameter key</param>
        /// <param name="value">Parameter value</param>
        public void SetParam(string keyName, string value)
        {
            if (value == null)
            {
                value = "";
            }
            mJobParams[keyName] = value;
        }

        /// <summary>
        /// Wrapper for requesting a task from the database
        /// </summary>
        /// <returns>num indicating if task was found</returns>
        public override EnumRequestTaskResult RequestTask()
        {
            var retVal = RequestTaskDetailed();

            switch (retVal)
            {
                case EnumRequestTaskResult.TaskFound:
                    mTaskWasAssigned = true;
                    break;
                case EnumRequestTaskResult.NoTaskFound:
                    mTaskWasAssigned = false;
                    break;
                case EnumRequestTaskResult.TooManyRetries:
                case EnumRequestTaskResult.Deadlock:
                    // Make sure the database didn't actually assign a job to this manager
                    ReportManagerIdle();
                    mTaskWasAssigned = false;
                    break;
                default:
                    mTaskWasAssigned = false;
                    break;
            }

            return retVal;
        }

        /// <summary>
        /// Detailed step request
        /// </summary>
        /// <returns>RequestTaskResult enum</returns>
        private EnumRequestTaskResult RequestTaskDetailed()
        {
            EnumRequestTaskResult outcome;
            var appVersion = Assembly.GetEntryAssembly().GetName().Version.ToString();

            try
            {
                var spCmd = new SqlCommand(SP_NAME_REQUEST_TASK)
                {
                    CommandType = CommandType.StoredProcedure
                };

                spCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                spCmd.Parameters.Add(new SqlParameter("@processorName", SqlDbType.VarChar, 128)).Value = ManagerName;
                spCmd.Parameters.Add(new SqlParameter("@jobNumber", SqlDbType.Int)).Direction = ParameterDirection.Output;
                spCmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512)).Direction = ParameterDirection.Output;
                spCmd.Parameters.Add(new SqlParameter("@infoOnly", SqlDbType.TinyInt)).Value = 0;
                spCmd.Parameters.Add(new SqlParameter("@ManagerVersion", SqlDbType.VarChar, 128)).Value = appVersion;
                spCmd.Parameters.Add(new SqlParameter("@JobCountToPreview", SqlDbType.Int)).Value = 10;

                LogDebug("clsCaptureTask.RequestTaskDetailed(), connection string: " + mConnStr);

                if (mDebugLevel >= 5 || TraceMode)
                {
                    PrintCommandParams(spCmd);
                }

                // Execute the SP
                var resCode = mCaptureTaskDBProcedureExecutor.ExecuteSP(spCmd, out var results);

                switch (resCode)
                {
                    case RET_VAL_OK:
                        // No errors found in SP call, so see if any step tasks were found

                        // Step task was found; get the data for it
                        var paramSuccess = FillParamDict(results);
                        if (paramSuccess)
                        {
                            outcome = EnumRequestTaskResult.TaskFound;
                        }
                        else
                        {
                            // There was an error
                            outcome = EnumRequestTaskResult.ResultError;
                        }
                        break;
                    case RET_VAL_TASK_NOT_AVAILABLE:
                        // No jobs found
                        outcome = EnumRequestTaskResult.NoTaskFound;
                        break;
                    case ExecuteDatabaseSP.RET_VAL_EXCESSIVE_RETRIES:
                        // Too many retries
                        outcome = EnumRequestTaskResult.TooManyRetries;
                        break;
                    case ExecuteDatabaseSP.RET_VAL_DEADLOCK:
                        // Transaction was deadlocked on lock resources with another process and has been chosen as the deadlock victim
                        outcome = EnumRequestTaskResult.Deadlock;
                        break;
                    default:
                        // There was an SP error
                        LogError("clsCaptureTask.RequestTaskDetailed(), SP execution error " + resCode +
                            "; Msg text = " + (string)spCmd.Parameters["@message"].Value);
                        outcome = EnumRequestTaskResult.ResultError;
                        break;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception requesting analysis job", ex);
                LogError("Stack trace: " + StackTraceFormatter.GetExceptionStackTrace(ex));
                outcome = EnumRequestTaskResult.ResultError;
            }
            return outcome;
        }

        /// <summary>
        /// Closes a capture pipeline task (Overloaded)
        /// </summary>
        /// <param name="taskResult">Enum representing task state</param>
        public override void CloseTask(EnumCloseOutType taskResult)
        {
            CloseTask(taskResult, "", EnumEvalCode.EVAL_CODE_SUCCESS, "");
        }

        /// <summary>
        /// Closes a capture pipeline task (Overloaded)
        /// </summary>
        /// <param name="taskResult">Enum representing task state</param>
        /// <param name="closeoutMsg">Message related to task closeout</param>
        public override void CloseTask(EnumCloseOutType taskResult, string closeoutMsg)
        {
            CloseTask(taskResult, closeoutMsg, EnumEvalCode.EVAL_CODE_SUCCESS, "");
        }

        /// <summary>
        /// Closes a capture pipeline task (Overloaded)
        /// </summary>
        /// <param name="taskResult">Enum representing task state</param>
        /// <param name="closeoutMsg">Message related to task closeout</param>
        /// <param name="evalCode">Enum representing evaluation results</param>
        public override void CloseTask(EnumCloseOutType taskResult, string closeoutMsg, EnumEvalCode evalCode)
        {
            CloseTask(taskResult, closeoutMsg, evalCode, "");
        }

        /// <summary>
        /// Closes a capture pipeline task (Overloaded)
        /// </summary>
        /// <param name="taskResult">Enum representing task state</param>
        /// <param name="closeoutMsg">Message related to task closeout</param>
        /// <param name="evalCode">Enum representing evaluation results</param>
        /// <param name="evalMsg">Message related to evaluation results</param>
        public override void CloseTask(EnumCloseOutType taskResult, string closeoutMsg, EnumEvalCode evalCode, string evalMsg)
        {
            var success = SetCaptureTaskComplete((int)taskResult, closeoutMsg, (int)evalCode, evalMsg);
            if (!success)
            {
                LogError("Error setting task complete in database, job " + GetParam("Job", "??"));
            }
            else
            {
                LogDebug("Successfully set task complete in database, job " + GetParam("Job", "??"));
            }
        }

        /// <summary>
        /// Call stored procedure ReportManagerIdle to make sure that
        /// the database didn't actually assign a job to this manager
        /// </summary>
        private void ReportManagerIdle()
        {
            // Setup for execution of the stored procedure
            var spCmd = new SqlCommand(SP_NAME_REPORT_IDLE)
            {
                CommandType = CommandType.StoredProcedure
            };

            spCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
            spCmd.Parameters.Add(new SqlParameter("@managerName", SqlDbType.VarChar, 128)).Value = ManagerName;
            spCmd.Parameters.Add(new SqlParameter("@infoOnly", SqlDbType.TinyInt)).Value = 0;
            spCmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512)).Direction = ParameterDirection.Output;

            // Execute the Stored Procedure (retry the call up to 3 times)
            var returnCode = mCaptureTaskDBProcedureExecutor.ExecuteSP(spCmd, 3);

            if (returnCode == 0)
            {
                return;
            }

            LogError("Error " + returnCode + " calling " + spCmd.CommandText);
        }

        /// <summary>
        /// Database calls to set a capture task complete
        /// </summary>
        /// <param name="compCode">Integer representation of completion code</param>
        /// <param name="compMsg">Completion message</param>
        /// <param name="evalCode">Integer representation of evaluation code</param>
        /// <param name="evalMsg">Evaluation message</param>
        /// <returns>TRUE for success; FALSE for failure</returns>
        private bool SetCaptureTaskComplete(int compCode, string compMsg, int evalCode, string evalMsg)
        {
            bool outcome;

            try
            {
                // Setup for execution of the stored procedure
                var spCmd = new SqlCommand(SP_NAME_SET_COMPLETE)
                {
                    CommandType = CommandType.StoredProcedure
                };

                spCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int)).Direction = ParameterDirection.ReturnValue;
                spCmd.Parameters.Add(new SqlParameter("@job", SqlDbType.Int)).Value = int.Parse(mJobParams["Job"]);
                spCmd.Parameters.Add(new SqlParameter("@step", SqlDbType.Int)).Value = int.Parse(mJobParams["Step"]);
                spCmd.Parameters.Add(new SqlParameter("@completionCode", SqlDbType.Int)).Value = compCode;
                spCmd.Parameters.Add(new SqlParameter("@completionMessage", SqlDbType.VarChar, 512)).Value = compMsg.Trim('\r', '\n');
                spCmd.Parameters.Add(new SqlParameter("@evaluationCode", SqlDbType.Int)).Value = evalCode;
                spCmd.Parameters.Add(new SqlParameter("@evaluationMessage", SqlDbType.VarChar, 256)).Value = evalMsg.Trim('\r', '\n');
                spCmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512)).Direction = ParameterDirection.Output;

                LogDebug("Calling stored procedure " + SP_NAME_SET_COMPLETE);

                if (mDebugLevel >= 5)
                {
                    PrintCommandParams(spCmd);
                }

                // Execute the SP
                var resCode = mCaptureTaskDBProcedureExecutor.ExecuteSP(spCmd);

                if (resCode == 0)
                {
                    outcome = true;
                }
                else
                {
                    LogError("Error " + resCode + " setting transfer task complete; " +
                             "Message = " + (string)spCmd.Parameters["@message"].Value);
                    outcome = false;
                }
            }
            catch (Exception ex)
            {
                LogError("Exception calling stored procedure " + SP_NAME_SET_COMPLETE, ex);
                outcome = false;
            }

            return outcome;
        }

        #endregion
    }
}