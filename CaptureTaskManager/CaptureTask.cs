﻿//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/15/2009
//*********************************************************************************************************

using PRISM;
using System;
using System.Data;
using System.Reflection;
using PRISMDatabaseUtils;

namespace CaptureTaskManager
{
    /// <summary>
    /// Contacts the database to retrieve a task or mark a task as complete (or failed)
    /// </summary>
    internal class CaptureTask : DbTask, ITaskParams
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
        public CaptureTask(IMgrParams mgrParams)
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
            {
                return valueIfMissing;
            }

            if (string.IsNullOrWhiteSpace(valueText))
            {
                return valueIfMissing;
            }

            if (bool.TryParse(valueText, out var value))
            {
                return value;
            }

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
        /// <returns>Parameter value if found, otherwise valueIfMissing</returns>
        public float GetParam(string name, float valueIfMissing)
        {
            if (mJobParams.TryGetValue(name, out var valueText))
            {
                if (float.TryParse(valueText, out var value))
                {
                    return value;
                }

                return valueIfMissing;
            }

            return valueIfMissing;
        }

        /// <summary>
        /// Gets a job task parameter
        /// </summary>
        /// <param name="name">Parameter name</param>
        /// <param name="valueIfMissing">Value to return if the parameter does not exist</param>
        /// <returns>Parameter value if found, valueIfMissing</returns>
        public int GetParam(string name, int valueIfMissing)
        {
            if (mJobParams.TryGetValue(name, out var valueText))
            {
                if (int.TryParse(valueText, out var value))
                {
                    return value;
                }

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
                // Add/update the value
                mJobParams[paramName] = paramValue;
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
            value ??= string.Empty;
            mJobParams[keyName] = value;
        }

        /// <summary>
        /// Wrapper for requesting a task from the database
        /// </summary>
        /// <returns>Enum indicating if task was found</returns>
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
        /// Contact the database to request a job step be assigned
        /// </summary>
        /// <remarks>
        /// Certain scripts will only assign jobs to managers that are on the same server as a dataset
        /// This is controlled by column Machine in table T_Local_Processors
        /// </remarks>
        /// <returns>RequestTaskResult enum</returns>
        private EnumRequestTaskResult RequestTaskDetailed()
        {
            EnumRequestTaskResult outcome;
            var appVersion = Assembly.GetEntryAssembly()?.GetName().Version.ToString();

            try
            {
                var dbTools = mCaptureTaskDBProcedureExecutor;
                var cmd = dbTools.CreateCommand(SP_NAME_REQUEST_TASK, CommandType.StoredProcedure);

                dbTools.AddParameter(cmd, "@processorName", SqlType.VarChar, 128, ManagerName);
                dbTools.AddParameter(cmd, "@jobNumber", SqlType.Int, ParameterDirection.Output);
                var messageParam = dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.Output);
                dbTools.AddTypedParameter(cmd, "@infoOnly", SqlType.TinyInt, value: 0);
                dbTools.AddParameter(cmd, "@ManagerVersion", SqlType.VarChar, 128, appVersion ?? "(unknown version)");
                dbTools.AddTypedParameter(cmd, "@JobCountToPreview", SqlType.Int, value: 10);
                var returnParam = dbTools.AddParameter(cmd, "@returnCode", SqlType.VarChar, 64, ParameterDirection.Output);

                LogDebug("CaptureTask.RequestTaskDetailed(), connection string: " + mConnStr);

                if (mDebugLevel >= 5 || TraceMode)
                {
                    PrintCommandParams(cmd);
                }

                // Execute the SP
                var resCode = mCaptureTaskDBProcedureExecutor.ExecuteSPData(cmd, out var results);

                var returnCode = dbTools.GetString(returnParam.Value);
                var returnCodeValue = Conversion.GetReturnCodeValue(returnCode);

                if (returnCodeValue != 0)
                {
                    if (returnCodeValue == RET_VAL_TASK_NOT_AVAILABLE)
                    {
                        // No jobs found
                        return EnumRequestTaskResult.NoTaskFound;
                    }

                    // The return code was not an empty string, which indicates an error
                    LogError("CaptureTask.RequestTaskDetailed(), SP execution has return code " + returnCode +
                             "; Message text = " + (string)messageParam.Value);
                    return EnumRequestTaskResult.ResultError;
                }

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

                    case DbUtilsConstants.RET_VAL_EXCESSIVE_RETRIES:
                        // Too many retries
                        outcome = EnumRequestTaskResult.TooManyRetries;
                        break;
                    case DbUtilsConstants.RET_VAL_DEADLOCK:
                        // Transaction was deadlocked on lock resources with another process and has been chosen as the deadlock victim
                        outcome = EnumRequestTaskResult.Deadlock;
                        break;
                    default:
                        // There was an SP error
                        LogError("CaptureTask.RequestTaskDetailed(), SP execution error " + resCode +
                            "; Message text = " + (string)messageParam.Value);
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
            CloseTask(taskResult, string.Empty, EnumEvalCode.EVAL_CODE_SUCCESS, string.Empty);
        }

        /// <summary>
        /// Closes a capture pipeline task (Overloaded)
        /// </summary>
        /// <param name="taskResult">Enum representing task state</param>
        /// <param name="closeoutMsg">Message related to task closeout</param>
        public override void CloseTask(EnumCloseOutType taskResult, string closeoutMsg)
        {
            CloseTask(taskResult, closeoutMsg, EnumEvalCode.EVAL_CODE_SUCCESS, string.Empty);
        }

        /// <summary>
        /// Closes a capture pipeline task (Overloaded)
        /// </summary>
        /// <param name="taskResult">Enum representing task state</param>
        /// <param name="closeoutMsg">Message related to task closeout</param>
        /// <param name="evalCode">Enum representing evaluation results</param>
        public override void CloseTask(EnumCloseOutType taskResult, string closeoutMsg, EnumEvalCode evalCode)
        {
            CloseTask(taskResult, closeoutMsg, evalCode, string.Empty);
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
            var dbTools = mCaptureTaskDBProcedureExecutor;
            var cmd = dbTools.CreateCommand(SP_NAME_REPORT_IDLE, CommandType.StoredProcedure);

            dbTools.AddParameter(cmd, "@managerName", SqlType.VarChar, 128, ManagerName);
            dbTools.AddParameter(cmd, "@infoOnly", SqlType.TinyInt).Value = 0;
            dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.Output);
            var returnParam = dbTools.AddParameter(cmd, "@returnCode", SqlType.VarChar, 64, ParameterDirection.Output);

            // Execute the Stored Procedure (retry the call up to 3 times)
            var resCode = mCaptureTaskDBProcedureExecutor.ExecuteSP(cmd, 3);

            var returnCode = dbTools.GetString(returnParam.Value);
            var returnCodeValue = Conversion.GetReturnCodeValue(returnCode);

            if (resCode == 0 && returnCodeValue == 0)
            {
                return;
            }

            if (resCode != 0)
            {
                LogError("Error " + resCode + " calling stored procedure " + SP_NAME_REPORT_IDLE);
                return;
            }

            LogError("Stored procedure " + SP_NAME_REPORT_IDLE + " reported return code " + returnCode);
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
            try
            {
                // Setup for execution of the stored procedure
                var dbTools = mCaptureTaskDBProcedureExecutor;
                var cmd = dbTools.CreateCommand(SP_NAME_SET_COMPLETE, CommandType.StoredProcedure);

                dbTools.AddTypedParameter(cmd, "@job", SqlType.Int, value: int.Parse(mJobParams["Job"]));
                dbTools.AddTypedParameter(cmd, "@step", SqlType.Int, value: int.Parse(mJobParams["Step"]));
                dbTools.AddTypedParameter(cmd, "@completionCode", SqlType.Int, value: compCode);
                dbTools.AddParameter(cmd, "@completionMessage", SqlType.VarChar, 512, compMsg.Trim('\r', '\n'));
                dbTools.AddTypedParameter(cmd, "@evaluationCode", SqlType.Int, value: evalCode);
                dbTools.AddParameter(cmd, "@evaluationMessage", SqlType.VarChar, 256, evalMsg.Trim('\r', '\n'));
                dbTools.AddParameter(cmd, "@message", SqlType.VarChar, 512, ParameterDirection.Output);
                var returnParam = dbTools.AddParameter(cmd, "@returnCode", SqlType.VarChar, 64, ParameterDirection.Output);

                LogDebug("Calling stored procedure " + SP_NAME_SET_COMPLETE);

                if (mDebugLevel >= 5)
                {
                    PrintCommandParams(cmd);
                }

                // Execute the SP
                var resCode = mCaptureTaskDBProcedureExecutor.ExecuteSP(cmd);

                var returnCode = dbTools.GetString(returnParam.Value);
                var returnCodeValue = Conversion.GetReturnCodeValue(returnCode);

                if (resCode == 0 && returnCodeValue == 0)
                {
                    return true;
                }

                if (resCode != 0)
                {
                    LogError("Error " + resCode + " setting capture task complete");
                    return false;
                }

                LogError("Stored procedure " + SP_NAME_SET_COMPLETE + " reported return code " + returnCode);
                return false;
            }
            catch (Exception ex)
            {
                LogError("Exception calling stored procedure " + SP_NAME_SET_COMPLETE, ex);
                return false;
            }
        }

        #endregion
    }
}