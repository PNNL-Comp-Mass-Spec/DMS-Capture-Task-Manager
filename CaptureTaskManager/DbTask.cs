﻿//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/10/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using PRISM;
using PRISM.Logging;
using PRISMDatabaseUtils;

// ReSharper disable UnusedMember.Global
namespace CaptureTaskManager
{
    /// <summary>
    /// Base class for handling task-related data
    /// </summary>
    internal abstract class DbTask : LoggerBase
    {
        // Ignore Spelling: Ret

        protected const int RET_VAL_OK = 0;

        /// <summary>
        /// Return value for request_ctm_step_task on SQL Server
        /// </summary>
        protected const int RET_VAL_TASK_NOT_AVAILABLE = 53000;

        /// <summary>
        /// Return code for request_ctm_step_task on PostgreSQL
        /// </summary>
        /// <remarks>
        /// The actual return code is 'U5301' but Conversion.GetReturnCodeValue() converts this to integer 5301
        /// </remarks>
        protected const int RET_VAL_TASK_NOT_AVAILABLE_ALT = 5301;

        protected readonly IMgrParams mMgrParams;

        protected readonly string mConnStr;

        protected bool mTaskWasAssigned = false;

        /// <summary>
        /// Debug level
        /// </summary>
        /// <remarks>4 means Info level (normal) logging; 5 for Debug level (verbose) logging</remarks>
        protected readonly int mDebugLevel;

        /// <summary>
        /// Job parameters
        /// </summary>
        protected readonly Dictionary<string, string> mJobParams = new(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Procedure calling mechanism
        /// </summary>
        protected readonly IDBTools mCaptureTaskDBProcedureExecutor;

        /// <summary>
        /// Manager name
        /// </summary>
        public string ManagerName { get; }

        /// <summary>
        /// Job parameters
        /// </summary>
        public Dictionary<string, string> TaskDictionary => mJobParams;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams"></param>
        protected DbTask(IMgrParams mgrParams)
        {
            mMgrParams = mgrParams;
            var traceMode = mMgrParams.TraceMode;

            ManagerName = mMgrParams.GetParam("MgrName", System.Net.Dns.GetHostName() + "_Undefined-Manager");

            // DMS database on prismdb2 (previously, DMS_Capture on Gigasax)
            var connectionString = mMgrParams.GetParam("ConnectionString");

            mConnStr = DbToolsFactory.AddApplicationNameToConnectionString(connectionString, ManagerName);

            mCaptureTaskDBProcedureExecutor = DbToolsFactory.GetDBTools(mConnStr, debugMode: traceMode);
            RegisterEvents(mCaptureTaskDBProcedureExecutor);

            UnregisterEventHandler((EventNotifier)mCaptureTaskDBProcedureExecutor, BaseLogger.LogLevels.ERROR);
            mCaptureTaskDBProcedureExecutor.ErrorEvent += CaptureTaskDBProcedureExecutor_DBErrorEvent;

            // Cache the log level
            // 4 means Info level (normal) logging; 5 for Debug level (verbose) logging
            mDebugLevel = mgrParams.GetParam("DebugLevel", 4);
        }

        /// <summary>
        /// Requests a capture pipeline task
        /// </summary>
        /// <returns>RequestTaskResult enum specifying call result</returns>
        // ReSharper disable once UnusedMemberInSuper.Global
        public abstract EnumRequestTaskResult RequestTask();

        /// <summary>
        /// Closes a capture pipeline task (Overloaded)
        /// </summary>
        /// <param name="taskResult">Enum representing task state</param>
        public abstract void CloseTask(EnumCloseOutType taskResult);

        /// <summary>
        /// Closes a capture pipeline task (Overloaded)
        /// </summary>
        /// <param name="taskResult">Enum representing task state</param>
        /// <param name="closeoutMsg">Message related to task closeout</param>
        // ReSharper disable once UnusedMemberInSuper.Global
        public abstract void CloseTask(EnumCloseOutType taskResult, string closeoutMsg);

        /// <summary>
        /// Closes a capture pipeline task (Overloaded)
        /// </summary>
        /// <param name="taskResult">Enum representing task state</param>
        /// <param name="closeoutMsg">Message related to task closeout</param>
        /// <param name="evalCode">Enum representing evaluation results</param>
        // ReSharper disable once UnusedMemberInSuper.Global
        public abstract void CloseTask(EnumCloseOutType taskResult, string closeoutMsg, EnumEvalCode evalCode);

        /// <summary>
        /// Closes a capture pipeline task (Overloaded)
        /// </summary>
        /// <param name="taskResult">Enum representing task state</param>
        /// <param name="closeoutMsg">Message related to task closeout</param>
        /// <param name="evalCode">Enum representing evaluation results</param>
        /// <param name="evalMsg">Message related to evaluation results</param>
        // ReSharper disable once UnusedMemberInSuper.Global
        public abstract void CloseTask(EnumCloseOutType taskResult, string closeoutMsg, EnumEvalCode evalCode, string evalMsg);

        /// <summary>
        /// Debugging routine for showing values passed to the procedure parameters
        /// </summary>
        /// <param name="cmd">SQL command object containing params</param>
        protected virtual void PrintCommandParams(DbCommand cmd)
        {
            // Verify there really are command parameters
            if (cmd == null)
            {
                return;
            }

            if (cmd.Parameters.Count < 1)
            {
                return;
            }

            var msg = new StringBuilder();

            foreach (DbParameter myParam in cmd.Parameters)
            {
                msg.AppendLine();
                msg.AppendFormat("  Name= {0,-20}, Value= {1}", myParam.ParameterName, DbCStr(myParam.Value));
            }

            var writeToLog = mDebugLevel >= 5;
            LogDebug("Parameter list:" + msg, writeToLog);
        }

        /// <summary>
        /// Fill string dictionary with parameter values
        /// </summary>
        /// <param name="parameters">Result table from call to request_ctm_step_task</param>
        /// <returns>True if successful, false if an error</returns>
        protected virtual bool FillParamDictionary(List<List<string>> parameters)
        {
            // Verify valid parameters
            if (parameters == null)
            {
                LogError("DbTask.FillParamDictionary(): parameters is null");
                return false;
            }

            // Verify at least one row present
            if (parameters.Count < 1)
            {
                LogError("DbTask.FillParamDictionary(): No parameters returned by request SP");
                return false;
            }

            mJobParams.Clear();

            try
            {
                foreach (var dataRow in parameters)
                {
                    if (dataRow.Count < 2)
                    {
                        continue;
                    }

                    var paramName = dataRow[0];
                    var paramValue = dataRow[1];

                    if (string.IsNullOrWhiteSpace(paramName))
                    {
                        continue;
                    }

                    if (mJobParams.TryGetValue(paramName, out var existingValue))
                    {
                        if (string.Equals(existingValue, paramValue))
                        {
                            LogDebug("Skipping duplicate task parameter named {0}: the new value matches the existing value of '{1}'",
                                paramName, existingValue);
                        }
                        else
                        {
                            LogError("Duplicate task parameters have the same name ({0}), but conflicting values: existing value is '{1}' vs. new value of '{2}'",
                                paramName, existingValue, paramValue);
                            return false;
                        }
                    }
                    else
                    {
                        mJobParams.Add(paramName, paramValue);
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                LogError("DbTask.FillParamDictionary(): Exception reading task parameters", ex);
                return false;
            }
        }

        private static string DbCStr(object paramValue)
        {
            // If input object is DbNull, returns string.Empty, otherwise returns String representation of object
            if (paramValue == null || ReferenceEquals(paramValue, DBNull.Value))
            {
                return string.Empty;
            }

            return paramValue.ToString();
        }

        protected static float DbCFloat(object paramValue)
        {
            // If input object is DbNull, returns 0.0, otherwise returns Single representation of object
            if (ReferenceEquals(paramValue, DBNull.Value))
            {
                return 0.0F;
            }

            return (float)paramValue;
        }

        protected static double DbCDbl(object paramValue)
        {
            // If input object is DbNull, returns 0.0, otherwise returns Double representation of object
            if (ReferenceEquals(paramValue, DBNull.Value))
            {
                return 0.0;
            }

            return (double)paramValue;
        }

        protected static int DbCInt(object paramValue)
        {
            // If input object is DbNull, returns 0, otherwise returns Integer representation of object
            if (ReferenceEquals(paramValue, DBNull.Value))
            {
                return 0;
            }

            return (int)paramValue;
        }

        protected static long DbCLng(object paramValue)
        {
            // If input object is DbNull, returns 0, otherwise returns Integer representation of object
            if (ReferenceEquals(paramValue, DBNull.Value))
            {
                return 0;
            }

            return (long)paramValue;
        }

        protected static decimal DbCDec(object paramValue)
        {
            // If input object is DbNull, returns 0, otherwise returns Decimal representation of object
            if (ReferenceEquals(paramValue, DBNull.Value))
            {
                return 0;
            }

            return (decimal)paramValue;
        }

        protected static short DbCShort(object paramValue)
        {
            // If input object is DbNull, returns 0, otherwise returns Short representation of object
            if (ReferenceEquals(paramValue, DBNull.Value))
            {
                return 0;
            }

            return (short)paramValue;
        }

        private void CaptureTaskDBProcedureExecutor_DBErrorEvent(string message, Exception ex)
        {
            var logToDb = message.IndexOf("permission was denied", StringComparison.OrdinalIgnoreCase) >= 0 ||
                          message.IndexOf("permission denied", StringComparison.OrdinalIgnoreCase) >= 0;

            if (logToDb)
            {
                LogError(message, logToDb:true);
            }
            else
            {
                LogError(message, ex);
            }
        }
    }
}