//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/10/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using System.Data.SqlClient;

// ReSharper disable UnusedMember.Global
namespace CaptureTaskManager
{
    /// <summary>
    /// Base class for handling task-related data
    /// </summary>
    abstract class clsDbTask : clsLoggerBase
    {

        #region "Constants"

        protected const int RET_VAL_OK = 0;
        protected const int RET_VAL_TASK_NOT_AVAILABLE = 53000;

        #endregion

        #region "Class variables"

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
        protected readonly Dictionary<string, string> mJobParams = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

        /// <summary>
        /// Stored procedure executor
        /// </summary>
        protected readonly PRISM.ExecuteDatabaseSP mCaptureTaskDBProcedureExecutor;

        #endregion

        #region "Properties"

        /// <summary>
        /// Manager name
        /// </summary>
        public string ManagerName { get; }

        /// <summary>
        /// Job parameters
        /// </summary>
        public Dictionary<string, string> TaskDictionary => mJobParams;

        #endregion

        #region "Constructor"

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="mgrParams"></param>
        protected clsDbTask(IMgrParams mgrParams)
        {
            mMgrParams = mgrParams;

            ManagerName = mMgrParams.GetParam("MgrName", System.Net.Dns.GetHostName() + "_Undefined-Manager");

            // Gigasax.DMS_Capture
            mConnStr = mMgrParams.GetParam("ConnectionString");

            mCaptureTaskDBProcedureExecutor = new PRISM.ExecuteDatabaseSP(mConnStr);

            mCaptureTaskDBProcedureExecutor.ErrorEvent += CaptureTaskDBProcedureExecutor_DBErrorEvent;

            // Cache the log level
            // 4 means Info level (normal) logging; 5 for Debug level (verbose) logging
            mDebugLevel = mgrParams.GetParam("DebugLevel", 4);
        }

        #endregion

        #region "Methods"

        /// <summary>
        /// Requests a capture pipeline task
        /// </summary>
        /// <returns>RequestTaskResult enum specifying call result</returns>
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
        public abstract void CloseTask(EnumCloseOutType taskResult, string closeoutMsg);

        /// <summary>
        /// Closes a capture pipeline task (Overloaded)
        /// </summary>
        /// <param name="taskResult">Enum representing task state</param>
        /// <param name="closeoutMsg">Message related to task closeout</param>
        /// <param name="evalCode">Enum representing evaluation results</param>
        public abstract void CloseTask(EnumCloseOutType taskResult, string closeoutMsg, EnumEvalCode evalCode);

        /// <summary>
        /// Closes a capture pipeline task (Overloaded)
        /// </summary>
        /// <param name="taskResult">Enum representing task state</param>
        /// <param name="closeoutMsg">Message related to task closeout</param>
        /// <param name="evalCode">Enum representing evaluation results</param>
        /// <param name="evalMsg">Message related to evaluation results</param>
        public abstract void CloseTask(EnumCloseOutType taskResult, string closeoutMsg, EnumEvalCode evalCode, string evalMsg);

        /// <summary>
        /// Debugging routine for printing SP calling params
        /// </summary>
        /// <param name="inpCmd">SQL command object containing params</param>
        protected virtual void PrintCommandParams(SqlCommand inpCmd)
        {
            // Verify there really are command parameters
            if (inpCmd == null)
                return;

            if (inpCmd.Parameters.Count < 1)
                return;

            var msg = "";

            foreach (SqlParameter myParam in inpCmd.Parameters)
            {
                msg += Environment.NewLine + string.Format("  Name= {0,-20}, Value= {1}", myParam.ParameterName, DbCStr(myParam.Value));
            }

            var writeToLog = mDebugLevel >= 5;
            LogDebug("Parameter list:" + msg, writeToLog);
        }

        /// <summary>
        /// Fill string dictionary with parameter values
        /// </summary>
        /// <param name="parameters">Result table from call to RequestStepTask</param>
        /// <returns></returns>
        protected virtual bool FillParamDict(List<List<string>> parameters)
        {
            // Verify valid parameters
            if (parameters == null)
            {
                LogError("clsDbTask.FillParamDict(): parameters is null");
                return false;
            }

            // Verify at least one row present
            if (parameters.Count < 1)
            {
                LogError("clsDbTask.FillParamDict(): No parameters returned by request SP");
                return false;
            }

            mJobParams.Clear();

            try
            {
                foreach (var dataRow in parameters)
                {
                    if (dataRow.Count < 2)
                        continue;

                    var paramName = dataRow[0];
                    var paramValue = dataRow[1];

                    if (!string.IsNullOrWhiteSpace(paramName))
                    {
                        mJobParams.Add(paramName, paramValue);
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                LogError("clsDbTask.FillParamDict(): Exception reading task parameters", ex);
                return false;
            }
        }

        private string DbCStr(object InpObj)
        {
            // If input object is DbNull, returns "", otherwise returns String representation of object
            if (InpObj == null || ReferenceEquals(InpObj, DBNull.Value))
            {
                return "";
            }

            return InpObj.ToString();
        }

        protected float DbCSng(object InpObj)
        {
            // If input object is DbNull, returns 0.0, otherwise returns Single representation of object
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0.0F;
            }

            return (float)InpObj;
        }

        protected double DbCDbl(object InpObj)
        {
            // If input object is DbNull, returns 0.0, otherwise returns Double representation of object
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0.0;
            }

            return (double)InpObj;
        }

        protected int DbCInt(object InpObj)
        {
            // If input object is DbNull, returns 0, otherwise returns Integer representation of object
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0;
            }

            return (int)InpObj;
        }

        protected long DbCLng(object InpObj)
        {
            // If input object is DbNull, returns 0, otherwise returns Integer representation of object
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0;
            }

            return (long)InpObj;
        }

        protected decimal DbCDec(object InpObj)
        {
            // If input object is DbNull, returns 0, otherwise returns Decimal representation of object
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0;
            }

            return (decimal)InpObj;
        }

        protected short DbCShort(object InpObj)
        {
            // If input object is DbNull, returns 0, otherwise returns Short representation of object
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0;
            }

            return (short)InpObj;
        }

        #endregion

        #region "Event handlers"

        void CaptureTaskDBProcedureExecutor_DBErrorEvent(string message, Exception ex)
        {
            var logToDb = message.Contains("permission was denied");

            if (logToDb)
                LogError(message, logToDb:true);
            else
                LogError(message, ex);
        }

        #endregion
    }
}