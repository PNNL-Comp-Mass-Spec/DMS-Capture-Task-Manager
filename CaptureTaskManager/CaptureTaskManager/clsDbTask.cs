
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/10/2009
//
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Data;

namespace CaptureTaskManager
{
    abstract class clsDbTask
    {
        //*********************************************************************************************************
        // Base class for handling task-related data
        //**********************************************************************************************************

        #region "Constants"
        protected const int RET_VAL_OK = 0;
        protected const int RET_VAL_TASK_NOT_AVAILABLE = 53000;
        #endregion

        #region "Class variables"

        protected IMgrParams m_MgrParams;
        protected string m_ConnStr;
        protected bool m_TaskWasAssigned = false;
        protected Dictionary<string, string> m_JobParams = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);

        protected PRISM.DataBase.clsExecuteDatabaseSP CaptureTaskDBProcedureExecutor;

        #endregion

        #region "Properties"
        public bool TaskWasAssigned
        {
            get
            {
                return m_TaskWasAssigned;
            }
        }

        public Dictionary<string, string> TaskDictionary
        {
            get
            {
                return m_JobParams;
            }
        }
        #endregion

        #region "Constructors"
        protected clsDbTask(IMgrParams MgrParams)
        {
            m_MgrParams = MgrParams;
            m_ConnStr = m_MgrParams.GetParam("ConnectionString");

            CaptureTaskDBProcedureExecutor = new PRISM.DataBase.clsExecuteDatabaseSP(m_ConnStr);

            CaptureTaskDBProcedureExecutor.DBErrorEvent += CaptureTaskDBProcedureExecutor_DBErrorEvent;
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
            //Verify there really are command paramters
            if (inpCmd == null) return;

            if (inpCmd.Parameters.Count < 1) return;

            var myMsg = "";

            foreach (SqlParameter myParam in inpCmd.Parameters)
            {
                myMsg += Environment.NewLine + "Name= " + myParam.ParameterName + "\t, Value= " + DbCStr(myParam.Value);
            }

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parameter list:" + myMsg);
        }

        protected virtual bool FillParamDict(DataTable dt)
        {
            string msg;

            // Verify valid datatable
            if (dt == null)
            {
                msg = "clsDbTask.FillParamDict(): No parameter table";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                return false;
            }

            // Verify at least one row present
            if (dt.Rows.Count < 1)
            {
                msg = "clsDbTask.FillParamDict(): No parameters returned by request SP";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
                return false;
            }

            // Fill string dictionary with parameter values
            m_JobParams.Clear();
            m_JobParams = new Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
            try
            {
                foreach (DataRow currRow in dt.Rows)
                {
                    var myKey = currRow[dt.Columns["Parameter"]] as string;
                    var myVal = currRow[dt.Columns["Value"]] as string;
                    if (myKey != null)
                    {
                        m_JobParams.Add(myKey, myVal);
                    }
                }
                return true;
            }
            catch (Exception ex)
            {
                msg = "clsDbTask.FillParamDict(): Exception reading task parameters";
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
                return false;
            }
        }

        protected string DbCStr(object InpObj)
        {
            //If input object is DbNull, returns "", otherwise returns String representation of object
            if ((InpObj == null) || (ReferenceEquals(InpObj, DBNull.Value)))
            {
                return "";
            }
            
            return InpObj.ToString();
        }

        protected float DbCSng(object InpObj)
        {
            //If input object is DbNull, returns 0.0, otherwise returns Single representation of object
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0.0F;
            }
            
            return (float)InpObj;
        }

        protected double DbCDbl(object InpObj)
        {
            //If input object is DbNull, returns 0.0, otherwise returns Double representation of object
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0.0;
            }
            
            return (double)InpObj;
        }

        protected int DbCInt(object InpObj)
        {
            //If input object is DbNull, returns 0, otherwise returns Integer representation of object
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0;
            }
            
            return (int)InpObj;
        }

        protected long DbCLng(object InpObj)
        {
            //If input object is DbNull, returns 0, otherwise returns Integer representation of object
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0;
            }
            
            return (long)InpObj;
        }

        protected decimal DbCDec(object InpObj)
        {
            //If input object is DbNull, returns 0, otherwise returns Decimal representation of object
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0;
            }
            
            return (decimal)InpObj;
        }

        protected short DbCShort(object InpObj)
        {
            //If input object is DbNull, returns 0, otherwise returns Short representation of object
            if (ReferenceEquals(InpObj, DBNull.Value))
            {
                return 0;
            }
            
            return (short)InpObj;
        }

        #endregion

        #region "Event handlers"

        void CaptureTaskDBProcedureExecutor_DBErrorEvent(string Message)
        {
            if (Message.Contains("permission was denied"))
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogDb, clsLogTools.LogLevels.ERROR, Message);
            else
                clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, Message);
        }

        #endregion
    }	// End class
}	// End namespace
