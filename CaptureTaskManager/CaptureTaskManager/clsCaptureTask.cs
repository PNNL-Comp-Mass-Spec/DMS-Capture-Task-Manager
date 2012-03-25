
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/15/2009
//
// Last modified 09/15/2009
//*********************************************************************************************************
using System;
using System.Data.SqlClient;
using System.Data;
using System.Windows.Forms;

namespace CaptureTaskManager
{
	class clsCaptureTask : clsDbTask, ITaskParams
	{
		//*********************************************************************************************************
		// Provides database access and tools for one capture task
		//**********************************************************************************************************

		#region "Constants"
			protected const string SP_NAME_SET_COMPLETE = "SetStepTaskComplete";
			protected const string SP_NAME_REQUEST_TASK = "RequestStepTask";
		#endregion

		#region "Class variables"
			int m_JobID = 0;
		#endregion

		#region "Constructors"
			/// <summary>
			/// Class constructor
			/// </summary>
			/// <param name="mgrParams">Manager params for use by class</param>
			public clsCaptureTask(IMgrParams mgrParams)
				: base(mgrParams)
			{
				m_JobParams.Clear();
			}	// End sub
		#endregion

		#region "Methods"
			/// <summary>
			/// Gets a stored parameter
			/// </summary>
			/// <param name="name">Parameter name</param>
			/// <returns>Parameter value if found, otherwise empty string</returns>
			public string GetParam(string name)
			{
				if (m_JobParams.ContainsKey(name))
				{
					return m_JobParams[name];
				}
				else
				{
					return string.Empty;
				}
			}	// End sub

			/// <summary>
			/// Adds a parameter
			/// </summary>
			/// <param name="paramName">Name of parameter</param>
			/// <param name="paramValue">Value for parameter</param>
			/// <returns>RUE for success, FALSE for error</returns>
			public bool AddAdditionalParameter(string paramName, string paramValue)
			{
				try
				{
					m_JobParams.Add(paramName, paramValue);
					return true;
				}
				catch (Exception ex)
				{
					string msg = "Exception adding parameter: " + paramName + ", Value: " + paramValue;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
					return false;
				}
			}	// End sub

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
				m_JobParams[keyName] = value;
			}	// End sub

			/// <summary>
			/// Wrapper for requesting a task from the database
			/// </summary>
			/// <returns>num indicating if task was found</returns>
			public override EnumRequestTaskResult RequestTask()
			{
				EnumRequestTaskResult retVal;

				retVal = RequestTaskDetailed();
				switch (retVal)
				{
					case EnumRequestTaskResult.TaskFound:
						m_TaskWasAssigned = true;
						break;
					case EnumRequestTaskResult.NoTaskFound:
						m_TaskWasAssigned = false;
						break;
					default:
						m_TaskWasAssigned = false;
						break;
				}

				return retVal;
			}	// End sub

			/// <summary>
			/// Detailed step request
			/// </summary>
			/// <returns>RequestTaskResult enum</returns>
			private EnumRequestTaskResult RequestTaskDetailed()
			{
				string msg;
				SqlCommand myCmd = new SqlCommand();
				EnumRequestTaskResult outcome = EnumRequestTaskResult.NoTaskFound;
				int retVal = 0;
				DataTable dt = new DataTable();
				string strProductVersion = Application.ProductVersion;
				if (strProductVersion == null) strProductVersion = "??";

				try
				{
					//Set up the command object prior to SP execution
					{
						myCmd.CommandType = CommandType.StoredProcedure;
						myCmd.CommandText = SP_NAME_REQUEST_TASK;
						myCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int));
						myCmd.Parameters["@Return"].Direction = ParameterDirection.ReturnValue;

						myCmd.Parameters.Add(new SqlParameter("@processorName", SqlDbType.VarChar, 128));
						myCmd.Parameters["@processorName"].Direction = ParameterDirection.Input;
						myCmd.Parameters["@processorName"].Value = m_MgrParams.GetParam("MgrName");

						myCmd.Parameters.Add(new SqlParameter("@jobNumber", SqlDbType.Int));
						myCmd.Parameters["@jobNumber"].Direction = ParameterDirection.Output;

						myCmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512));
						myCmd.Parameters["@message"].Direction = ParameterDirection.Output;
						myCmd.Parameters["@message"].Value = "";

						myCmd.Parameters.Add(new SqlParameter("@infoOnly", SqlDbType.TinyInt));
						myCmd.Parameters["@infoOnly"].Direction = ParameterDirection.Input;
						myCmd.Parameters["@infoOnly"].Value = 0;

						myCmd.Parameters.Add(new SqlParameter("@ManagerVersion", SqlDbType.VarChar, 128));
						myCmd.Parameters["@ManagerVersion"].Direction = ParameterDirection.Input;
						myCmd.Parameters["@ManagerVersion"].Value = strProductVersion;

						myCmd.Parameters.Add(new SqlParameter("@JobCountToPreview", SqlDbType.Int));
						myCmd.Parameters["@JobCountToPreview"].Direction = ParameterDirection.Input;
						myCmd.Parameters["@JobCountToPreview"].Value = 10;
					}

					msg = "clsCaptureTask.RequestTaskDetailed(), connection string: " + m_ConnStr;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
					msg = "clsCaptureTask.RequestTaskDetailed(), printing param list";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
					PrintCommandParams(myCmd);

					//Execute the SP
					retVal = ExecuteSP(myCmd, ref dt, m_ConnStr);

					switch (retVal)
					{
						case RET_VAL_OK:
							//No errors found in SP call, so see if any step tasks were found
							m_JobID = (int)myCmd.Parameters["@jobNumber"].Value;

							//Step task was found; get the data for it
							bool paramSuccess = FillParamDict(dt);
							if (paramSuccess)
							{
								outcome = EnumRequestTaskResult.TaskFound;
							}
							else
							{
								//There was an error
								outcome = EnumRequestTaskResult.ResultError;
							}
							break;
						case RET_VAL_TASK_NOT_AVAILABLE:
							//No jobs found
							outcome = EnumRequestTaskResult.NoTaskFound;
							break;
						default:
							//There was an SP error
							msg = "clsCaptureTask.RequestTaskDetailed(), SP execution error " + retVal.ToString();
							msg += "; Msg text = " + (string)myCmd.Parameters["@message"].Value;
							clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
							outcome = EnumRequestTaskResult.ResultError;
							break;
					}
				}
				catch (System.Exception ex)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, "Exception requesting analysis job: " + ex.Message);
					outcome = EnumRequestTaskResult.ResultError;
				}
				return outcome;
			}	// End sub

			/// <summary>
			/// Closes a capture pipeline task (Overloaded)
			/// </summary>
			/// <param name="taskResult">Enum representing task state</param>
			public override void CloseTask(EnumCloseOutType taskResult)
			{
				CloseTask(taskResult, "", EnumEvalCode.EVAL_CODE_SUCCESS, "");
			}	// End sub

			/// <summary>
			/// Closes a capture pipeline task (Overloaded)
			/// </summary>
			/// <param name="taskResult">Enum representing task state</param>
			/// <param name="closeoutMsg">Message related to task closeout</param>
			public override void CloseTask(EnumCloseOutType taskResult, string closeoutMsg)
			{
				CloseTask(taskResult, closeoutMsg, EnumEvalCode.EVAL_CODE_SUCCESS, "");
			}	// End sub

			/// <summary>
			/// Closes a capture pipeline task (Overloaded)
			/// </summary>
			/// <param name="taskResult">Enum representing task state</param>
			/// <param name="closeoutMsg">Message related to task closeout</param>
			/// <param name="evalCode">Enum representing evaluation results</param>
			public override void CloseTask(EnumCloseOutType taskResult, string closeoutMsg, EnumEvalCode evalCode)
			{
				CloseTask(taskResult, closeoutMsg, evalCode, "");
			}	// End sub

			/// <summary>
			/// Closes a capture pipeline task (Overloaded)
			/// </summary>
			/// <param name="taskResult">Enum representing task state</param>
            /// <param name="closeoutMsg">Message related to task closeout</param>
			/// <param name="evalCode">Enum representing evaluation results</param>
			/// <param name="evalMsg">Message related to evaluation results</param>
            public override void CloseTask(EnumCloseOutType taskResult, string closeoutMsg, EnumEvalCode evalCode, string evalMsg)
			{
				string msg;
				int compCode = (int)taskResult;

                if (!SetCaptureTaskComplete(SP_NAME_SET_COMPLETE, m_ConnStr, (int)taskResult, closeoutMsg, (int)evalCode, evalMsg))
				{
					msg = "Error setting task complete in database, job " + m_JobParams["Job"];
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile,clsLogTools.LogLevels.ERROR,msg);
				}
				else
				{
					msg = msg = "Successfully set task complete in database, job " + m_JobParams["Job"];
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile,clsLogTools.LogLevels.DEBUG,msg);
				}
			}	// End sub

			/// <summary>
			/// Database calls to set a capture task complete
			/// </summary>
			/// <param name="SpName">Name of SetComplete stored procedure</param>
			/// <param name="CompletionCode">Integer representation of completion code</param>
			/// <param name="ConnStr">Db connection string</param>
			/// <returns>TRUE for sucesss; FALSE for failure</returns>
			public bool SetCaptureTaskComplete(string spName, string connStr, int compCode, string compMsg, int evalCode, string evalMsg)
			{
				string msg;
				bool Outcome = false;
				int ResCode = 0;

                try
                {

                    //Setup for execution of the stored procedure
                    SqlCommand MyCmd = new SqlCommand();
                    {
                        MyCmd.CommandType = CommandType.StoredProcedure;
                        MyCmd.CommandText = spName;
                        MyCmd.Parameters.Add(new SqlParameter("@Return", SqlDbType.Int));
                        MyCmd.Parameters["@Return"].Direction = ParameterDirection.ReturnValue;
                        MyCmd.Parameters.Add(new SqlParameter("@job", SqlDbType.Int));
                        MyCmd.Parameters["@job"].Direction = ParameterDirection.Input;
                        MyCmd.Parameters["@job"].Value = int.Parse(m_JobParams["Job"]);
                        MyCmd.Parameters.Add(new SqlParameter("@step", SqlDbType.Int));
                        MyCmd.Parameters["@step"].Direction = ParameterDirection.Input;
                        MyCmd.Parameters["@step"].Value = int.Parse(m_JobParams["Step"]);
                        MyCmd.Parameters.Add(new SqlParameter("@completionCode", SqlDbType.Int));
                        MyCmd.Parameters["@completionCode"].Direction = ParameterDirection.Input;
                        MyCmd.Parameters["@completionCode"].Value = compCode;
                        MyCmd.Parameters.Add(new SqlParameter("@completionMessage", SqlDbType.VarChar, 256));
                        MyCmd.Parameters["@completionMessage"].Direction = ParameterDirection.Input;
                        MyCmd.Parameters["@completionMessage"].Value = compMsg;
                        MyCmd.Parameters.Add(new SqlParameter("@evaluationCode", SqlDbType.Int));
                        MyCmd.Parameters["@evaluationCode"].Direction = ParameterDirection.Input;
                        MyCmd.Parameters["@evaluationCode"].Value = evalCode;
                        MyCmd.Parameters.Add(new SqlParameter("@evaluationMessage", SqlDbType.VarChar, 256));
                        MyCmd.Parameters["@evaluationMessage"].Direction = ParameterDirection.Input;
                        MyCmd.Parameters["@evaluationMessage"].Value = evalMsg;
                        MyCmd.Parameters.Add(new SqlParameter("@message", SqlDbType.VarChar, 512));
                        MyCmd.Parameters["@message"].Direction = ParameterDirection.Output;
                    }

                    msg = "Calling stored procedure " + spName;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

                    msg = "Parameters: Job=" + MyCmd.Parameters["@job"].Value +
                                    ", Step=" + MyCmd.Parameters["@step"].Value +
                                    ", completionCode=" + MyCmd.Parameters["@completionCode"].Value +
                                    ", completionMessage=" + MyCmd.Parameters["@completionMessage"].Value +
                                    ", evaluationCode=" + MyCmd.Parameters["@evaluationCode"].Value +
                                    ", evaluationMessage=" + MyCmd.Parameters["@evaluationMessage"].Value;

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);


                    //Execute the SP
                    ResCode = ExecuteSP(MyCmd, connStr);

                    if (ResCode == 0)
                    {
                        Outcome = true;
                    }
                    else
                    {
                        msg = "Error " + ResCode.ToString() + " setting transfer task complete";
                        msg += "; Message = " + (string)MyCmd.Parameters["@message"].Value;
                        Outcome = false;
                    }
                }
                catch (Exception ex)
                {
                    msg = "Exception calling stored procedure " + spName;
                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
                    Outcome = false;
                }

				return Outcome;
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
