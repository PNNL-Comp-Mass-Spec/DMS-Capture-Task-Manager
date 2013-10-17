
//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/10/2009
//
// Last modified 09/10/2009
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Text;
using System.Data.SqlClient;
using System.Data;

namespace CaptureTaskManager
{
	abstract class clsDbTask
	{
		//*********************************************************************************************************
		// Base class for handling task-related data
		//**********************************************************************************************************

		#region "Enums"
		#endregion

		#region "Constants"
			protected const int RET_VAL_OK = 0;
			protected const int RET_VAL_TASK_NOT_AVAILABLE = 53000;
		#endregion

		#region "Class variables"
			protected IMgrParams m_MgrParams;
			protected string m_ConnStr;
			protected List<string> m_ErrorList = new List<string>();
			protected bool m_TaskWasAssigned = false;
			protected System.Collections.Generic.Dictionary<string, string> m_JobParams = new System.Collections.Generic.Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
		#endregion

		#region "Properties"
			public bool TaskWasAssigned
			{
				get
				{
					return m_TaskWasAssigned;
				}
			}

			public System.Collections.Generic.Dictionary<string,string> TaskDictionary 
			{	get 
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
			}	// End sub
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
			/// Reports database errors to local log
			/// </summary>
			protected virtual void LogErrorEvents()
			{
				if (m_ErrorList.Count > 0)
				{
					string msg = "Warning messages were posted to local log";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile,clsLogTools.LogLevels.WARN,msg);
				}
				foreach (string s in m_ErrorList)
				{
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.WARN, s);
				}
			}	// End sub

			/// <summary>
			/// Method for executing a db stored procedure, assuming no data table is returned
			/// </summary>
			/// <param name="SpCmd">SQL command object containing stored procedure params</param>
			/// <param name="ConnStr">Db connection string</param>
			/// <returns>Result code returned by SP; -1 if unable to execute SP</returns>
			protected virtual int ExecuteSP(SqlCommand spCmd, string connStr)
			{
				DataTable dummyTable = null;
				return ExecuteSP(spCmd, ref dummyTable, connStr);
			}	// End sub

			/// <summary>
			/// Method for executing a db stored procedure if a data table is to be returned
			/// </summary>
			/// <param name="SpCmd">SQL command object containing stored procedure params</param>
			/// <param name="OutTable">NOTHING when called; if SP successful, contains data table on return</param>
			/// <param name="ConnStr">Db connection string</param>
			/// <returns>Result code returned by SP; -1 if unable to execute SP</returns>
			protected virtual int ExecuteSP(SqlCommand spCmd, ref DataTable outTable, string connStr)
			{
				int resCode = -9999;
				//If this value is in error msg, then exception occurred before ResCode was set
				string msg = null;
				System.Diagnostics.Stopwatch myTimer = new System.Diagnostics.Stopwatch();
				int retryCount = 3;

				m_ErrorList.Clear();
				while (retryCount > 0)
				{
					//Multiple retry loop for handling SP execution failures
					try
					{
						using (SqlConnection cn = new SqlConnection(connStr))
						{
							cn.InfoMessage += OnInfoMessage;
							using (SqlDataAdapter da = new SqlDataAdapter())
							{
								using (DataSet ds = new DataSet())
								{
									//NOTE: The connection has to be added here because it didn't exist at the time the command object was created
									spCmd.Connection = cn;
									//Change command timeout from 30 second default in attempt to reduce SP execution timeout errors
									spCmd.CommandTimeout = int.Parse(m_MgrParams.GetParam("cmdtimeout", "30"));
									da.SelectCommand = spCmd;
									myTimer.Start();
									da.Fill(ds);
									myTimer.Stop();
									resCode = (int)da.SelectCommand.Parameters["@Return"].Value;
									if ((outTable != null) && (ds.Tables.Count>0)) outTable = ds.Tables[0];
								}	// ds
							}	//de
							cn.InfoMessage -= OnInfoMessage;
						}	// cn
						LogErrorEvents();
						break;
					}
					catch (System.Exception ex)
					{
						myTimer.Stop();
						if (ex.Message.Contains("permission was denied"))
							retryCount = 0;
						else
							retryCount -= 1;

						msg = "clsDBTask.ExecuteSP(), exception filling data adapter, " + ex.Message;
						msg += ". ResCode = " + resCode.ToString() + ". Retry count = " + retryCount.ToString();
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					}
					finally
					{
						//Log debugging info
						msg = "SP execution time: " + ((double)myTimer.ElapsedMilliseconds / 1000.0).ToString("##0.000") + " seconds ";
						msg += "for SP " + spCmd.CommandText;
						clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);

						//Reset the connection timer
						myTimer.Reset();
					}

					if (retryCount > 0)
					{
						//Wait 10 seconds before retrying
						System.Threading.Thread.Sleep(10000);
					}
				}

				if (retryCount < 1)
				{
					//Too many retries, log and return error
					msg = "Excessive retries executing SP " + spCmd.CommandText;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return -1;
				}

				return resCode;
			}	// End sub

			/// <summary>
			/// Debugging routine for printing SP calling params
			/// </summary>
			/// <param name="InpCmd">SQL command object containing params</param>
			protected virtual void PrintCommandParams(SqlCommand inpCmd)
			{
				//Verify there really are command paramters
				if (inpCmd == null) return;

				if (inpCmd.Parameters.Count < 1) return;

				string myMsg = "";

				foreach (SqlParameter myParam in inpCmd.Parameters)
				{
					myMsg += Environment.NewLine + "Name= " + myParam.ParameterName + "\t, Value= " + DbCStr(myParam.Value);
				}

				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Parameter list:" + myMsg);
			}	// End sub

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
				m_JobParams = new System.Collections.Generic.Dictionary<string, string>(StringComparer.CurrentCultureIgnoreCase);
				try
				{
					foreach (DataRow currRow in dt.Rows)
					{
						string myKey = currRow[dt.Columns["Parameter"]] as string;
						string myVal = currRow[dt.Columns["Value"]] as string;
						m_JobParams.Add(myKey, myVal);
					}
					return true;
				}
				catch (Exception ex)
				{
					msg = "clsDbTask.FillParamDict(): Exception reading task parameters";
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
					return false;
				}
			}	// End sub

			protected string DbCStr(object InpObj)
			{
				//If input object is DbNull, returns "", otherwise returns String representation of object
				if ((InpObj == null) || (object.ReferenceEquals(InpObj, DBNull.Value)))
				{
					return "";
				}
				else
				{
					return InpObj.ToString();
				}
			}	// End sub

			protected float DbCSng(object InpObj)
			{
				//If input object is DbNull, returns 0.0, otherwise returns Single representation of object
				if (object.ReferenceEquals(InpObj, DBNull.Value))
				{
					return 0.0F;
				}
				else
				{
					return (float)InpObj;
				}
			}	// End sub

			protected double DbCDbl(object InpObj)
			{
				//If input object is DbNull, returns 0.0, otherwise returns Double representation of object
				if (object.ReferenceEquals(InpObj, DBNull.Value))
				{
					return 0.0;
				}
				else
				{
					return (double)InpObj;
				}
			}	// End sub

			protected int DbCInt(object InpObj)
			{
				//If input object is DbNull, returns 0, otherwise returns Integer representation of object
				if (object.ReferenceEquals(InpObj, DBNull.Value))
				{
					return 0;
				}
				else
				{
					return (int)InpObj;
				}
			}	// End sub

			protected long DbCLng(object InpObj)
			{
				//If input object is DbNull, returns 0, otherwise returns Integer representation of object
				if (object.ReferenceEquals(InpObj, DBNull.Value))
				{
					return 0;
				}
				else
				{
					return (long)InpObj;
				}
			}	// End sub

			protected decimal DbCDec(object InpObj)
			{
				//If input object is DbNull, returns 0, otherwise returns Decimal representation of object
				if (object.ReferenceEquals(InpObj, DBNull.Value))
				{
					return 0;
				}
				else
				{
					return (decimal)InpObj;
				}
			}	// End sub

			protected short DbCShort(object InpObj)
			{
				//If input object is DbNull, returns 0, otherwise returns Short representation of object
				if (object.ReferenceEquals(InpObj, DBNull.Value))
				{
					return 0;
				}
				else
				{
					return (short)InpObj;
				}
			}	// End sub
		#endregion

		#region "Event handlers"
			/// <summary>
			/// Event handler for InfoMessage event from SQL Server
			/// </summary>
			/// <param name="sender"></param>
			/// <param name="args"></param>
			private void OnInfoMessage(object sender, SqlInfoMessageEventArgs args)
			{
				StringBuilder errString=new StringBuilder();
				foreach (SqlError err in args.Errors)
				{
					errString.Length = 0;
					errString.Append("Message: " + err.Message);
					errString.Append(", Source: " + err.Source);
					errString.Append(", Class: " + err.Class);
					errString.Append(", State: " + err.State);
					errString.Append(", Number: " + err.Number);
					errString.Append(", LineNumber: " + err.LineNumber);
					errString.Append(", Procedure:" + err.Procedure);
					errString.Append(", Server: " + err.Server);
					m_ErrorList.Add(errString.ToString());
				}
			}	// End sub
		#endregion
	}	// End class
}	// End namespace
