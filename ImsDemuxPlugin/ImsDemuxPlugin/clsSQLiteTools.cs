//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2011, Battelle Memorial Institute
// Created 03/07/2011
//
// Last modified 03/07/2011
//*********************************************************************************************************
using System;
using System.Collections.Generic;
using System.Text;
using CaptureTaskManager;
using System.Data.SQLite;
using System.Data;

namespace ImsDemuxPlugin
{
	public class clsSQLiteTools
	{
		//*********************************************************************************************************
		// Tools for querying SQLite database (UIMF file, in this case)
		//**********************************************************************************************************

		#region "Enums"
			public enum UimfQueryResults
			{
				NonMultiplexed,
				Multiplexed,
				Error
			}
		#endregion

		#region "Constants"
			const string DEF_QUERY_STRING = "SELECT DISTINCT IMFProfile FROM Frame_Parameters";
		#endregion

		#region "Methods"
			/// <summary>
			/// Evaluates UMIF file to determine if it is multiplexed or not
			/// </summary>
			/// <param name="uimfFileNamePath">Full path to uimf file</param>
			/// <returns>Enum indicating test results</returns>
			public UimfQueryResults GetUimfMuxStatus(string uimfFileNamePath)
			{
				string connStr = "data source=" + uimfFileNamePath;
				string sqlStr = DEF_QUERY_STRING;

				// Get a data table containing the multiplexing information for the UIMF file
				DataTable queryResult = GetDataTable(sqlStr, connStr);

				// If null returned, there was an error
				if (queryResult == null) return UimfQueryResults.Error;

				// If empty table returned, there was an error
				if (queryResult.Rows.Count < 1)
				{
                    string msg = "No rows retrieved when querying UIMF file with " + sqlStr;
					clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
					return UimfQueryResults.Error;
				}

				// Evaluate results. If any row in table has a file name containing "bit" then de-multiplexing is required
				bool deMuxRequired = false;
				foreach (DataRow currRow in queryResult.Rows)
				{
					string testStr = "";
					object tmpObj = currRow[queryResult.Columns[0]];
					if (tmpObj == DBNull.Value)
					{
						// Null field means demux not required
						continue;
					}
					else
					{
						testStr = (string)tmpObj;

						// Empty string means demux not required
						if (testStr == "") continue;
					}

					// Get the file name, and check to see if it contains "bit"
					string fileName = System.IO.Path.GetFileName(testStr).ToLower();
					if (fileName.Contains("bit"))
					{
						// Filename contains "bit", so de-multiplexing is required
						deMuxRequired = true;
						// No need to check additional rows, if any
						break;
					}
				}

				// Return results
				if (deMuxRequired)
				{
					return UimfQueryResults.Multiplexed;
				}
				else return UimfQueryResults.NonMultiplexed;
			}	// End sub

			/// <summary>
			/// Gets a table from a UIMF file
			/// </summary>
			/// <param name="cmdStr">SQL query string</param>
			/// <param name="connStr">Connection string</param>
			/// <returns>Table containg query results</returns>
			private DataTable GetDataTable(string cmdStr, string connStr)
			{
				DataTable retTable = new DataTable();
				int filledRows = 0;

				using (SQLiteConnection cn = new SQLiteConnection(connStr))
				{
					using (SQLiteDataAdapter da = new SQLiteDataAdapter())
					{
						using (SQLiteCommand cmd = new SQLiteCommand(cmdStr, cn))
						{
							cmd.CommandType = CommandType.Text;
							da.SelectCommand = cmd;
							try
							{
								filledRows = da.Fill(retTable);
							}
							catch (Exception ex)
							{
								string msg = "Exception reading UIMF file: " + ex.Message;
								clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);
								return null;
							}
						}	// End using cmd
					}	// End using da
				}	// End using cn

				return retTable;
			} // End sub
		#endregion
	}	// End class
}	// End namespace
