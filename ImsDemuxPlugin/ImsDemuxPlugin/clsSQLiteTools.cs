//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2011, Battelle Memorial Institute
// Created 03/07/2011

//*********************************************************************************************************

using System;
using System.Data.SQLite;
using System.Data;
using System.Text.RegularExpressions;
using PRISM;

namespace ImsDemuxPlugin
{
    /// <summary>
    /// Tools for querying SQLite database (UIMF file, in this case)
    /// </summary>
    public class clsSQLiteTools : clsEventNotifier
    {

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
        /// Evaluates the UMIF file to determine if it is multiplexed or not
        /// </summary>
        /// <param name="uimfFilePath">Full path to uimf file</param>
        /// <param name="numBitsForEncoding">Number of bits used for encoding; 0 if not multiplexed</param>
        /// <returns>Enum indicating test results</returns>
        public UimfQueryResults GetUimfMuxStatus(string uimfFilePath, out byte numBitsForEncoding)
        {
            var connStr = "data source=" + uimfFilePath + ";Version=3;Read Only=True;";
            const string sqlStr = DEF_QUERY_STRING;

            numBitsForEncoding = 0;

            // Get a data table containing the multiplexing information for the UIMF file
            var queryResult = GetDataTable(sqlStr, connStr);

            // If null returned, there was an error
            if (queryResult == null)
                return UimfQueryResults.Error;

            // If empty table returned, there was an error
            if (queryResult.Rows.Count < 1)
            {
                const string msg = "No rows retrieved when querying UIMF file with " + sqlStr;
                OnErrorEvent(msg);
                return UimfQueryResults.Error;
            }

            var reBitValue = new Regex(@"^(\d)bit", RegexOptions.Compiled | RegexOptions.IgnoreCase);
            var imfProfileFieldAlwaysBlank = true;

            // Evaluate results. If any row in table has a file name containing "bit" then de-multiplexing is required
            // In addition, parse the "bit" value to determine numBitsForEncoding
            var deMuxRequired = false;
            foreach (DataRow currRow in queryResult.Rows)
            {
                var tmpObj = currRow[queryResult.Columns[0]];
                if (tmpObj == DBNull.Value)
                {
                    // Null field means demux not required
                    continue;
                }

                var testStr = (string)tmpObj;

                // Empty string means demux not required
                if (string.IsNullOrEmpty(testStr))
                    continue;

                imfProfileFieldAlwaysBlank = false;

                // Get the file name, and check to see if it starts with 3bit (or 4bit or 5bit etc.)
                var multiplexFilename = System.IO.Path.GetFileName(testStr).ToLower();
                var reMatch = reBitValue.Match(multiplexFilename);
                if (reMatch.Success)
                {
                    // Multiplex Filename contains "bit", so de-multiplexing is required
                    deMuxRequired = true;

                    byte.TryParse(reMatch.Groups[1].Value, out numBitsForEncoding);

                    // No need to check additional rows; we assume the same multiplexing is used throughout the dataset
                    break;
                }
            }

            if (imfProfileFieldAlwaysBlank)
            {
                // Examine the filename to determine if it is a multiplexed dataset
                // This RegEx will match filenames of this format:
                //   BSA_65min_0pt5uL_1pt5ms_4bit_0001
                //   BSA_65min_0pt5uL_1pt5ms_4bit

                reBitValue = new Regex(@"(_(\d)bit_|_(\d)bit$)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

                var fileName = System.IO.Path.GetFileNameWithoutExtension(uimfFilePath);
                if (fileName != null)
                {
                    var reMatch = reBitValue.Match(fileName);
                    if (reMatch.Success)
                    {
                        // Filename contains "bit", so de-multiplexing is required
                        deMuxRequired = true;
                        byte.TryParse(reMatch.Groups[2].Value, out numBitsForEncoding);
                    }
                }
            }

            // Return results
            if (deMuxRequired)
            {
                return UimfQueryResults.Multiplexed;
            }

            return UimfQueryResults.NonMultiplexed;
        }

        /// <summary>
        /// Gets a table from a UIMF file
        /// </summary>
        /// <param name="cmdStr">SQL query string</param>
        /// <param name="connStr">Connection string</param>
        /// <returns>Table containg query results</returns>
        private DataTable GetDataTable(string cmdStr, string connStr)
        {
            // Note: providing true for parseViaFramework as a workaround for reading SqLite files located on a remote UNC share or in readonly folders
            using (var cn = new SQLiteConnection(connStr, true))
            {
                using (var da = new SQLiteDataAdapter())
                {
                    using (var cmd = new SQLiteCommand(cmdStr, cn))
                    {
                        cmd.CommandType = CommandType.Text;
                        da.SelectCommand = cmd;
                        try
                        {
                            var retTable = new DataTable();
                            da.Fill(retTable);
                            return retTable;
                        }
                        catch (Exception ex)
                        {
                            var msg = "Exception reading UIMF file: " + ex.Message;
                            OnErrorEvent(msg, ex);
                            return null;
                        }
                    }
                }
            }

            // Alternative method:
            //using (var cn = new SQLiteConnection(connStr, true)) {
            //    cn.Open();
            //    using (var command = new SQLiteCommand(cmdStr, cn)) {
            //        command.CommandType = CommandType.Text;
            //        using (SQLiteDataReader reader = command.ExecuteReader()) {
            //            var retTable = new DataTable();
            //            retTable.Load(reader);
            //            return retTable;
            //        } }	}

        }
        #endregion
    }
}
