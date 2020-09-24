//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/10/2009
//*********************************************************************************************************

using System.Collections.Generic;

namespace CaptureTaskManager
{
    /// <summary>
    /// Holds long-term data for status reporting.
    /// </summary>
    /// <remarks>
    /// Static class to avoid adding an instance of the status file class to the log tools class
    /// </remarks>
    public static class clsStatusData
    {
        #region "Class wide variables"

        private static string mMostRecentLogMessage;

        #endregion

        #region "Properties"

        public static string MostRecentLogMessage
        {
            get => mMostRecentLogMessage;
            set
            {
                // Filter out routine startup and shutdown messages
                if (value.Contains("=== Started") || (value.Contains("===== Closing")))
                {
                    // Do nothing
                }
                else
                {
                    mMostRecentLogMessage = value;
                }
            }
        }

        public static Queue<string> ErrorQueue { get; } = new Queue<string>();

        #endregion

        #region "Methods"

        public static void AddErrorMessage(string ErrMsg)
        {
            // Add the most recent error message
            ErrorQueue.Enqueue(ErrMsg);

            // If there are > 4 entries in the queue, delete the oldest ones
            while (ErrorQueue.Count > 4)
            {
                ErrorQueue.Dequeue();
            }
        }

        #endregion
    }
}
