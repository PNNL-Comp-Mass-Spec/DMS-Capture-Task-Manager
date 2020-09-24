//*********************************************************************************************************
// Written by Dave Clark and Matthew Monroe for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 06/18/2009
//*********************************************************************************************************

using System;

// ReSharper disable UnusedMember.Global
namespace CaptureTaskManager
{
    /// <summary>
    /// Interface used by classes that create and update task status file
    /// </summary>
    public interface IStatusFile
    {
        #region "Events"

        event StatusMonitorUpdateReceived MonitorUpdateRequired;

        #endregion

        #region "Properties"

        /// <summary>
        /// Status file path
        /// </summary>
        string FileNamePath { get; set; }

        /// <summary>
        /// Manager name
        /// </summary>
        string MgrName { get; set; }

        /// <summary>
        /// Manager status
        /// </summary>
        EnumMgrStatus MgrStatus { get; set; }

        /// <summary>
        /// Overall CPU utilization of all threads
        /// </summary>
        /// <remarks></remarks>
        int CpuUtilization { get; set; }

        /// <summary>
        /// Step tool name
        /// </summary>
        string Tool { get; set; }

        /// <summary>
        /// Task status
        /// </summary>
        EnumTaskStatus TaskStatus { get; set; }

        /// <summary>
        /// Task start time (UTC-based)
        /// </summary>
        DateTime TaskStartTime { get; set; }

        /// <summary>
        /// Progress (value between 0 and 100)
        /// </summary>
        float Progress { get; set; }

        /// <summary>
        /// Current task
        /// </summary>
        string CurrentOperation { get; set; }

        /// <summary>
        /// Task status detail
        /// </summary>
        EnumTaskStatusDetail TaskStatusDetail { get; set; }

        /// <summary>
        /// Job number
        /// </summary>
        int JobNumber { get; set; }

        /// <summary>
        /// Step number
        /// </summary>
        int JobStep { get; set; }

        /// <summary>
        /// Dataset name
        /// </summary>
        string Dataset { get; set; }

        /// <summary>
        /// Most recent job info
        /// </summary>
        string MostRecentJobInfo { get; set; }

        /// <summary>
        /// Flag file path
        /// </summary>
        string FlagFilePath { get; }

        /// <summary>
        /// URI for the manager status message queue, e.g. tcp://Proto-7.pnl.gov:61616
        /// </summary>
        string MessageQueueURI { get; }

        /// <summary>
        /// Topic name for the manager status message queue
        /// </summary>
        string MessageQueueTopic { get; }

        /// <summary>
        /// When true, the status XML is being sent to the manager status message queue
        /// </summary>
        bool LogToMsgQueue { get; }

        #endregion

        #region "Methods"

        void ClearCachedInfo();

        void ConfigureMessageQueueLogging(bool logStatusToMessageQueue, string msgQueueURI, string messageQueueTopicMgrStatus);

        void CreateStatusFlagFile();

        bool DeleteStatusFlagFile();

        bool DetectStatusFlagFile();

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        /// <remarks></remarks>
        void UpdateAndWrite(float percentComplete);

        /// <summary>
        /// Updates status file
        /// </summary>
        /// <param name="status">Job status enum</param>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        /// <remarks></remarks>
        void UpdateAndWrite(EnumTaskStatusDetail status, float percentComplete);

        void UpdateStopped(bool mgrError);

        void UpdateDisabled(bool disabledLocally);

        void UpdateIdle();

        /// <summary>
        /// Writes out a new status file, indicating that the manager is still alive
        /// </summary>
        /// <remarks></remarks>
        void WriteStatusFile();

        #endregion
    }
}