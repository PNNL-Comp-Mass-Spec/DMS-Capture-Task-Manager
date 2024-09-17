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
        // Ignore Spelling: tcp

        event StatusMonitorUpdateReceived MonitorUpdateRequired;

        /// <summary>
        /// Manager status
        /// </summary>
        EnumMgrStatus MgrStatus { get; set; }

        /// <summary>
        /// Overall CPU utilization of all threads
        /// </summary>
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

        void ClearCachedInfo();

        void ConfigureMessageQueueLogging(bool logStatusToMessageQueue, string msgQueueURI, string messageQueueTopicMgrStatus);

        void CreateStatusFlagFile();

        bool DeleteStatusFlagFile();

        bool DetectStatusFlagFile();

        /// <summary>
        /// Update cached progress, write the status file, and optionally send the status to the message queue
        /// </summary>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        void UpdateAndWrite(float percentComplete);

        /// <summary>
        /// Update cached progress, write the status file, and optionally send the status to the message queue
        /// </summary>
        /// <param name="status">Job status enum</param>
        /// <param name="percentComplete">Job completion percentage (value between 0 and 100)</param>
        void UpdateAndWrite(EnumTaskStatusDetail status, float percentComplete);

        void UpdateStopped(bool mgrError);

        void UpdateDisabled(bool disabledLocally);

        void UpdateIdle();

        /// <summary>
        /// Writes out a new status file, indicating that the manager is still alive
        /// </summary>
        void WriteStatusFile();
    }
}