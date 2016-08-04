//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/01/2009
//
//*********************************************************************************************************

using System.Collections.Generic;

namespace CaptureTaskManager
{
    class clsBroadcastCmd
    {
        //*********************************************************************************************************
        // Class to hold data receieved from Broadcast command queue for control of manager
        //**********************************************************************************************************

        #region "Class variables"

        #endregion

        #region "Properties"

        /// <summary>
        /// List of machines the received message applies to
        /// </summary>
        public List<string> MachineList { get; }

        // The command that was broadcast
        public string MachCmd { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public clsBroadcastCmd()
        {
            MachineList = new List<string>();
        }

        #endregion
    }
}