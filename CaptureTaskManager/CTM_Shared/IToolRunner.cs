//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/15/2009
//
//*********************************************************************************************************

namespace CaptureTaskManager
{
    public interface IToolRunner
    {
        //*********************************************************************************************************
        // Interface for step tool plugins
        //**********************************************************************************************************

        #region "Methods"

        void Setup(IMgrParams mgrParams, ITaskParams taskParams, IStatusFile statusTools);

        clsToolReturnData RunTool();

        #endregion
    }
}