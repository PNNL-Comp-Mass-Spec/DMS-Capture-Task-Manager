﻿//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/05/2009
//*********************************************************************************************************

namespace CaptureTaskManager
{
    /// <summary>
    /// Holds data to be returned from step tool operations
    /// </summary>
    public class ToolReturnData
    {
        public EnumCloseOutType CloseoutType { get; set; }

        public string CloseoutMsg { get; set; }

        public EnumEvalCode EvalCode { get; set; }

        public string EvalMsg { get; set; }

        /// <summary>
        /// Constructor
        /// </summary>
        public ToolReturnData()
        {
            CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            CloseoutMsg = string.Empty;
            EvalCode = EnumEvalCode.EVAL_CODE_SUCCESS;
            EvalMsg = string.Empty;
        }
    }
}