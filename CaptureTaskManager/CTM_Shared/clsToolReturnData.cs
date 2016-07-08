//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 10/05/2009
//
//*********************************************************************************************************

namespace CaptureTaskManager
{
    public class clsToolReturnData
    {
        //*********************************************************************************************************
        // Holds data to be returned from step tool operations
        //**********************************************************************************************************

        #region "Class variables"

        EnumCloseOutType m_CloseoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
        string m_CloseoutMsg = "";
        EnumEvalCode m_EvalCode = EnumEvalCode.EVAL_CODE_SUCCESS;
        string m_EvalMsg = "";

        #endregion

        #region "Properties"

        public EnumCloseOutType CloseoutType
        {
            get { return m_CloseoutType; }
            set { m_CloseoutType = value; }
        }

        public string CloseoutMsg
        {
            get { return m_CloseoutMsg; }
            set { m_CloseoutMsg = value; }
        }

        public EnumEvalCode EvalCode
        {
            get { return m_EvalCode; }
            set { m_EvalCode = value; }
        }

        public string EvalMsg
        {
            get { return m_EvalMsg; }
            set { m_EvalMsg = value; }
        }

        #endregion
    }
}