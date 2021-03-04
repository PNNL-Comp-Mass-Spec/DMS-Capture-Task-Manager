//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/15/2009
//*********************************************************************************************************

// ReSharper disable UnusedMember.Global

namespace CaptureTaskManager
{
    /// <summary>
    /// Interface for step task parameters
    /// </summary>
    public interface ITaskParams
    {
        #region "Properties"

        System.Collections.Generic.Dictionary<string, string> TaskDictionary { get; }

        #endregion

        #region "Methods"

        string GetParam(string name);
        string GetParam(string name, string valueIfMissing);
        bool GetParam(string name, bool valueIfMissing);
        float GetParam(string name, float valueIfMissing);
        int GetParam(string name, int valueIfMissing);

        bool HasParam(string name);

        bool AddAdditionalParameter(string paramName, string paramValue);
        void SetParam(string keyName, string value);

        #endregion
    }
}