﻿//*********************************************************************************************************
// Written by Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 09/15/2009
//*********************************************************************************************************

// ReSharper disable UnusedMember.Global

using System;

namespace CaptureTaskManager
{
    /// <summary>
    /// Interface for step task parameters
    /// </summary>
    public interface ITaskParams
    {
        System.Collections.Generic.Dictionary<string, string> TaskDictionary { get; }

        string GetParam(string name);
        string GetParam(string name, string valueIfMissing);
        bool GetParam(string name, bool valueIfMissing);
        float GetParam(string name, float valueIfMissing);
        int GetParam(string name, int valueIfMissing);
        DateTime GetParamAsDate(string name, DateTime valueIfMissing = default, string dateFormat = "yyyy-MM-dd HH:mm:ss");

        bool HasParam(string name);

        bool AddAdditionalParameter(string paramName, string paramValue);
        void SetParam(string keyName, string value);
    }
}