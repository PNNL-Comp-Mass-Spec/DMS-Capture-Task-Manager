using System;
using CaptureTaskManager;
using PRISM;

namespace CaptureToolPlugin
{
    internal class ShareConnection : LoggerBase
    {
        // Ignore Spelling: bio, bionet, dotnet, fso, secfso, Username

        public enum ConnectionType
        {
            NotConnected,
            Prism,
            DotNET
        }

        /// <summary>
        /// Username for connecting to bionet
        /// </summary>
        private readonly string mUserName = string.Empty;

        private ShareConnector mShareConnectorPRISM;
        private NetworkConnection mShareConnectorDotNET;
        private ConnectionType mConnectionType = ConnectionType.NotConnected;
        private readonly ConnectionType mConnectionTypeToUse;
        private readonly SharedState mToolState;

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="toolState">SharedState object for tracking critical errors</param>
        /// <param name="mgrParams">Parameters for manager operation</param>
        /// <param name="useBioNet">Flag to indicate if source instrument is on Bionet</param>
        public ShareConnection(SharedState toolState, IMgrParams mgrParams, bool useBioNet)
        {
            mToolState = toolState;

            // Setup for Bionet use, if applicable
            if (useBioNet)
            {
                mUserName = mgrParams.GetParam("BionetUser");

                if (!mUserName.Contains(@"\"))
                {
                    // Prepend this computer's name to the username
                    mUserName = System.Net.Dns.GetHostName() + @"\" + mUserName;
                }
            }

            var shareConnectorType = mgrParams.GetParam("ShareConnectorType");             // Can be PRISM or DotNET (but has been PRISM since 2012)

            // Determine whether the connector class should be used to connect to Bionet
            // This is defined by manager parameter ShareConnectorType
            // Default in October 2014 is PRISM
            mConnectionTypeToUse = string.Equals(shareConnectorType, "dotnet", StringComparison.OrdinalIgnoreCase)
                ? ConnectionType.DotNET
                : ConnectionType.Prism;
        }

        /// <summary>
        /// Returns a string that describes the username and connection method currently active
        /// </summary>
        public string GetConnectionDescription()
        {
            return mConnectionType switch
            {
                ConnectionType.NotConnected => " as user " + Environment.UserName + " using fso",
                ConnectionType.DotNET => " as user " + mUserName + " using CaptureTaskManager.NetworkConnection",
                ConnectionType.Prism => " as user " + mUserName + " using PRISM.ShareConnector",
                _ => " via unknown connection mode"
            };
        }

        /// <summary>
        /// Connect to a Bionet share using either mShareConnectorPRISM or mShareConnectorDotNET
        /// </summary>
        /// <param name="userName">Username</param>
        /// <param name="password">Password</param>
        /// <param name="directorySharePath">Share path</param>
        /// <param name="closeoutType">Closeout code (output)</param>
        /// <param name="evalCode"></param>
        /// <returns>True if success, false if an error</returns>
        public bool ConnectToShare(
            string userName,
            string password,
            string directorySharePath,
            out EnumCloseOutType closeoutType,
            out EnumEvalCode evalCode)
        {
            bool success;

            // ReSharper disable once ConvertIfStatementToConditionalTernaryExpression
            if (mConnectionTypeToUse == ConnectionType.DotNET)
            {
                success = ConnectToShare(userName, password, directorySharePath, out mShareConnectorDotNET, out closeoutType, out evalCode);
            }
            else
            {
                // Assume Prism Connector
                success = ConnectToShare(userName, password, directorySharePath, out mShareConnectorPRISM, out closeoutType, out evalCode);
            }

            return success;
        }

        /// <summary>
        /// Connect to a remote share using a specific username and password
        /// Uses class PRISM.ShareConnector
        /// </summary>
        /// <param name="userName">Username</param>
        /// <param name="password">Password</param>
        /// <param name="shareDirectoryPath">Share path</param>
        /// <param name="myConn">Connection object (output)</param>
        /// <param name="closeoutType">Closeout code (output)</param>
        /// <param name="evalCode"></param>
        /// <returns>True if success, false if an error</returns>
        private bool ConnectToShare(
            string userName,
            string password,
            string shareDirectoryPath,
            out ShareConnector myConn,
            out EnumCloseOutType closeoutType,
            out EnumEvalCode evalCode)
        {
            closeoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
            evalCode = EnumEvalCode.EVAL_CODE_SUCCESS;

            myConn = new ShareConnector(userName, password)
            {
                Share = shareDirectoryPath
            };

            if (myConn.Connect())
            {
                LogDebug("Connected to Bionet (" + shareDirectoryPath + ") as user " + userName + " using PRISM.ShareConnector");
                mConnectionType = ConnectionType.Prism;
                return true;
            }

            mToolState.ErrorMessage = "Error " + myConn.ErrorMessage + " connecting to " + shareDirectoryPath + " as user " + userName + " using 'secfso'";

            var msg = mToolState.ErrorMessage;

            if (myConn.ErrorMessage == "1326")
            {
                msg += "; you likely need to change the Capture_Method from secfso to fso";
            }

            if (myConn.ErrorMessage == "53")
            {
                msg += "; the password may need to be reset";
            }

            LogError(msg);

            if (myConn.ErrorMessage is "1219" or "1203" or "53" or "64")
            {
                // Likely had error "An unexpected network error occurred" while copying a file for a previous dataset
                // Need to completely exit the capture task manager
                mToolState.SetAbortProcessing();
                closeoutType = EnumCloseOutType.CLOSEOUT_NEED_TO_ABORT_PROCESSING;
                evalCode = EnumEvalCode.EVAL_CODE_NETWORK_ERROR_RETRY_CAPTURE;
            }
            else
            {
                closeoutType = EnumCloseOutType.CLOSEOUT_FAILED;
            }

            mConnectionType = ConnectionType.NotConnected;
            return false;
        }

        /// <summary>
        /// Connect to a remote share using a specific username and password
        /// Uses class CaptureTaskManager.NetworkConnection
        /// </summary>
        /// <param name="userName">Username</param>
        /// <param name="password">Password</param>
        /// <param name="directorySharePath">Remote share path</param>
        /// <param name="myConn">Connection object (output)</param>
        /// <param name="closeoutType">Closeout code (output)</param>
        /// <param name="evalCode"></param>
        /// <returns>True if success, false if an error</returns>
        private bool ConnectToShare(
            string userName,
            string password,
            string directorySharePath,
            out NetworkConnection myConn,
            out EnumCloseOutType closeoutType,
            out EnumEvalCode evalCode)
        {
            evalCode = EnumEvalCode.EVAL_CODE_SUCCESS;
            myConn = null;

            try
            {
                // Make sure directorySharePath does not end in a backslash
                if (directorySharePath.EndsWith(@"\"))
                {
                    directorySharePath = directorySharePath.Substring(0, directorySharePath.Length - 1);
                }

                var accessCredentials = new System.Net.NetworkCredential(userName, password, string.Empty);

                myConn = new NetworkConnection(directorySharePath, accessCredentials);

                LogDebug("Connected to Bionet (" + directorySharePath + ") as user " + userName + " using CaptureTaskManager.NetworkConnection");
                mConnectionType = ConnectionType.DotNET;

                closeoutType = EnumCloseOutType.CLOSEOUT_SUCCESS;
                return true;
            }
            catch (Exception ex)
            {
                mToolState.ErrorMessage = "Error connecting to " + directorySharePath + " as user " + userName + " (using NetworkConnection class)";
                LogError(mToolState.ErrorMessage, ex);

                var returnData = new ToolReturnData();
                mToolState.HandleCopyException(returnData, ex);

                closeoutType = returnData.CloseoutType;
                evalCode = returnData.EvalCode;

                mConnectionType = ConnectionType.NotConnected;
                return false;
            }
        }

        /// <summary>
        /// Disconnect from a bionet share if required
        /// </summary>
        public void DisconnectShareIfRequired()
        {
            if (mConnectionType == ConnectionType.Prism)
            {
                DisconnectShare(ref mShareConnectorPRISM);
            }
            else if (mConnectionType == ConnectionType.DotNET)
            {
                DisconnectShare(ref mShareConnectorDotNET);
            }
        }

        /// <summary>
        /// Disconnects a Bionet shared drive
        /// </summary>
        /// <param name="myConn">Connection object (class PRISM.ShareConnector) for shared drive</param>
        private void DisconnectShare(ref ShareConnector myConn)
        {
            myConn.Disconnect();
            AppUtils.GarbageCollectNow();

            LogDebug("Bionet disconnected");
            mConnectionType = ConnectionType.NotConnected;
        }

        /// <summary>
        /// Disconnects a Bionet shared drive
        /// </summary>
        /// <param name="myConn">Connection object (class CaptureTaskManager.NetworkConnection) for shared drive</param>
        private void DisconnectShare(ref NetworkConnection myConn)
        {
            myConn.Dispose();
            myConn = null;
            AppUtils.GarbageCollectNow();

            LogDebug("Bionet disconnected");
            mConnectionType = ConnectionType.NotConnected;
        }
    }
}
