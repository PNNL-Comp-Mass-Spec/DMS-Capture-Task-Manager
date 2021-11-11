//*********************************************************************************************************
// Written by Gary Kiebel and Dave Clark for the US Department of Energy
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 06/26/2009
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using Apache.NMS;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Commands;
using PRISM.AppSettings;

namespace CaptureTaskManager
{
    /// <summary>
    /// Received commands are sent to a delegate function with this signature
    /// </summary>
    /// <param name="cmdText"></param>
    // ReSharper disable once UnusedMember.Global
    public delegate void MessageProcessorDelegate(string cmdText);

    /// <summary>
    /// Handles sending and receiving of control and status messages
    /// </summary>
    internal class MessageHandler : LoggerBase, IDisposable
    {
        private MgrSettings mMgrSettings;

        private IConnection mConnection;
        private ISession mStatusSession;
        private IMessageProducer mStatusSender;

        private bool mIsDisposed;
        private bool mHasConnection;

        public MgrSettings MgrSettings
        {
            set => mMgrSettings = value;
        }

        public string BrokerUri { get; set; }

        public string StatusTopicName { get; set; }

        /// <summary>
        /// Create set of NMS connection objects necessary to talk to the ActiveMQ broker
        /// </summary>
        /// <param name="retryCount">Number of times to try the connection</param>
        /// <param name="timeoutSeconds">Number of seconds to wait for the broker to respond</param>
        protected void CreateConnection(int retryCount = 2, int timeoutSeconds = 15)
        {
            if (mHasConnection)
            {
                return;
            }

            if (retryCount < 0)
            {
                retryCount = 0;
            }

            var retriesRemaining = retryCount;

            if (timeoutSeconds < 5)
            {
                timeoutSeconds = 5;
            }

            var errorList = new List<string>();

            while (retriesRemaining >= 0)
            {
                try
                {
                    IConnectionFactory connectionFactory = new ConnectionFactory(BrokerUri, mMgrSettings.ManagerName);
                    mConnection = connectionFactory.CreateConnection();
                    mConnection.RequestTimeout = new TimeSpan(0, 0, timeoutSeconds);
                    mConnection.Start();

                    mHasConnection = true;

                    var username = System.Security.Principal.WindowsIdentity.GetCurrent().Name;

                    LogDebug("Connected to broker as user {0}", username);

                    return;
                }
                catch (Exception ex)
                {
                    // Connection failed
                    if (!errorList.Contains(ex.Message))
                    {
                        errorList.Add(ex.Message);
                    }

                    // Sleep for 3 seconds
                    System.Threading.Thread.Sleep(3000);
                }

                retriesRemaining--;
            }

            // If we get here, we never could connect to the message broker

            LogError("Exception creating broker connection{0}: {1}",
                retryCount > 0 ? " after " + (retryCount + 1) + " attempts" : string.Empty,
                string.Join("; ", errorList));
        }

        /// <summary>
        /// Create the message broker communication objects and register the listener function
        /// </summary>
        /// <returns>TRUE for success; FALSE otherwise</returns>
        public bool Init()
        {
            try
            {
                if (!mHasConnection)
                {
                    CreateConnection();
                }

                if (!mHasConnection)
                {
                    return false;
                }

                if (string.IsNullOrWhiteSpace(StatusTopicName))
                {
                    LogWarning("Status topic queue name is undefined");
                }
                else
                {
                    // topic for the capture tool manager to send status information over
                    mStatusSession = mConnection.CreateSession();
                    mStatusSender = mStatusSession.CreateProducer(new ActiveMQTopic(StatusTopicName));
                    LogDebug("Status sender established");
                }

                return true;
            }
            catch (Exception ex)
            {
                LogWarning("Exception while initializing message sessions: " + ex.Message);
                DestroyConnection();
                return false;
            }
        }

        /// <summary>
        /// Sends a status message
        /// </summary>
        /// <param name="message">Outgoing message string</param>
        public void SendMessage(string message)
        {
            if (!mIsDisposed)
            {
                var textMessage = mStatusSession.CreateTextMessage(message);
                textMessage.NMSTimeToLive = TimeSpan.FromMinutes(60);
                textMessage.NMSDeliveryMode = MsgDeliveryMode.NonPersistent;
                textMessage.Properties.SetString("ProcessorName", mMgrSettings.ManagerName);
                try
                {
                    mStatusSender.Send(textMessage);
                }
                catch
                {
                    // Do nothing
                }
            }
            else
            {
                throw new ObjectDisposedException(GetType().FullName);
            }
        }

        /// <summary>
        /// Cleans up a connection after error or when closing
        /// </summary>
        protected void DestroyConnection()
        {
            try
            {
                if (mHasConnection)
                {
                    mConnection?.Close();
                    mHasConnection = false;
                }
            }
            catch (Exception)
            {
                // Ignore errors here
            }
        }

        /// <summary>
        /// Implements IDisposable interface
        /// </summary>
        public void Dispose()
        {
            if (mIsDisposed)
            {
                return;
            }

            DestroyConnection();
            mIsDisposed = true;
        }
    }
}
