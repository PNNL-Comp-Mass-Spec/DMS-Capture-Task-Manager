//*********************************************************************************************************
// Written by Gary Kiebel and Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 06/26/2009
//
//*********************************************************************************************************

using System;
using System.Collections.Generic;
using Apache.NMS;
using Apache.NMS.ActiveMQ;
using Apache.NMS.ActiveMQ.Commands;

namespace CaptureTaskManager
{
    // received commands are sent to a delegate function with this signature
    public delegate void MessageProcessorDelegate(string cmdText);

    class clsMessageHandler : clsLoggerBase, IDisposable
    {
        //*********************************************************************************************************
        // Handles sending and receiving of control and status messages
        // Base code provided by Gary Kiebel
        //**********************************************************************************************************

        #region "Class variables"

        private string m_BrokerUri;

        private string m_StatusTopicName;       // Used for status output
        private clsMgrSettings m_MgrSettings;

        private IConnection m_Connection;
        private ISession m_StatusSession;
        private IMessageProducer m_StatusSender;

        private bool m_IsDisposed;
        private bool m_HasConnection;

        #endregion

        #region "Events"

        #endregion

        #region "Properties"

        public clsMgrSettings MgrSettings
        {
            set { m_MgrSettings = value; }
        }

        public string BrokerUri
        {
            get { return m_BrokerUri; }
            set { m_BrokerUri = value; }
        }

        //[Obsolete("Unused")]
        //public string CommandQueueName
        //{
        //    get { return m_CommandQueueName; }
        //    set { m_CommandQueueName = value; }
        //}

        //[Obsolete("Unused")]
        //public string BroadcastTopicName
        //{
        //    get { return m_BroadcastTopicName; }
        //    set { m_BroadcastTopicName = value; }
        //}

        public string StatusTopicName
        {
            get { return m_StatusTopicName; }
            set { m_StatusTopicName = value; }
        }

        #endregion

        #region "Methods"

        /// <summary>
        /// Create set of NMS connection objects necessary to talk to the ActiveMQ broker
        /// </summary>
        /// <param name="retryCount">Number of times to try the connection</param>
        /// <param name="timeoutSeconds">Number of seconds to wait for the broker to respond</param>
        protected void CreateConnection(int retryCount = 2, int timeoutSeconds = 15)
        {
            if (m_HasConnection)
                return;

            if (retryCount < 0)
                retryCount = 0;

            var retriesRemaining = retryCount;

            if (timeoutSeconds < 5)
                timeoutSeconds = 5;

            var errorList = new List<string>();

            while (retriesRemaining >= 0)
            {
                try
                {
                    IConnectionFactory connectionFactory = new ConnectionFactory(m_BrokerUri);
                    m_Connection = connectionFactory.CreateConnection();
                    m_Connection.RequestTimeout = new TimeSpan(0, 0, timeoutSeconds);
                    m_Connection.Start();

                    m_HasConnection = true;

                    LogDebug("Connected to broker");
                    return;
                }
                catch (Exception ex)
                {
                    // Connection failed
                    if (!errorList.Contains(ex.Message))
                        errorList.Add(ex.Message);

                    // Sleep for 3 seconds
                    System.Threading.Thread.Sleep(3000);
                }

                retriesRemaining -= 1;
            }

            // If we get here, we never could connect to the message broker

            var msg = "Exception creating broker connection";
            if (retryCount > 0)
                msg += " after " + (retryCount + 1) + " attempts";

            msg += ": " + string.Join("; ", errorList);

            LogError(msg);
        }

        /// <summary>
        /// Create the message broker communication objects and register the listener function
        /// </summary>
        /// <returns>TRUE for success; FALSE otherwise</returns>
        public bool Init()
        {
            try
            {
                if (!m_HasConnection)
                    CreateConnection();

                if (!m_HasConnection)
                    return false;

                if (string.IsNullOrWhiteSpace(m_StatusTopicName))
                {
                    LogWarning("Status topic queue name is undefined");
                }
                else
                {
                    // topic for the capture tool manager to send status information over
                    m_StatusSession = m_Connection.CreateSession();
                    m_StatusSender = m_StatusSession.CreateProducer(new ActiveMQTopic(m_StatusTopicName));
                    LogDebug("Status sender established");
                }

                return true;
            }
            catch (Exception ex)
            {
                LogError("Exception while initializing message sessions", ex);
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
            if (!m_IsDisposed)
            {
                var textMessage = m_StatusSession.CreateTextMessage(message);
                textMessage.Properties.SetString("ProcessorName",
                                                 m_MgrSettings.GetParam(clsMgrSettings.MGR_PARAM_MGR_NAME));
                try
                {
                    m_StatusSender.Send(textMessage);
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

        #endregion

        #region "Cleanup"

        /// <summary>
        /// Cleans up a connection after error or when closing
        /// </summary>
        protected void DestroyConnection()
        {
            if (m_HasConnection)
            {
                m_Connection.Dispose();
                m_HasConnection = false;
                LogDebug("Message connection closed");
            }
        }

        /// <summary>
        /// Implements IDisposable interface
        /// </summary>
        public void Dispose()
        {
            if (m_IsDisposed)
                return;

            DestroyConnection();
            m_IsDisposed = true;
        }

        // <summary>
        // Registers the command and broadcast listeners under control of main program.
        // This is done to prevent loss of queued messages if listeners are registered too early.
        // </summary>
        // [Obsolete("Unused")]
        // public void RegisterListeners()
        // {
        //    m_CommandConsumer.Listener += OnCommandReceived;
        //    m_BroadcastConsumer.Listener += OnBroadcastReceived;
        // }

        #endregion
    }
}
