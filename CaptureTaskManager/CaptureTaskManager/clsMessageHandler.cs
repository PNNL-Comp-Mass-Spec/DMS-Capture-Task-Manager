
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

	class clsMessageHandler : IDisposable
	{
		//*********************************************************************************************************
		// Handles sending and receiving of control and status messages
		// Base code provided by Gary Kiebel
		//**********************************************************************************************************

		#region "Class variables"
		private string m_BrokerUri;
		private string m_CommandQueueName;	// Not presently used
		private string m_BroadcastTopicName;	// Used for manager control functions (ie, start, read config)
		private string m_StatusTopicName;	// Used for status output
		private clsMgrSettings m_MgrSettings;

		private IConnection m_Connection;
		private ISession m_StatusSession;
		private IMessageProducer m_StatusSender;
		private IMessageConsumer m_CommandConsumer;
		private IMessageConsumer m_BroadcastConsumer;

		private bool m_IsDisposed;
		private bool m_HasConnection;
		#endregion

		#region "Events"
		public event MessageProcessorDelegate CommandReceived;
		public event MessageProcessorDelegate BroadcastReceived;
		#endregion

		#region "Properties"
		public clsMgrSettings MgrSettings
		{
			set
			{
				m_MgrSettings = value;
			}
		}

		public string BrokerUri
		{
			get { return m_BrokerUri; }
			set { m_BrokerUri = value; }
		}

		public string CommandQueueName
		{
			get { return m_CommandQueueName; }
			set { m_CommandQueueName = value; }
		}

		public string BroadcastTopicName
		{
			get { return m_BroadcastTopicName; }
			set { m_BroadcastTopicName = value; }
		}

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
			if (m_HasConnection) return;
            
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
		            // temp debug
		            // Console.WriteLine("--- New connection made ---" + Environment.NewLine); //+ e.ToString()

                    clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Connected to broker");
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

            clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg);

		}

		/// <summary>
		/// Create the message broker communication objects and register the listener function
		/// </summary>
		/// <returns>TRUE for success; FALSE otherwise</returns>
		public bool Init()
		{
			try
			{
				if (!m_HasConnection) CreateConnection();
				if (!m_HasConnection) return false;

				// queue for telling manager to perform task (future?)
				var commandSession = m_Connection.CreateSession();
				m_CommandConsumer = commandSession.CreateConsumer(new ActiveMQQueue(m_CommandQueueName));
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Command listener established");

				// topic for commands broadcast to all capture tool managers
				var broadcastSession = m_Connection.CreateSession();
				m_BroadcastConsumer = broadcastSession.CreateConsumer(new ActiveMQTopic(m_BroadcastTopicName));
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Broadcast listener established");

				// topic for the capture tool manager to send status information over
				m_StatusSession = m_Connection.CreateSession();
				m_StatusSender = m_StatusSession.CreateProducer(new ActiveMQTopic(m_StatusTopicName));
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Status sender established");

				return true;
			}
			catch (Exception ex)
			{
				var msg = "Exception while initializing message sessions";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, ex);
				DestroyConnection();
				return false;
			}
		}

		/// <summary>
		/// Command listener function. Received commands will cause this to be called
		///	and it will trigger an event to pass on the command to all registered listeners
		/// </summary>
		/// <param name="message">Incoming message</param>
		private void OnCommandReceived(IMessage message)
		{
			var textMessage = message as ITextMessage;
			var Msg = "clsMessageHandler(), Command message received";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg);
			if (CommandReceived != null)
			{
				// call the delegate to process the commnd
				Msg = "clsMessageHandler().OnCommandReceived: At lease one event handler assigned";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg);
			    if (textMessage != null)
			    {
			        CommandReceived(textMessage.Text);
			    }
			}
			else
			{
				Msg = "clsMessageHandler().OnCommandReceived: No event handlers assigned";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg);
			}
		}

		/// <summary>
		/// Broadcast listener function. Received Broadcasts will cause this to be called
		///	and it will trigger an event to pass on the command to all registered listeners
		/// </summary>
		/// <param name="message">Incoming message</param>
		private void OnBroadcastReceived(IMessage message)
		{
			var textMessage = message as ITextMessage;
			var msg = "clsMessageHandler(), Broadcast message received";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
			if (BroadcastReceived != null)
			{
				// call the delegate to process the commnd
				msg = "clsMessageHandler().OnBroadcastReceived: At lease one event handler assigned";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
			    if (textMessage != null)
			    {
			        BroadcastReceived(textMessage.Text);
			    }
			}
			else
			{
				msg = "clsMessageHandler().OnBroadcastReceived: No event handlers assigned";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
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
                textMessage.Properties.SetString("ProcessorName", m_MgrSettings.GetParam(clsMgrSettings.MGR_PARAM_MGR_NAME));
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
				var msg = "Message connection closed";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
			}
		}

		/// <summary>
		/// Implements IDisposable interface
		/// </summary>
		public void Dispose()
		{
			if (!m_IsDisposed)
			{
				DestroyConnection();
				m_IsDisposed = true;
			}
		}

		/// <summary>
		/// Registers the command and broadcast listeners under control of main program.
		/// This is done to prevent loss of queued messages if listeners are registered too early.
		/// </summary>
		public void RegisterListeners()
		{
			m_CommandConsumer.Listener += OnCommandReceived;
			m_BroadcastConsumer.Listener += OnBroadcastReceived;
		}
		#endregion
	}	// End class
}	// End namespace
