
//*********************************************************************************************************
// Written by Gary Kiebel and Dave Clark for the US Department of Energy 
// Pacific Northwest National Laboratory, Richland, WA
// Copyright 2009, Battelle Memorial Institute
// Created 06/26/2009
//
// Last modified 06/26/2009
//*********************************************************************************************************
using System;
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
		private string m_BrokerUri = null;
		private string m_CommandQueueName = null;	// Not presently used
		private string m_BroadcastTopicName = null;	// Used for manager control functions (ie, start, read config)
		private string m_StatusTopicName = null;	// Used for status output
		private clsMgrSettings m_MgrSettings = null;

		private IConnection m_Connection;
		private ISession m_StatusSession;
		private IMessageProducer m_StatusSender;
		private IMessageConsumer m_CommandConsumer;
		private IMessageConsumer m_BroadcastConsumer;

		private bool m_IsDisposed = false;
		private bool m_HasConnection = false;
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
		/// create set of NMS connection objects necessary to talk to the ActiveMQ broker
		/// </summary>
		protected void CreateConnection()
		{
			if (m_HasConnection) return;
			try
			{
				IConnectionFactory connectionFactory = new ConnectionFactory(this.m_BrokerUri);
				this.m_Connection = connectionFactory.CreateConnection();
				this.m_Connection.RequestTimeout = new System.TimeSpan(0, 0, 15);
				this.m_Connection.Start();

				this.m_HasConnection = true;
				// temp debug
				// Console.WriteLine("--- New connection made ---" + Environment.NewLine); //+ e.ToString()
				string msg = "Connected to broker";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, msg);
			}
			catch (Exception Ex)
			{
				// we couldn't make a viable set of connection objects 
				// - this has "long day" written all over it,
				// but we don't have to do anything specific at this point (except eat the exception)

				// Console.WriteLine("=== Error creating connection ===" + Environment.NewLine); //+ e.ToString() // temp debug
				string msg = "Exception creating broker connection";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, Ex);
			}
		}	// End sub

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
				ISession commandSession = m_Connection.CreateSession();
				m_CommandConsumer = commandSession.CreateConsumer(new ActiveMQQueue(this.m_CommandQueueName));
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Command listener established");

				// topic for commands broadcast to all capture tool managers
				ISession broadcastSession = m_Connection.CreateSession();
				m_BroadcastConsumer = broadcastSession.CreateConsumer(new ActiveMQTopic(this.m_BroadcastTopicName));
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Broadcast listener established");

				// topic for the capture tool manager to send status information over
				this.m_StatusSession = m_Connection.CreateSession();
				this.m_StatusSender = m_StatusSession.CreateProducer(new ActiveMQTopic(m_StatusTopicName));
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, "Status sender established");

				return true;
			}
			catch (Exception Ex)
			{
				string msg = "Exception while initializing message sessions";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.ERROR, msg, Ex);
				DestroyConnection();
				return false;
			}
		}	// End sub

		/// <summary>
		/// Command listener function. Received commands will cause this to be called
		///	and it will trigger an event to pass on the command to all registered listeners
		/// </summary>
		/// <param name="message">Incoming message</param>
		private void OnCommandReceived(IMessage message)
		{
			ITextMessage textMessage = message as ITextMessage;
			string Msg = "clsMessageHandler(), Command message received";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg);
			if (this.CommandReceived != null)
			{
				// call the delegate to process the commnd
				Msg = "clsMessageHandler().OnCommandReceived: At lease one event handler assigned";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg);
				this.CommandReceived(textMessage.Text);
			}
			else
			{
				Msg = "clsMessageHandler().OnCommandReceived: No event handlers assigned";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg);
			}
		}	// End sub

		/// <summary>
		/// Broadcast listener function. Received Broadcasts will cause this to be called
		///	and it will trigger an event to pass on the command to all registered listeners
		/// </summary>
		/// <param name="message">Incoming message</param>
		private void OnBroadcastReceived(IMessage message)
		{
			ITextMessage textMessage = message as ITextMessage;
			string Msg = "clsMessageHandler(), Broadcast message received";
			clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg);
			if (this.BroadcastReceived != null)
			{
				// call the delegate to process the commnd
				Msg = "clsMessageHandler().OnBroadcastReceived: At lease one event handler assigned";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg);
				this.BroadcastReceived(textMessage.Text);
			}
			else
			{
				Msg = "clsMessageHandler().OnBroadcastReceived: No event handlers assigned";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.DEBUG, Msg);
			}
		}	// End sub

		/// <summary>
		/// Sends a status message
		/// </summary>
		/// <param name="message">Outgoing message string</param>
		public void SendMessage(string message)
		{
			if (!this.m_IsDisposed)
			{
				ITextMessage textMessage = this.m_StatusSession.CreateTextMessage(message);
				textMessage.Properties.SetString("ProcessorName", m_MgrSettings.GetParam("MgrName"));
				try
				{
					this.m_StatusSender.Send(textMessage);
				}
				catch
				{
					// Do nothing
				}
			}
			else
			{
				throw new ObjectDisposedException(this.GetType().FullName);
			}
		}	// End sub
		#endregion

		#region "Cleanup"
		/// <summary>
		/// Cleans up a connection after error or when closing
		/// </summary>
		protected void DestroyConnection()
		{
			if (m_HasConnection)
			{
				this.m_Connection.Dispose();
				this.m_HasConnection = false;
				string msg = "Message connection closed";
				clsLogTools.WriteLog(clsLogTools.LoggerTypes.LogFile, clsLogTools.LogLevels.INFO, msg);
			}
		}	// End sub

		/// <summary>
		/// Implements IDisposable interface
		/// </summary>
		public void Dispose()
		{
			if (!this.m_IsDisposed)
			{
				this.DestroyConnection();
				this.m_IsDisposed = true;
			}
		}	// End sub

		/// <summary>
		/// Registers the command and broadcast listeners under control of main program.
		/// This is done to prevent loss of queued messages if listeners are registered too early.
		/// </summary>
		public void RegisterListeners()
		{
			m_CommandConsumer.Listener += OnCommandReceived;
			m_BroadcastConsumer.Listener += OnBroadcastReceived;
		}	// End sub
		#endregion
	}	// End class
}	// End namespace
